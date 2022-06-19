package slsm

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"syscall"
	"unsafe"
)

type DiskRun struct {
	dataref []byte   // 原始数据
	data    []KVPair // 原始数据转成KVPAIR

	runID    int
	level    int
	filename string

	fd            *os.File
	capacity      uint64
	pageSize      uint64
	fencePointers []int
	iMaxFP        uint64
	bf            *BloomFilter
	bffp          float64
	minKey        int
	maxKey        int
}

// @param capacity - 最大存多少个kv对
func NewDiskRun(capacity uint64, pageSize uint32, level int, runID int, bffp float64) *DiskRun {
	filename := "C_" + strconv.Itoa(level) + "_" + strconv.Itoa(runID) + ".txt"
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0x0600)
	if err != nil {
		panic(err)
	}

	// 设置读写位置
	filesize := capacity * uint64(unsafe.Sizeof(KVPair{}))
	if _, err := fd.Seek(int64(filesize-1), os.SEEK_SET); err != nil {
		panic(err)
	}

	if _, err := fd.Write([]byte{'\n'}); err != nil {
		panic(err)
	}

	//与其它所有映射这个对象的进程共享映射空间。对共享区的写入，相当于输出到文件
	// https://nieyong.github.io/wiki_cpu/mmap%E8%AF%A6%E8%A7%A3.html
	b, err := syscall.Mmap(int(fd.Fd()), 0, int(filesize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		fd.Close()
		panic(err)
	}

	sz := int(unsafe.Sizeof(KVPair{}))
	sh := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&b[0])),
		Len:  len(b) / sz,
		Cap:  len(b) / sz,
	}

	data := *(*[]KVPair)(unsafe.Pointer(&sh))
	return &DiskRun{
		dataref:  b,
		data:     data,
		filename: filename,
		fd:       fd,
		level:    level,
		capacity: capacity,
		pageSize: uint64(pageSize),
		minKey:   math.MinInt64,
		maxKey:   math.MinInt64,
		bffp:     bffp,
		bf:       NewBloomFilter(capacity, bffp),
	}
}

func (dr *DiskRun) Close() {
	dr.doUnmap()
}

func (dr *DiskRun) ChangeRunID(id int) {
	dr.runID = id

	newName := fmt.Sprintf("C_%v_%v.txt", dr.level, dr.runID)
	if err := os.Rename(dr.filename, newName); err != nil {
		panic(fmt.Errorf("rename %v to %v err[%v]", dr.filename, newName, err))
	}
	dr.filename = newName
}

func (dr *DiskRun) doUnmap() {
	if err := syscall.Munmap(dr.dataref); err != nil {
		panic(err)
	}

	if err := dr.fd.Close(); err != nil {
		panic(err)
	}
	dr.fd = nil
	dr.dataref = nil
}

func (dr *DiskRun) WiteData(run []KVPair, offset uint32) {
	copy(dr.data[offset:], run)
	dr.capacity = uint64(len(run))
}

// ConstructIndex 按照pageSize记录索引
func (dr *DiskRun) ConstructIndex() {
	// construct fence pointers and write BF
	//        _fencePointers.resize(0);
	dr.iMaxFP = 0 // TODO IS THIS SAFE?
	dr.fencePointers = make([]int, 0, dr.capacity/uint64(dr.pageSize)+1)
	for j := uint64(0); j < dr.capacity; j++ {
		dr.bf.Add(encodeInt(dr.data[j].Key))
		if j%dr.pageSize == 0 {
			dr.fencePointers = append(dr.fencePointers, dr.data[j].Key)
			if j != 0 {
				dr.iMaxFP++
			}
		}
	}
	dr.minKey = (dr.data[0].Key)
	dr.maxKey = (dr.data[dr.capacity-1].Key)
}

func (dr *DiskRun) SetCapacity(newCap uint64) {
	dr.capacity = newCap
}

func (dr *DiskRun) Lookup(key int) (int, bool) {
	idx, found := dr.GetIndex(key)
	if found {
		return dr.data[idx].Value, true
	}
	return 0, false
}

// GetIndex 获得key的索引
func (dr *DiskRun) GetIndex(key int) (uint64, bool) {
	var start, end = dr.getFlankingFp(key)
	return dr.binarySearch(start, end-start, key)
}

// 查找key所在的段
func (dr *DiskRun) getFlankingFp(key int) (start, end uint64) {
	if dr.iMaxFP == 0 { // 只有一段
		start = 0
		end = dr.capacity
	} else if key < dr.fencePointers[1] { // 第一段
		start = 0
		end = uint64(dr.pageSize)
	} else if key >= dr.fencePointers[dr.iMaxFP] { // 最后一段
		start = dr.iMaxFP * uint64(dr.pageSize)
		end = dr.capacity
	} else {
		// 2分查找
		min, max := uint64(0), dr.iMaxFP
		for min < max {
			middle := (min + max) >> 1
			if key > dr.fencePointers[middle] {
				if key < dr.fencePointers[middle+1] {
					start = middle * dr.pageSize
					end = (middle + 1) * dr.pageSize
					return // TODO THIS IS ALSO GROSS
				}
				min = middle + 1
			} else if key < dr.fencePointers[middle] {
				if key >= dr.fencePointers[middle-1] {
					start = (middle - 1) * dr.pageSize
					end = middle * dr.pageSize
					return // TODO THIS IS ALSO GROSS. THIS WILL BREAK IF YOU DON'T KEEP TRACK OF MIN AND MAX.
				}
				max = middle - 1
			} else {
				start = middle * dr.pageSize
				end = start
				return
			}
		}
	}
	panic("getFlankingFp")
}

func (dr *DiskRun) binarySearch(offset uint64, n uint64, key int) (uint64, bool) {
	if n == 0 { // 找到了
		return offset, true
	}
	min, max := offset, offset+n-1
	for min <= max {
		middle := (min + max) >> 1
		if key > dr.data[middle].Key {
			min = middle + 1
		} else if key == dr.data[middle].Key {
			return middle, true
		} else {
			max = middle - 1
		}
	}
	return min, false
}

func (dr *DiskRun) Range(key1 int, key2 int) (i1 uint64, i2 uint64) {
	if key1 > dr.maxKey || key2 < dr.minKey {
		return
	}
	if key1 < dr.minKey {
		i1 = 0
	} else {
		i1, _ = dr.GetIndex(key1)
	}
	if key2 > dr.maxKey {
		i2 = dr.capacity
	} else {
		i2, _ = dr.GetIndex(key2)
	}
	return i1, i2
}

// GetCapacity 获得最多的元素个数
func (dr *DiskRun) GetCapacity() uint64 {
	return dr.capacity
}
