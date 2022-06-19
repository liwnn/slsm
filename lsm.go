package slsm

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"sync"
	"unsafe"
)

const (
	TOMBSTONE int = math.MinInt64
)

type LSM struct {
	C0        []Run // 内存run
	activeRun int   // 当前run
	filters   []*BloomFilter

	diskLevels []*DiskLevel // 磁盘

	eltsPerRun          uint64 // 每个run的kv个数
	numRuns             int    // run的最大个数
	numToMerge          int    // 达到多少个run后merge
	bfFalsePositiveRate float64
	mergedFrac          float64
	diskRunsPerLevel    int // 每层磁盘run的个数
	pageSize            uint32

	mergeWg sync.WaitGroup

	V_TOMBSTONE int
}

// @param eltsPerRun - 每个run的kv个数
// @param numRuns - 内存run的个数
// @param mergedFrac - 需要合并的比率(1.0代表所有run都满了才合并)
// @param bfFp - 布隆过滤器误判率
// @param pageSize - 磁盘页大小
// @param diskRunsPerLevel - 每层磁盘run的个数
func NewLSM(eltsPerRun uint64, numRuns int, mergedFrac float64, bfFp float64,
	pageSize uint32, diskRunsPerLevel int) *LSM {
	// 达到多少个run后merge
	var numToMerge = int(math.Ceil(float64(numRuns) * mergedFrac))
	lsm := &LSM{
		eltsPerRun:          eltsPerRun,
		numRuns:             numRuns,
		numToMerge:          numToMerge,
		bfFalsePositiveRate: bfFp,
		mergedFrac:          mergedFrac,
		pageSize:            pageSize,
		diskRunsPerLevel:    diskRunsPerLevel,
		V_TOMBSTONE:         TOMBSTONE,
	}

	// numToMerge*eltsPerRun - merge的个数就是磁盘run的元素数
	mergeSize := int(math.Ceil(float64(diskRunsPerLevel) * mergedFrac))
	diskLevel := NewDiskLevel(pageSize, 1, uint64(numToMerge)*eltsPerRun, diskRunsPerLevel, mergeSize, bfFp)
	lsm.diskLevels = append(lsm.diskLevels, diskLevel)

	for i := 0; i < numRuns; i++ {
		run := NewMemRun(math.MinInt32, math.MaxInt32)
		run.SetSize(eltsPerRun)
		lsm.C0 = append(lsm.C0, run)

		bf := NewBloomFilter(eltsPerRun, bfFp)
		lsm.filters = append(lsm.filters, bf)
	}
	return lsm
}

func (lsm *LSM) InsertKey(key int, value int) {
	if lsm.C0[lsm.activeRun].GetElementsNum() >= lsm.eltsPerRun {
		lsm.activeRun++
	}

	if lsm.activeRun >= lsm.numRuns {
		lsm.doMerge()
	}

	lsm.C0[lsm.activeRun].InsertKey(KVPair{key, value})
	lsm.filters[lsm.activeRun].Add(encodeInt(key))
}

func (lsm *LSM) doMerge() {
	if lsm.numToMerge == 0 {
		return
	}

	// 合并
	mergeRuns := append([]Run{}, lsm.C0[:lsm.numToMerge]...)
	mergeFilters := append([]*BloomFilter{}, lsm.filters[:lsm.numToMerge]...)
	lsm.mergeWg.Wait()
	lsm.mergeWg.Add(1)
	go func(runs []Run, bf []*BloomFilter) {
		defer lsm.mergeWg.Done()
		lsm.mergeRuns(runs, bf)
	}(mergeRuns, mergeFilters)

	// 未合并的run前移
	copy(lsm.C0, lsm.C0[lsm.numToMerge:])
	lsm.C0 = lsm.C0[:len(lsm.C0)-lsm.numToMerge]

	copy(lsm.filters, lsm.filters[lsm.numToMerge:])
	lsm.filters = lsm.filters[:len(lsm.filters)-lsm.numToMerge]

	lsm.activeRun -= lsm.numToMerge

	// 补充空run
	for i := lsm.activeRun; i < lsm.numRuns; i++ {
		run := NewMemRun(math.MinInt32, math.MaxInt32)
		run.SetSize(lsm.eltsPerRun)
		lsm.C0 = append(lsm.C0, run)

		bf := NewBloomFilter(lsm.eltsPerRun, lsm.bfFalsePositiveRate)
		lsm.filters = append(lsm.filters, bf)
	}
}

func (lsm *LSM) mergeRuns(runsToMerge []Run, bfToMerge []*BloomFilter) {
	toMerge := make([]KVPair, 0, lsm.eltsPerRun*uint64(lsm.numToMerge))
	for i := 0; i < len(runsToMerge); i++ {
		all := runsToMerge[i].GetAll()
		toMerge = append(toMerge, all...)
	}
	// ????? sort? 有序合并
	sort.Slice(toMerge, func(i, j int) bool {
		return toMerge[i].Key < toMerge[j].Key
	})
	if lsm.diskLevels[0].LevelFull() {
		lsm.mergeRunsToLevel(1)
	}
	lsm.diskLevels[0].AddRunByArray(toMerge)
}

// @param level - 要合并到的层索引
func (lsm *LSM) mergeRunsToLevel(level int) {
	if level == len(lsm.diskLevels) { // if this is the last level
		lastLevel := lsm.diskLevels[level-1]
		mergeSize := math.Ceil(float64(lsm.diskRunsPerLevel) * lsm.mergedFrac) // 需要合并的run个数
		runSize := lastLevel.runSize * uint64(lastLevel.mergeSize)
		newLevel := NewDiskLevel(lsm.pageSize, level+1, runSize, lsm.diskRunsPerLevel, int(mergeSize), lsm.bfFalsePositiveRate)
		lsm.diskLevels = append(lsm.diskLevels, newLevel)
	}

	if lsm.diskLevels[level].LevelFull() {
		lsm.mergeRunsToLevel(level + 1) // merge down one, recursively
	}

	var isLast = false
	if level+1 == len(lsm.diskLevels) && lsm.diskLevels[level].LevelEmpty() {
		isLast = true
	}

	runsToMerge := lsm.diskLevels[level-1].GetRunsToMerge()
	runLen := lsm.diskLevels[level-1].runSize
	lsm.diskLevels[level].AddRuns(runsToMerge, runLen, isLast)
	lsm.diskLevels[level-1].FreeMergedRuns()
}

func (lsm *LSM) Lookup(key int) (int, bool) {
	// 内存中找
	for i := lsm.activeRun; i >= 0; i-- {
		if key < lsm.C0[i].GetMin() ||
			key > lsm.C0[i].GetMax() ||
			!lsm.filters[i].MayContain(encodeInt(key)) {
			continue
		}

		value, found := lsm.C0[i].Lookup(key)
		if found {
			return value, value != lsm.V_TOMBSTONE
		}
	}

	// 磁盘中找
	// make sure that there isn't a merge happening as you search the disk
	lsm.mergeWg.Wait()

	// it's not in C_0 so let's look at disk.
	for _, l := range lsm.diskLevels {
		value, found := l.Lookup(key)
		if found {
			return value, value != lsm.V_TOMBSTONE
		}
	}
	return 0, false
}

func (lsm *LSM) DeleteKey(key int) {
	lsm.InsertKey(key, lsm.V_TOMBSTONE)
}

func (lsm *LSM) Range(key1, key2 int) []KVPair {
	if key2 <= key1 {
		return nil
	}

	ht := make(map[int]int)
	etlsInRange := make([]KVPair, 0, 8)

	// 内存
	for i := lsm.activeRun; i >= 0; i-- {
		cur_elts := lsm.C0[i].GetAllInRange(key1, key2)
		for _, e := range cur_elts {
			if _, ok := ht[e.Key]; !ok && e.Value != int(lsm.V_TOMBSTONE) {
				etlsInRange = append(etlsInRange, e)
			}
			ht[e.Key] = e.Value
		}
	}

	// 磁盘
	lsm.mergeWg.Wait()
	for _, l := range lsm.diskLevels {
		for r := l.activeRun - 1; r >= 0; r-- {
			var i1, i2 = l.runs[r].Range(key1, key2)
			for m := i1; m < i2; m++ {
				KV := l.runs[r].data[m]
				if _, ok := ht[KV.Key]; !ok && KV.Value != int(lsm.V_TOMBSTONE) {
					etlsInRange = append(etlsInRange, KV)
				}
				ht[KV.Key] = KV.Value
			}
		}
	}
	return etlsInRange
}

func (lsm *LSM) Close() {
	lsm.mergeWg.Wait()
}

func encodeInt(i int) []byte {
	data := unsafe.Pointer(&i)
	sz := int(unsafe.Sizeof(i))
	t := reflect.SliceHeader{
		Data: uintptr(data),
		Len:  sz,
		Cap:  sz,
	}

	d := *(*[]byte)(unsafe.Pointer(&t))
	return d
}

func (lsm *LSM) numBuffer() uint64 {
	lsm.mergeWg.Wait()
	var total uint64
	for i := 0; i <= lsm.activeRun; i++ {
		total += lsm.C0[i].GetElementsNum()
	}
	return total
}

func (lsm *LSM) PrintStats() {
	fmt.Printf("Number of Elements: %v\n", len(lsm.Range(lsm.V_TOMBSTONE, math.MaxInt64)))
	fmt.Printf("Number of Elements in Buffer (including deletes): %v\n", lsm.numBuffer())

	for i := 0; i < len(lsm.diskLevels); i++ {
		fmt.Printf("Number of Elements in Disk Level %v(including deletes): %v\n",
			i, lsm.diskLevels[i].GetElementsNum())
	}
	fmt.Println("KEY VALUE DUMP BY LEVEL: ")
	lsm.printElts()
}

func (lsm *LSM) printElts() {
	lsm.mergeWg.Wait()

	fmt.Println("MEMORY BUFFER")
	for i := 0; i <= lsm.activeRun; i++ {
		fmt.Printf("MEMORY BUFFER RUN %v\n", i)
		all := lsm.C0[i].GetAll()
		for _, c := range all {
			fmt.Printf("%v:%v ", c.Key, c.Value)
		}
		fmt.Println()
	}

	fmt.Println("\nDISK BUFFER")

	for i, l := range lsm.diskLevels {
		fmt.Printf("DISK LEVEL %v\n", i)
		for j := 0; j < lsm.diskLevels[i].activeRun; j++ {
			fmt.Printf("RUN %v\n", j)
			for k := uint64(0); k < l.runs[j].GetCapacity(); k++ {
				fmt.Printf("%v:%v  ",
					l.runs[j].data[k].Key, lsm.diskLevels[i].runs[j].data[k].Value)
			}
			fmt.Println()
		}
		fmt.Println()
	}
}
