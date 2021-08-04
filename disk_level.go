package main

import (
	"math"
)

const (
	None = math.MinInt64
)

type KVIntPair struct {
	KVPair
	k int
}

func NewKVIntPair(kv KVPair, index int) KVIntPair {
	return KVIntPair{
		KVPair: kv,
		k:      index,
	}
}

// StaticHeap is a min-heap
type StaticHeap struct {
	arr []KVIntPair
}

// NewStaticHeap new
func NewStaticHeap(sz int) *StaticHeap {
	return &StaticHeap{
		arr: make([]KVIntPair, 0, sz),
	}
}

// Len return heap size.
func (h StaticHeap) Len() int {
	return len(h.arr)
}

// Push add x as element Len()
func (h *StaticHeap) Push(x KVIntPair) {
	h.arr = append(h.arr, x)
	i := h.Len() - 1
	for {
		p := (i - 1) / 2 // parent
		if p == i || h.arr[i].Key >= h.arr[p].Key {
			break
		}
		h.arr[i], h.arr[p] = h.arr[p], h.arr[i]
		i = p
	}
}

// Pop remove and return element Len() - 1
func (h *StaticHeap) Pop() KVIntPair {
	min := h.arr[0]
	n := len(h.arr)
	h.arr[0] = h.arr[n-1]
	h.arr = h.arr[:n-1]
	h.heapify(0)
	return min
}

func (h *StaticHeap) heapify(i int) {
	l := i*2 + 1 // left child
	r := i*2 + 2 // right child
	var smallest = i
	if l < len(h.arr) && h.arr[l].Key < h.arr[i].Key {
		smallest = l
	}

	if r < len(h.arr) && h.arr[r].Key < h.arr[smallest].Key {
		smallest = r
	}
	if smallest != i {
		h.arr[smallest], h.arr[i] = h.arr[i], h.arr[smallest]
		h.heapify(smallest)
	}
}

type DiskLevel struct {
	level     int    // 第几层（从0开始)
	numRuns   int    // number of runs in a level
	runSize   uint64 // number of elts in a run
	mergeSize int    // 一次合并run的个数
	pageSize  uint32
	bffp      float64

	runs      []*DiskRun
	activeRun int
}

// @param pageSize -
// @param level - 第几层
// @param runSize - 每个run得大小
// @param numRuns - run得个数
// @param mergeSize - 需要merge得run个数
func NewDiskLevel(pageSize uint32, level int, runSize uint64, numRuns int, mergeSize int, bffp float64) *DiskLevel {
	dl := &DiskLevel{
		level:     level,
		numRuns:   numRuns,
		runSize:   runSize,
		mergeSize: mergeSize,
		pageSize:  pageSize,
		runs:      make([]*DiskRun, 0, numRuns),
	}

	for i := 0; i < int(numRuns); i++ {
		run := NewDiskRun(runSize, pageSize, level, i, bffp)
		dl.runs = append(dl.runs, run)
	}
	return dl
}

// AddRunByArray新增加一个run
func (dl *DiskLevel) AddRunByArray(runToAdd []KVPair) {
	if dl.activeRun >= dl.numRuns {
		panic("")
	}
	runLen := len(runToAdd)
	if uint64(runLen) != dl.runSize {
		panic("")
	}
	dl.runs[dl.activeRun].WiteData(runToAdd, 0)
	dl.runs[dl.activeRun].ConstructIndex()
	dl.activeRun++
}

// AddRuns 合并runList构造一个run
func (dl *DiskLevel) AddRuns(runList []*DiskRun, runLen uint64, lastLevel bool) {
	k := len(runList)
	var h = NewStaticHeap(k)
	var S = dl.runs[dl.activeRun]
	for r := 0; r < k; r++ {
		kvp := runList[r].data[0]
		h.Push(NewKVIntPair(kvp, r))
	}
	j := -1
	lastKey := None
	lastK := None
	Heads := make([]int, k)
	for h.Len() > 0 {
		v := h.Pop()
		if v.Key == lastKey { // ?? 也可以在堆中处理相等的情况，则只要相等就是老的 ??
			// when several runs from level k contain the same key,
			// the value that remains tied to that key on level k+1 must be the most recently writen
			if lastK < v.k {
				S.data[j] = v.KVPair
				lastK = v.k
			}
		} else {
			j = j + 1
			S.data[j] = v.KVPair
			lastKey = v.Key
			lastK = v.k
		}

		Heads[v.k]++
		if Heads[v.k] < len(runList[v.k].data) {
			kvp := runList[v.k].data[Heads[v.k]]
			h.Push(NewKVIntPair(kvp, v.k))
		}
	}
	S.SetCapacity(uint64(j + 1))
	S.ConstructIndex()
	if j+1 > 0 {
		dl.activeRun++
	}
}

func (dl *DiskLevel) LevelFull() bool {
	return dl.activeRun == dl.numRuns
}

func (dl *DiskLevel) LevelEmpty() bool {
	return dl.activeRun == 0
}

func (dl *DiskLevel) GetRunsToMerge() []*DiskRun {
	toMerge := make([]*DiskRun, 0, dl.mergeSize)
	for i := 0; i < dl.mergeSize; i++ {
		toMerge = append(toMerge, dl.runs[i])
	}
	return toMerge
}

func (dl *DiskLevel) FreeMergedRuns() {
	copy(dl.runs, dl.runs[dl.mergeSize:])
	dl.runs = dl.runs[:len(dl.runs)-dl.mergeSize]
	dl.activeRun -= dl.mergeSize
	for i := 0; i < dl.activeRun; i++ {
		dl.runs[i].ChangeRunID(i)
	}

	for i := dl.activeRun; i < dl.numRuns; i++ {
		newRun := NewDiskRun(dl.runSize, dl.pageSize, dl.level, i, dl.bffp)
		dl.runs = append(dl.runs, newRun)
	}
}

func (dl *DiskLevel) Lookup(key int) (int, bool) {
	maxRunToSearch := dl.activeRun - 1
	if dl.LevelFull() {
		maxRunToSearch = dl.numRuns - 1
	}
	for i := maxRunToSearch; i >= 0; i-- {
		if key < dl.runs[i].minKey ||
			key > dl.runs[i].maxKey ||
			!dl.runs[i].bf.MayContain(encodeInt(key)) {
			continue
		}
		lookupRes, found := dl.runs[i].Lookup(key)
		if found {
			return lookupRes, found
		}
	}
	return 0, false
}

func (dl *DiskLevel) GetElementsNum() uint64 {
	var total uint64
	for i := 0; i < dl.activeRun; i++ {
		total += dl.runs[i].GetCapacity()
	}
	return total
}
