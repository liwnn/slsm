package slsm

import "github.com/liwnn/skiplist"

type Run interface {
	GetElementsNum() uint64
	InsertKey(key KVPair)
	SetSize(size uint64)
	GetAll() []KVPair
	GetMin() int
	GetMax() int
	Lookup(key int) (int, bool)
	GetAllInRange(key1, key2 int) []KVPair
}

type KVPair struct {
	Key   int
	Value int
}

func (kv KVPair) Less(than skiplist.Item) bool {
	return kv.Key < than.(KVPair).Key
}

// MemRun 内存run
type MemRun struct {
	sl       *skiplist.SkipList
	min, max int
	size     int
}

func NewMemRun(minKey, maxKey int) *MemRun {
	return &MemRun{
		sl:  skiplist.New(),
		min: minKey,
		max: maxKey,
	}
}

func (r *MemRun) SetSize(size uint64) {
	//_maxSize = size
}

func (r *MemRun) InsertKey(kv KVPair) {
	if kv.Key > r.max {
		r.max = kv.Key
	} else if kv.Key < r.min {
		r.min = kv.Key
	}

	r.sl.Insert(kv)
	if kv.Value != TOMBSTONE {
		r.size++
	} else {
		r.size--
	}
}

func (r *MemRun) GetElementsNum() uint64 {
	return uint64(r.sl.Len())
}

func (r *MemRun) GetAll() []KVPair {
	vec := make([]KVPair, 0, r.sl.Len())
	for it := r.sl.NewIterator(); it.Valid(); it.Next() {
		vec = append(vec, it.Value().(KVPair))
	}

	return vec
}
func (r MemRun) GetMin() int {
	return r.min
}

func (r MemRun) GetMax() int {
	return r.max
}

func (r MemRun) Lookup(key int) (int, bool) {
	item := r.sl.Search(KVPair{Key: key})
	if item == nil {
		return -1, false
	}
	return item.(KVPair).Value, true
}

func (r MemRun) GetAllInRange(key1, key2 int) []KVPair {
	if key1 > r.max || key2 < r.min {
		return nil
	}

	vec := make([]KVPair, 0, 8)
	it := r.sl.NewIterator()
	for ; it.Valid() && it.Value().Less(KVPair{Key: key1}); it.Next() {
	}
	for ; it.Valid() && it.Value().Less(KVPair{Key: key2}); it.Next() {
		vec = append(vec, it.Value().(KVPair))
	}
	return vec
}
