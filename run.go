package main

type Run interface {
	GetElementsNum() uint64
	InsertKey(key int, value int)
	SetSize(size uint64)
	GetAll() []KVPair
	GetMin() int
	GetMax() int
	Lookup(key int) (int, bool)
	GetAllInRange(key1, key2 int) []KVPair
}

type CmpInt struct{}

func (CmpInt) Compare(a, b interface{}) int {
	l, r := a.(int), b.(int)
	if l < r {
		return -1
	}
	if l == r {
		return 0
	}
	return 1
}

type KVPair struct {
	Key   int
	Value int
}

// MemRun 内存run
type MemRun struct {
	sl       *SkipList
	min, max int
	size     int
}

func NewMemRun(minKey, maxKey int) *MemRun {
	return &MemRun{
		sl:  NewSkipList(&CmpInt{}),
		min: minKey,
		max: maxKey,
	}
}

func (r *MemRun) SetSize(size uint64) {
	//_maxSize = size
}

func (r *MemRun) InsertKey(key int, value int) {
	if key > r.max {
		r.max = key
	} else if key < r.min {
		r.min = key
	}

	r.sl.Insert(key, value)
	if value != TOMBSTONE {
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
	it := NewIterator(r.sl)
	for it.SeekToFirst(); it.Valid(); it.Next() {
		kv := KVPair{Key: it.Key().(int), Value: it.Value().(int)}
		vec = append(vec, kv)
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
	e, ok := r.sl.Search(key)
	if !ok {
		return -1, false
	}
	return e.(int), true
}

func (r MemRun) GetAllInRange(key1, key2 int) []KVPair {
	if key1 > r.max || key2 < r.min {
		return nil
	}

	vec := make([]KVPair, 0, 8)
	it := NewIterator(r.sl)
	for it.SeekToFirst(); it.Valid() && it.IsLessThan(key1); it.Next() {
	}
	for ; it.Valid() && it.IsLessThan(key2); it.Next() {
		vec = append(vec, KVPair{
			Key:   it.Key().(int),
			Value: it.Value().(int),
		})
	}
	return vec
}
