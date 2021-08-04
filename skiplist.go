package main

import "math/rand"

const (
	kMaxLevel = 12   // (1/p)^kMaxLevel >= maxNode
	p         = 0.25 // Skiplist P = 1/4
)

type Comparator interface {
	Compare(a, b interface{}) int
}

type Iterator struct {
	sl   *SkipList
	node *skipListNode
}

func NewIterator(sl *SkipList) *Iterator {
	return &Iterator{
		sl: sl,
	}
}

func (it *Iterator) SeekToFirst() {
	it.node = it.sl.header.forward[0]
}

func (it *Iterator) Valid() bool {
	return it.node != nil
}

func (it *Iterator) Next() {
	it.node = it.node.forward[0]
}

func (it *Iterator) IsLessThan(key interface{}) bool {
	return it.sl.isLessThan(it.node.key, key)
}

func (it *Iterator) Key() interface{} {
	return it.node.key
}

func (it *Iterator) Value() interface{} {
	return it.node.value
}

// skipListNode is an element of a skip list
type skipListNode struct {
	key     interface{}
	value   interface{}
	forward []*skipListNode
}

func createNode(level int, key int, value int) *skipListNode {
	return &skipListNode{
		key:     key,
		value:   value,
		forward: make([]*skipListNode, level),
	}
}

func (n *skipListNode) Key() interface{} {
	return n.key
}

func (n *skipListNode) Value() interface{} {
	return n.value
}

// SkipList represents a skip list
type SkipList struct {
	header     *skipListNode
	level      int // current level count
	length     int
	comparator Comparator
}

// NewSkipList creates a skip list
func NewSkipList(cmp Comparator) *SkipList {
	sl := &SkipList{
		comparator: cmp,
		level:      1,
		header:     createNode(kMaxLevel, 0, 0),
	}
	return sl
}

func (sl *SkipList) isLessThan(l, r interface{}) bool {
	return sl.comparator.Compare(l, r) < 0
}

// Search for an element by traversing forward pointers
func (sl *SkipList) Search(searchKey interface{}) (interface{}, bool) {
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && sl.isLessThan(x.forward[i].key, searchKey) {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && x.key == searchKey {
		return x.value, true
	}
	return -1, false
}

// Insert element
func (sl *SkipList) Insert(searchKey int, newValue int) {
	var update [kMaxLevel]*skipListNode
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && sl.isLessThan(x.forward[i].key, searchKey) {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != nil && x.key == searchKey {
		x.value = newValue
	} else {
		lvl := sl.randomLevel()
		if lvl > sl.level {
			for i := sl.level; i < lvl; i++ {
				update[i] = sl.header
			}
			sl.level = lvl
		}

		x = createNode(lvl, searchKey, newValue)
		for i := 0; i < lvl; i++ {
			x.forward[i] = update[i].forward[i]
			update[i].forward[i] = x
		}

		sl.length++
	}
}

// Delete element
func (sl *SkipList) Delete(searchKey int) {
	var update [kMaxLevel]*skipListNode
	x := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && sl.isLessThan(x.forward[i].key, searchKey) {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != nil && x.key == searchKey {
		for i := 0; i < sl.level; i++ {
			if update[i].forward[i] != x {
				break
			}
			update[i].forward[i] = x.forward[i]
		}
		for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
			sl.level--
		}

		sl.length--
	}
}

func (list *SkipList) randomLevel() int {
	lvl := 1
	for lvl < kMaxLevel && rand.Float64() < p {
		lvl++
	}
	return lvl
}

func (list *SkipList) Len() int {
	return list.length
}
