package slsm

import (
	"math"
)

type BitSet struct {
	values []byte
}

// NewBitSet initializes BitArray b.
func NewBitSet(length uint32) *BitSet {
	size := length >> 3
	if length&7 > 0 {
		size++
	}
	return &BitSet{
		values: make([]byte, size),
	}
}

// Set index to 1 if value is true, or set index to 0 if value is false.
func (b *BitSet) Set(index uint64, value bool) {
	if value {
		b.values[index>>3] |= 1 << (index & 7)
	} else {
		b.values[index>>3] &^= (1 << (index & 7))
	}
}

// Get true if index is set 1, or return false.
func (b *BitSet) Get(index uint64) bool {
	if index >= b.Size() {
		return false
	}
	return (b.values[index>>3] & (1 << (index & 7))) != 0
}

func (b *BitSet) Size() uint64 {
	return uint64(len(b.values)) * 8
}

// BloomFilter 布隆过滤器
type BloomFilter struct {
	bitSet    *BitSet // 位数组
	numHashes uint8   // hash函数个数
}

// NewBloomFilter new
// @param n - 预估元素个数
// @param p - 误判率
func NewBloomFilter(n uint64, p float64) *BloomFilter {
	ln2 := 0.693147180559945                                // ln2
	denom := 0.480453013918201                              // ln(2)^2
	m := math.Ceil(-1 * (float64(n) * math.Log(p)) / denom) // 位数组的位数
	k := math.Ceil(m / float64(n) * ln2)                    // hash函数个数
	return &BloomFilter{
		bitSet:    NewBitSet(uint32(m)),
		numHashes: uint8(k),
	}
}

func (bf *BloomFilter) BloomHash(data []byte) (uint64, uint64) {
	return MurmurHash3_x64_128(data, 0)
}

// Add 增加元素
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.BloomHash(key)
	for i := uint8(0); i < bf.numHashes; i++ {
		// 双重散列法(Double Hashing): h(i,k) = (h1(k) + i*h2(k)) % TABLE_SIZE
		h := (h1 + uint64(i)*h2) % bf.bitSet.Size()
		bf.bitSet.Set(h, true)
	}
}

// MayContain 是否有存在可能
func (bf *BloomFilter) MayContain(data []byte) bool {
	h1, h2 := bf.BloomHash(data)
	for i := uint8(0); i < bf.numHashes; i++ {
		h := (h1 + uint64(i)*h2) % bf.bitSet.Size()
		if !bf.bitSet.Get(h) {
			return false
		}
	}
	return true
}
