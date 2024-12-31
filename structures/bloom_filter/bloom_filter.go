package bloom_filter

import (
	"math"
)

// BloomFilter is a probabilistic data structure that efficiently tests whether an element is in a set.
// It can tell with 100% certainty that the element is conitaind, but will tell if it isn't
// conitaind with false positive rate used as a parameter when creating an instance.
// It works with uint32 for efficiency given the data size in our project.
type BloomFilter struct {
	m uint32         // Size of the bit array
	k uint32         // Number of hash functions
	h []HashWithSeed // Array of hash functions
	b []byte         // Byte array representing the bit array
}

// NewBloomFilter creates a new instance of a Bloom Filter.
// expectedElements: the number of elements expected to be added to the filter.
// falsePositiveRate: the desired false positive rate.
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	return &BloomFilter{
		m: uint32(m),
		k: uint32(k),
		h: CreateHashFunctions(uint32(k)),
		b: make([]byte, uint32(math.Ceil(float64(m)/8))), // Calculate the length of the byte array
	}
}

// Add inserts an element into the Bloom Filter by setting the corresponding bits to 1.
// item: the element to be added to the filter.
func (bf *BloomFilter) Add(item []byte) {
	for i := uint32(0); i < bf.k; i++ {
		hash := bf.h[i].Hash(item) % uint64(bf.m)
		bitMask := byte(1 << (hash % 8))
		bf.b[hash/8] |= bitMask
	}
}

// Contains checks if an item is in the Bloom Filter.
// It can tell with 100% certainty that the element is conitaind, but will tell if it isn't
// conitaind with false positive rate used as a parameter when creating an instance.
// item: the element to be checked.
func (bf *BloomFilter) Contains(item []byte) bool {
	for i := uint32(0); i < bf.k; i++ {
		hash := bf.h[i].Hash(item) % uint64(bf.m)
		bitMask := byte(1 << (hash % 8))
		if bf.b[hash/8]&bitMask == 0 {
			return false
		}
	}
	return true
}

// TODO: Serijalizacija
// TODO: Deserijalizacija
// TODO: Napisati testove (pogledati primer 5 sa trecih vezbi, i nalik toga napisati funkcije)

// TODO: Kada odradimo sve strukture ujediniti hash u jedan hash fajl, da nema nepotrebih ponavljanja
