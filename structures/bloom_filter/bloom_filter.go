package bloom_filter

import (
	"encoding/binary"
	"errors"
	"math"
)

// BloomFilter is a probabilistic data structure that efficiently tests whether an element is in a set.
// It can tell with 100% certainty that the element is contained, but will tell if it isn't
// contained with false positive rate used as a parameter when creating an instance.
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
// It can tell with 100% certainty that the element is contained, but will tell if it isn't
// contained with false positive rate used as a parameter when creating an instance.
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

// Serialize serializes the Bloom Filter into a byte array.
// The byte array contains the size of the bit array, the number of hash functions, the seeds of the hash functions, and the bit array.
// The format is as follows:
// - 4 bytes for the size of the bit array (m)
// - 4 bytes for the number of hash functions (k)
// - For each hash function:
//   - 4 bytes for the length of the seed
//   - Seed bytes
//
// - Bit array bytes
func (bf *BloomFilter) Serialize() []byte {
	totalSize := 8 + len(bf.b)
	for _, hash := range bf.h {
		totalSize += 4 + len(hash.Seed)
	}

	data := make([]byte, totalSize)
	offset := 0
	binary.LittleEndian.PutUint32(data[offset:], bf.m)
	offset += 4
	binary.LittleEndian.PutUint32(data[offset:], bf.k)
	offset += 4

	for _, hash := range bf.h {
		binary.LittleEndian.PutUint32(data[offset:], uint32(len(hash.Seed)))
		offset += 4

		copy(data[offset:], hash.Seed)
		offset += len(hash.Seed)
	}
	copy(data[offset:], bf.b)
	return data
}

// Deserialize creates a new Bloom Filter from a byte array.
// The byte array should contain the size of the bit array, the number of hash functions, the seeds of the hash functions, and the bit array.
func Deserialize(data []byte) (*BloomFilter, error) {
	if len(data) < 8 {
		return nil, errors.New("data is too short")
	}
	offset := 0
	m := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	k := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	h := make([]HashWithSeed, k)
	for i := uint32(0); i < k; i++ {
		if offset+4 > len(data) {
			return nil, errors.New("data is too short")
		}
		seedLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(seedLen) > len(data) {
			return nil, errors.New("data is too short")
		}
		seed := make([]byte, seedLen)
		copy(seed, data[offset:offset+int(seedLen)])
		offset += int(seedLen)
		h[i] = HashWithSeed{Seed: seed}
	}
	if offset > len(data) {
		return nil, errors.New("data is too short")
	}
	b := make([]byte, len(data)-offset)
	copy(b, data[offset:])
	return &BloomFilter{
		m: m,
		k: k,
		h: h,
		b: b,
	}, nil
}

// TODO: Kada odradimo sve strukture ujediniti hash u jedan hash fajl, da nema nepotrebih ponavljanja
