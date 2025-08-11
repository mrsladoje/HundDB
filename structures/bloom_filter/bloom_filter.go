package bloom_filter

import (
	"encoding/binary"
	"math"

	sh "hunddb/utils/seeded_hash"
)

// BloomFilter is a probabilistic data structure that efficiently tests whether an element is in a set.
// It can tell with 100% certainty that the element is contained, but will tell if it isn't
// contained with false positive rate used as a parameter when creating an instance.
// It works with uint32 for efficiency given the data size in our project.
type BloomFilter struct {
	m uint32            // Size of the bit array
	k uint32            // Number of hash functions
	h []sh.HashWithSeed // Array of hash functions
	b []byte            // Byte array representing the bit array
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
		h: sh.CreateHashFunctions(uint64(k)),
		b: make([]byte, uint32(math.Ceil(float64(m)/8))), // Calculate the length of the byte array
	}
}

// Calculates m value
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

// Calculates k value
func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
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
// - 8 bytes for each Seed of a hash function
// - (m rounded up to 0 of 8 module) bytes, representing the main byte array
func (bf *BloomFilter) Serialize() []byte {
	// totalSize is the size of the whole bloom filter structure
	totalSize := 8 + 8*bf.k + uint32(len(bf.b))
	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(data[0:4], bf.m)
	binary.LittleEndian.PutUint32(data[4:8], bf.k)

	offset := 8
	for _, hash := range bf.h {
		copy(data[offset:offset+8], hash.Serialize())
		offset += 8
	}
	copy(data[offset:], bf.b)

	return data
}

// Deserialize creates a new Bloom Filter from a byte array.
// The byte array contains the size of the bit array, the number of hash functions, the seeds of the hash functions, and the bit array.
// The format is as follows:
// - 4 bytes for the size of the bit array (m)
// - 4 bytes for the number of hash functions (k)
// - 8 bytes for each Seed of a hash function
// - (m rounded up to 0 of 8 module) bytes, representing the main byte array
func Deserialize(data []byte) *BloomFilter {
	offset := 0
	m := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	k := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	h := make([]sh.HashWithSeed, k)
	for i := uint32(0); i < k; i++ {
		h[i] = sh.Deserialize(data[offset : offset+8])
		offset += 8
	}
	b := make([]byte, len(data)-offset)
	copy(b, data[offset:])
	return &BloomFilter{
		m: m,
		k: k,
		h: h,
		b: b,
	}
}
