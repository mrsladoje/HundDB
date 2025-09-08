package count_min_sketch

import (
	"encoding/binary"
	"math"
	"sync"

	sh "hunddb/utils/seeded_hash"
)

// CMS is a probabilistic data structure that efficiently counts the frequency of elements in a set.
// It can over-estimate the count by the desired error rate passed as a parameter when creating an instance
// It works with uint32 for efficiency given the data size in our project.
// This implementation is thread-safe using RWMutex.
type CMS struct {
	m     uint32            // Size of the sets (columns)
	k     uint32            // Number of hash functions (rows)
	h     []sh.HashWithSeed // Array of hash functions
	table [][]uint32        // Table of uint32 values
	mu    sync.RWMutex      // RWMutex for thread-safety
}

// NewCMS creates a new instance of a Count-Min Sketch.
// epsilon: the desired error rate.
// delta: the desired confidence level.
func NewCMS(epsilon float64, delta float64) *CMS {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	matrix := make([][]uint32, k)
	for i := range matrix {
		matrix[i] = make([]uint32, m)
	}
	return &CMS{
		m:     uint32(m),
		k:     uint32(k),
		h:     sh.CreateHashFunctions(uint64(k)),
		table: matrix,
	}
}

// Calculates m value
func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

// Calculates k value
func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

// Add inserts an element into the Count-Min Sketch by incrementing the corresponding cells in the table.
// item: the element to be added to the sketch.
// This method is thread-safe and uses a write lock.
func (cms *CMS) Add(item []byte) {
	cms.mu.Lock()
	defer cms.mu.Unlock()

	for i := uint32(0); i < cms.k; i++ {
		j := cms.h[i].Hash(item) % uint64(cms.m)
		cms.table[i][j]++
	}
}

// Count estimates the frequency of an element in the Count-Min Sketch.
// item: the element to be checked.
// This method is thread-safe and uses a read lock.
func (cms *CMS) Count(item []byte) uint32 {
	cms.mu.RLock()
	defer cms.mu.RUnlock()

	min := uint32(4294967295) // Initialize to max uint32 value
	for i := uint32(0); i < cms.k; i++ {
		j := cms.h[i].Hash(item) % uint64(cms.m)
		if cms.table[i][j] < min {
			min = cms.table[i][j]
		}
	}
	return min
}

// Serialize serializes the Count-Min Sketch into a byte array.
// The byte array contains the m and k values, the hash seeds, and the table.
// The format is as follows:
// - 4 bytes for the m value of uint32 (number of columns)
// - 4 bytes for the k value of uint32 (number of rows)
// - 8 bytes for each uint64 Seed of a hash function (k seeds in total)
// - (4 * m * k) bytes, representing the size of the table (matrix of uint32)
// This method is thread-safe and uses a read lock.
func (cms *CMS) Serialize() []byte {
	cms.mu.RLock()
	defer cms.mu.RUnlock()

	// Total size of the entire structure in a byte array
	totalSize := 4 + 4 + 8*cms.k + 4*cms.k*cms.m
	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(data[0:4], cms.m)
	binary.LittleEndian.PutUint32(data[4:8], cms.k)

	offset := 8
	for _, hash := range cms.h {
		copy(data[offset:offset+8], hash.Serialize())
		offset += 8
	}

	for i := uint32(0); i < cms.k; i++ {
		for j := 0; j < int(cms.m); j++ {
			binary.LittleEndian.PutUint32(data[offset:offset+4], cms.table[i][j])
			offset += 4
		}
	}

	return data
}

// Deserialize creates a new Count-Min Sketch from a byte array.
// The byte array contains the m and k values, the hash seeds, and the table.
// The format is as follows:
// - 4 bytes for the m value of uint32 (number of columns)
// - 4 bytes for the k value of uint32 (number of rows)
// - 8 bytes for each uint64 Seed of a hash function (k seeds in total)
// - (4 * m * k) bytes, representing the size of the table (matrix of uint32)
func Deserialize(data []byte) *CMS {
	m := binary.LittleEndian.Uint32(data[0:4])
	k := binary.LittleEndian.Uint32(data[4:8])

	offset := 8
	h := make([]sh.HashWithSeed, k)
	for i := uint32(0); i < k; i++ {
		h[i] = sh.Deserialize(data[offset : offset+8])
		offset += 8
	}

	table := make([][]uint32, k)
	for i := uint32(0); i < k; i++ {
		row := make([]uint32, m)
		for j := uint32(0); j < m; j++ {
			row[j] = binary.LittleEndian.Uint32(data[offset:])
			offset += 4
		}
		table[i] = row
	}

	return &CMS{
		m:     m,
		k:     k,
		h:     h,
		table: table,
	}
}
