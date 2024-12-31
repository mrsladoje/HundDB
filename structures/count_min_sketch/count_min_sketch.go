package count_min_sketch

// CMS is a probabilistic data structure that efficiently counts the frequency of elements in a set.
// It can over-estimate the count by the desired error rate passed as a parameter when creating an instance
// It works with uint32 for efficiency given the data size in our project.
type CMS struct {
	m     uint32         // Size of the sets (columns)
	k     uint32         // Number of hash functions (rows)
	h     []HashWithSeed // Array of hash functions
	table [][]uint32     // Table of uint32 values
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
		h:     CreateHashFunctions(k),
		table: matrix,
	}
}

// Add inserts an element into the Count-Min Sketch by incrementing the corresponding cells in the table.
// item: the element to be added to the sketch.
func (cms *CMS) Add(item []byte) {
	for i := uint32(0); i < cms.k; i++ {
		j := cms.h[i].Hash(item) % uint64(cms.m)
		cms.table[i][j]++
	}
}

// Count estimates the frequency of an element in the Count-Min Sketch.
// item: the element to be checked.
func (cms *CMS) Count(item []byte) uint32 {
	min := uint32(4294967295) // Initialize to max uint32 value
	for i := uint32(0); i < cms.k; i++ {
		j := cms.h[i].Hash(item) % uint64(cms.m)
		if cms.table[i][j] < min {
			min = cms.table[i][j]
		}
	}
	return min
}

// TODO: Serijalizacija
// TODO: Deserijalizacija
// TODO: Napisati testove (pogledati primer 5 sa trecih vezbi, i nalik toga napisati funkcije)

// TODO: TEK Kada odradimo sve strukture ujediniti hash u jedan hash fajl, da nema nepotrebih ponavljanja
