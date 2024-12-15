package main

type CMS struct {
	m     uint // duzina setova (u ovom slucaju kolona)
	k     uint // broj hash funkcija (u ovom slucaju redova)
	hash  []HashWithSeed
	table [][]int
}

func NewCMS(epsilon float64, delta float64) *CMS {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	matrix := make([][]int, k)
	for i := range matrix {
		matrix[i] = make([]int, m)
	}

	return &CMS{
		m:     m,
		k:     k,
		hash:  CreateHashFunctions(k),
		table: matrix,
	}
}

func (cms *CMS) Add(item string) {
	data := []byte(item)
	for i := 0; i < int(cms.k); i++ {
		j := cms.hash[i].Hash(data) % uint64(cms.m)
		cms.table[i][j] += 1
	}
}

func (cms *CMS) Check(item string) int {
	data := []byte(item)
	min := int(^uint(0) >> 1)

	for i := 0; i < int(cms.k); i++ {
		j := cms.hash[i].Hash(data) % uint64(cms.m)
		if cms.table[i][j] < min {
			min = cms.table[i][j]
		}
	}
	return min
}
