package main

type BloomFilter struct {
	m uint
	k uint
	h []HashWithSeed
	b []bool
}

func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	return &BloomFilter{
		m: m,
		k: k,
		h: CreateHashFunctions(uint32(k)),
		b: make([]bool, m),
	}
}

func (bf *BloomFilter) Add(item string) {
	data := []byte(item)
	for i := 0; i < int(bf.k); i++ {
		hash := bf.h[i].Hash(data) % uint64(bf.m)
		bf.b[hash] = true
	}
}

func (bf *BloomFilter) Check(item string) bool {
	data := []byte(item)
	for i := 0; i < int(bf.k); i++ {
		hash := bf.h[i].Hash(data) % uint64(bf.m)
		if !bf.b[hash] {
			return false
		}
	}
	return true
}
