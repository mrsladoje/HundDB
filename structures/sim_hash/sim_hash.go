package sim_hash

import (
	"hash/fnv"
	"math/bits"
	"strings"
)

// Helper to hash a word to a 64-bit integer array
func HashToIntArray(word string) []int64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(word))
	hash := hasher.Sum64()

	bits := make([]int64, 64)
	for i := 0; i < 64; i++ {
		if (hash & (1 << (63 - i))) != 0 {
			bits[i] = 1
		} else {
			bits[i] = -1
		}
	}
	return bits
}

func GenerateWordFrequency(text string) map[string]uint32 {
	wordFrequency := make(map[string]uint32)
	words := strings.Fields(strings.ToLower(text)) // TODO: Unapredi tokenizerom u utils-u
	for _, word := range words {
		wordFrequency[word]++
	}
	return wordFrequency
}

func SimHash(text string) uint64 {
	wordFrequency := GenerateWordFrequency(text)
	sum := make([]int64, 64)
	for word, count := range wordFrequency {
		bitArray := HashToIntArray(word)
		for i := 0; i < 64; i++ {
			sum[i] += bitArray[i] * int64(count)
		}
	}

	fingerprint := uint64(0)
	for i, value := range sum {
		if value >= 0 {
			fingerprint |= (1 << (63 - i))
		}
	}
	return fingerprint
}

func HammingDistance(fingerprint1, fingerprint2 uint64) uint8 {
	diff := fingerprint1 ^ fingerprint2
	return uint8(bits.OnesCount64(diff))
}
