package main

import (
	"hash/fnv"
	"strings"
)

// helper to hash a word to a 64-bit integer array
func HashToIntArray(word string) []int {
	hasher := fnv.New64a()
	hasher.Write([]byte(word))
	hash := hasher.Sum64()

	bits := make([]int, 64)
	for i := 0; i < 64; i++ {
		if (hash & (1 << (63 - i))) != 0 {
			bits[i] = 1
		} else {
			bits[i] = -1
		}
	}
	return bits
}

func SimHash(text string) uint64 {
	wordCounts := make(map[string]int)
	words := strings.Fields(strings.ToLower(text)) // Split by spaces and lowercase

	for _, word := range words { // count the frequency of each word
		wordCounts[word]++
	}

	sum := make([]int, 64) // 64-bit fingerprint

	for word, count := range wordCounts {
		bitArray := HashToIntArray(word)
		for i := 0; i < 64; i++ {
			sum[i] += bitArray[i] * count // Weight by word frequency
		}
	}

	var fingerprint uint64
	for i, value := range sum {
		if value > 0 {
			fingerprint |= (1 << (63 - i)) // Set bit to 1 if positive
		}
	}

	return fingerprint
}

func HammingDistance(fp1, fp2 uint64) int {

	diff := fp1 ^ fp2

	count := 0
	for diff != 0 {
		count += int(diff & 1) // Add 1 if the least significant bit is set
		diff >>= 1             // Shift the bits to the right
	}

	return count
}
