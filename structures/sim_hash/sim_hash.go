package sim_hash

import (
	"hash/fnv"
	"math/bits"
	"strings"
)

// GenerateWordFrequency generates a frequency map of words in the text.
// text: the input text to be processed.
// TODO: maybe should be implemented using CMS, will consult with TA
func GenerateWordFrequency(text string) map[string]uint32 {
	wordFrequency := make(map[string]uint32)
	words := strings.Fields(strings.ToLower(text)) // TODO: MAKE A UTILS TOKENIZER
	for _, word := range words {
		wordFrequency[word]++
	}
	return wordFrequency
}

// SimHash generates a 64-bit fingerprint that describes the given text.
// It works by hashing each word, converting the hash to a bit array,
// and creating a weighted sum of the bit arrays. then its converted back to a 64-bit value.
// text: the input text to be processed.
func SimHash(text string) uint64 {
	wordFrequency := GenerateWordFrequency(text)
	sum := make([]int64, 64)
	hasher := fnv.New64a()

	for word, count := range wordFrequency {
		hasher.Reset()
		hasher.Write([]byte(word))
		hash := hasher.Sum64()

		for i := 0; i < 64; i++ {
			bit := int64(1)
			if (hash & (1 << (63 - i))) == 0 {
				bit = -1
			}
			sum[i] += bit * int64(count)
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

// HammingDistance calculates the Hamming distance between two 64-bit fingerprints.
// It returns the number of positions at which the corresponding bits are different.
// fingerprint1: the first fingerprint to compare.
// fingerprint2: the second fingerprint to compare.
func HammingDistance(fingerprint1, fingerprint2 uint64) uint8 {
	diff := fingerprint1 ^ fingerprint2
	return uint8(bits.OnesCount64(diff))
}

// TODO: Serijalizacija
// TODO: Deserijalizacija
// TODO: Napisati testove (pogledati primer 5 sa trecih vezbi, i nalik toga napisati funkcije)

// TODO: TEK Kada odradimo sve strukture ujediniti hash u jedan hash fajl, da nema nepotrebih ponavljanja
