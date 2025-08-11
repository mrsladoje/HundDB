package sim_hash

//cSpell:ignore hund

import (
	"hash/fnv"
	"hund_db/utils/tokenizer"
	"math/bits"
)

// GenerateWordFrequency generates a frequency map of words in the text.
// text: the input text to be processed.
// TODO: maybe should be implemented using CMS, will consult with TA
func GenerateWordFrequency(text string) map[string]uint32 {
	wordFrequency := make(map[string]uint32)
	words := tokenizer.ProcessText(text)
	for _, word := range words {
		wordFrequency[word]++
	}
	return wordFrequency
}

// SimHash generates a 128-bit fingerprint that describes the given text.
// It works by hashing each word, converting the hash to a bit array,
// and creating a weighted sum of the bit arrays. Then it's converted back to a 128-bit value.
// text: the input text to be processed.
func SimHash(text string) [16]byte {
	wordFrequency := GenerateWordFrequency(text)
	sum := make([]int64, 128)
	hasher := fnv.New128a()

	for word, count := range wordFrequency {
		hasher.Reset()
		hasher.Write([]byte(word))
		hash := hasher.Sum(nil)

		for i := 0; i < 128; i++ {
			bit := int64(1)
			if (hash[i/8] & (1 << uint(7-i%8))) == 0 {
				bit = -1
			}
			sum[i] += bit * int64(count)
		}
	}

	var fingerprint [16]byte
	for i, value := range sum {
		// Sets the bits to 1
		if value >= 0 {
			fingerprint[i/8] |= (1 << uint(7-i%8))
		}
	}
	return fingerprint
}

// HammingDistance calculates the Hamming distance between two 128-bit fingerprints.
// It returns the number of positions at which the corresponding bits are different.
// fingerprint1: the first fingerprint to compare.
// fingerprint2: the second fingerprint to compare.
func HammingDistance(fingerprint1, fingerprint2 [16]byte) uint8 {
	distance := uint8(0)
	for i := 0; i < 16; i++ {
		distance += uint8(bits.OnesCount8(fingerprint1[i] ^ fingerprint2[i]))
	}
	return distance
}
