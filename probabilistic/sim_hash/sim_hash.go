package sim_hash

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/bits"

	block_manager "hunddb/lsm/block_manager"
	crc_util "hunddb/utils/crc"
	"hunddb/utils/tokenizer"
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

// SaveSimHashToDisk saves a SimHash fingerprint to disk with the given name
func SaveSimHashToDisk(hash [16]byte, name string) error {
	filename := fmt.Sprintf("probabilistic/sim_hash_%s", name)
	
	// SimHash is always 16 bytes, so create file data: [size (8B) + hash (16B)]
	totalSize := 8 + 16
	fileData := make([]byte, totalSize)
	
	// Write size header (16 for SimHash)
	binary.LittleEndian.PutUint64(fileData[0:8], 16)
	
	// Copy hash data
	copy(fileData[8:], hash[:])
	
	// Add CRC blocks and write to disk
	dataWithCRC := crc_util.AddCRCsToData(fileData)
	
	blockManager := block_manager.GetBlockManager()
	return blockManager.WriteToDisk(dataWithCRC, filename, 0)
}

// LoadSimHashFromDisk loads a SimHash fingerprint from disk with the given name
func LoadSimHashFromDisk(name string) ([16]byte, error) {
	filename := fmt.Sprintf("probabilistic/sim_hash_%s", name)
	blockManager := block_manager.GetBlockManager()
	var result [16]byte
	
	// Read size header (first 8 bytes)
	sizeBytes, _, err := blockManager.ReadFromDisk(filename, 0, 8)
	if err != nil {
		return result, fmt.Errorf("file not found or corrupted: %v", err)
	}
	
	dataSize := binary.LittleEndian.Uint64(sizeBytes)
	if dataSize != 16 {
		return result, fmt.Errorf("invalid SimHash size: expected 16, got %d", dataSize)
	}
	
	// Read hash data starting from offset 8
	hashData, _, err := blockManager.ReadFromDisk(filename, 8, 16)
	if err != nil {
		return result, fmt.Errorf("failed to read hash data: %v", err)
	}
	
	// Copy to result array
	copy(result[:], hashData)
	return result, nil
}
