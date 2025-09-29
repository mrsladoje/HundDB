package sim_hash

import (
	"encoding/binary"
	"encoding/hex"

	"fmt"
	"hash/fnv"
	"math/bits"

	block_manager "hunddb/lsm/block_manager"
	crc_util "hunddb/utils/crc"
	"hunddb/utils/tokenizer"
)

// SimHashFingerprint is a wrapper for the 128-bit SimHash value.
// It implements encoding.TextMarshaler and encoding.TextUnmarshaler,
// allowing it to be seamlessly serialized to and deserialized from
// a hex-encoded string, which is ideal for disk storage (e.g., in JSON or database fields).
type SimHashFingerprint struct {
	// Hash is the 128-bit array returned by the SimHash function.
	Hash [16]byte
}

// NewSimHashFingerprint creates a new SimHashFingerprint struct from the raw [16]byte array.
func NewSimHashFingerprint(hash [16]byte) SimHashFingerprint {
	return SimHashFingerprint{Hash: hash}
}

// NewSimHashFingerprintFromText creates a SimHashFingerprint from text input.
func NewSimHashFingerprintFromText(text string) SimHashFingerprint {
	hash := SimHash(text)
	return SimHashFingerprint{Hash: hash}
}

// HammingDistance calculates the Hamming distance between two SimHashFingerprints.
// This is a convenient wrapper around the package-level function.
func (f SimHashFingerprint) HammingDistance(other SimHashFingerprint) uint8 {
	return HammingDistance(f.Hash, other.Hash)
}

// String returns the hash as a 32-character hexadecimal string.
// This is useful for printing and debugging.
func (f SimHashFingerprint) String() string {
	return hex.EncodeToString(f.Hash[:])
}

// Bytes returns the raw 16-byte array for direct access.
func (f SimHashFingerprint) Bytes() [16]byte {
	return f.Hash
}

// Equal returns true if two fingerprints are identical.
func (f SimHashFingerprint) Equal(other SimHashFingerprint) bool {
	return f.Hash == other.Hash
}

// IsZero returns true if the fingerprint is empty (all zeros).
func (f SimHashFingerprint) IsZero() bool {
	var zero [16]byte
	return f.Hash == zero
}

// MarshalText implements the encoding.TextMarshaler interface.
// It converts the hash into its hexadecimal string representation for serialization.
func (f SimHashFingerprint) MarshalText() ([]byte, error) {
	// Return the hex representation of the 16-byte hash.
	return []byte(f.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
// It converts a hexadecimal string back into the 16-byte hash array during deserialization.
func (f *SimHashFingerprint) UnmarshalText(text []byte) error {
	decoded, err := hex.DecodeString(string(text))
	if err != nil {
		return fmt.Errorf("sim_hash: failed to decode hex string: %w", err)
	}

	if len(decoded) != 16 {
		return fmt.Errorf("sim_hash: expected 16 bytes, got %d", len(decoded))
	}

	// Copy the decoded slice back into the fixed-size array
	copy(f.Hash[:], decoded)

	return nil
}

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

// SaveToDisk saves the SimHashFingerprint to disk with the given name
// name: identifier for the saved file (e.g., "document_123")
// The file will be saved as "sim_hash_fingerprint_{name}"
func (f SimHashFingerprint) SaveToDisk(name string) error {
	filename := fmt.Sprintf("sim_hash_fingerprint_%s", name)

	// Create file data: [size (8B) + hash data (16B)]
	totalSize := 8 + 16
	fileData := make([]byte, totalSize)

	// Write size header (16 for SimHash fingerprint)
	binary.LittleEndian.PutUint64(fileData[0:8], 16)

	// Copy hash data
	copy(fileData[8:], f.Hash[:])

	// Add CRC blocks and write to disk
	dataWithCRC := crc_util.AddCRCsToData(fileData)

	blockManager := block_manager.GetBlockManager()
	return blockManager.WriteToDisk(dataWithCRC, filename, 0)
}

// LoadFromDisk loads fingerprint data from disk into this instance
// name: identifier for the saved file (e.g., "document_123")
func (f *SimHashFingerprint) LoadFromDisk(name string) error {
	filename := fmt.Sprintf("sim_hash_fingerprint_%s", name)
	blockManager := block_manager.GetBlockManager()
	// Read size header (first 8 bytes)
	sizeBytes, _, err := blockManager.ReadFromDisk(filename, 0, 8)
	if err != nil {
		return fmt.Errorf("file not found or corrupted: %v", err)
	}
	dataSize := binary.LittleEndian.Uint64(sizeBytes)
	if dataSize != 16 {
		return fmt.Errorf("invalid SimHash size: expected 16, got %d", dataSize)
	}
	// Read hash data starting from offset 8 + CRC_SIZE
	// (BlockManager accounts for CRC internally)
	hashData, _, err := blockManager.ReadFromDisk(filename, 8+4, 16)
	if err != nil {
		return fmt.Errorf("failed to read hash data: %v", err)
	}

	// Copy to fingerprint hash
	copy(f.Hash[:], hashData)

	return nil
}

// LoadSimHashFingerprintFromDisk creates a new SimHashFingerprint and loads data from disk
// name: identifier for the saved file (e.g., "document_123")
// Returns a new SimHashFingerprint instance with loaded data
func LoadSimHashFingerprintFromDisk(name string) (SimHashFingerprint, error) {
	filename := fmt.Sprintf("sim_hash_fingerprint_%s", name)
	blockManager := block_manager.GetBlockManager()
	var result SimHashFingerprint
	// Read size header (first 8 bytes)
	sizeBytes, _, err := blockManager.ReadFromDisk(filename, 0, 8)
	if err != nil {
		return result, fmt.Errorf("file not found or corrupted: %v", err)
	}
	dataSize := binary.LittleEndian.Uint64(sizeBytes)
	if dataSize != 16 {
		return result, fmt.Errorf("invalid SimHash size: expected 16, got %d", dataSize)
	}
	// Read hash data starting from offset 8 + CRC_SIZE
	// (BlockManager accounts for CRC internally)
	hashData, _, err := blockManager.ReadFromDisk(filename, 8+4, 16)
	if err != nil {
		return result, fmt.Errorf("failed to read hash data: %v", err)
	}

	// Copy to result hash
	copy(result.Hash[:], hashData)

	return result, nil
}
