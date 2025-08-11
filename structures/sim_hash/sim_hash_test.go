package sim_hash

import (
	"encoding/hex"
	"testing"
)

// TestGenerateWordFrequency tests the GenerateWordFrequency function.
func TestGenerateWordFrequency(t *testing.T) {
	tests := []struct {
		text           string
		expectedCounts map[string]uint32
	}{
		{"hello world hello", map[string]uint32{"hello": 2, "world": 1}},
		{"apple banana apple apple", map[string]uint32{"apple": 3, "banana": 1}},
		{"go go go go go", map[string]uint32{"go": 5}},
	}

	for _, test := range tests {
		result := GenerateWordFrequency(test.text)
		for word, expectedCount := range test.expectedCounts {
			if result[word] != expectedCount {
				t.Errorf("Expected word '%s' to have count %d, got %d", word, expectedCount, result[word])
			}
		}
	}
}

// TestSimHash generates SimHashes for input texts and checks the expected bit-length and fingerprint consistency.
func TestSimHash(t *testing.T) {
	tests := []struct {
		text         string
		expectedHash string
	}{
		{"hello world hello", "e3e1efd54283d94f7081314b599d31b3"},
		{"apple banana apple apple", "32def085b783d94f708144a8363d0e8f"},
		{"go go go go go", "0880953fc0ab1be95aa0733055b380bb"},
	}

	for _, test := range tests {
		result := SimHash(test.text)
		resultHex := hex.EncodeToString(result[:])
		if resultHex != test.expectedHash {
			t.Errorf("SimHash(%s) = %s; expected %s", test.text, resultHex, test.expectedHash)
		}
	}
}

// TestHammingDistance calculates Hamming distance between two fingerprints and verifies it.
func TestHammingDistance(t *testing.T) {
	tests := []struct {
		fingerprint1 [16]byte
		fingerprint2 [16]byte
		expectedDist uint8
	}{
		{[16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0},
			[16]byte{0x98, 0x76, 0x54, 0x32, 0x10, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0xfe, 0xdc, 0xba},
			36},
		{[16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			[16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			128},
	}

	for _, test := range tests {
		result := HammingDistance(test.fingerprint1, test.fingerprint2)
		if result != test.expectedDist {
			t.Errorf("HammingDistance(%x, %x) = %d; expected %d", test.fingerprint1, test.fingerprint2, result, test.expectedDist)
		}
	}
}
