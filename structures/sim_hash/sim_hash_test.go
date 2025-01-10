package sim_hash

import (
	"testing"
)

// TestGenerateWordFrequency checks that the GenerateWordFrequency function returns the correct word counts.
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
		expectedHash uint64
	}{
		{"hello world hello", 0xA430D84680AABD0B},
		{"apple banana apple apple", 0xF74A62A458BEFDBF},
		{"go go go go go", 0x8953907B53F670B},
	}

	for _, test := range tests {
		result := SimHash(test.text)
		if result != test.expectedHash {
			t.Errorf("SimHash(%s) = %064b; expected %064b", test.text, result, test.expectedHash)
		}
	}
}

// TestHammingDistance calculates Hamming distance between two fingerprints and verifies it.
func TestHammingDistance(t *testing.T) {
	tests := []struct {
		fingerprint1 uint64
		fingerprint2 uint64
		expectedDist uint8
	}{
		{0xabc123def4567890, 0x9876543210abcdef, 44}, // Replace with actual expected values
		{0x123456789abcdef0, 0x123456789abcdef1, 1},
		{0x0, 0xFFFFFFFFFFFFFFFF, 64},
	}

	for _, test := range tests {
		result := HammingDistance(test.fingerprint1, test.fingerprint2)
		if result != test.expectedDist {
			t.Errorf("HammingDistance(%064b, %064b) = %d; expected %d", test.fingerprint1, test.fingerprint2, result, test.expectedDist)
		}
	}
}
