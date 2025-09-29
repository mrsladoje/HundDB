package sim_hash

import (
	"encoding/hex"
	"encoding/json"
	"os"
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

// TestSimHashFingerprint tests the SimHashFingerprint struct functionality
func TestSimHashFingerprint(t *testing.T) {
	// Test NewSimHashFingerprint
	hash := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	fingerprint := NewSimHashFingerprint(hash)
	if fingerprint.Hash != hash {
		t.Errorf("NewSimHashFingerprint failed: expected %x, got %x", hash, fingerprint.Hash)
	}
}

// TestNewSimHashFingerprintFromText tests creating fingerprint from text
func TestNewSimHashFingerprintFromText(t *testing.T) {
	text := "hello world hello"
	fingerprint := NewSimHashFingerprintFromText(text)
	expectedHash := SimHash(text)
	if fingerprint.Hash != expectedHash {
		t.Errorf("NewSimHashFingerprintFromText failed: expected %x, got %x", expectedHash, fingerprint.Hash)
	}
}

// TestSimHashFingerprintString tests the String method
func TestSimHashFingerprintString(t *testing.T) {
	hash := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	fingerprint := NewSimHashFingerprint(hash)
	expected := "123456789abcdef0123456789abcdef0"
	result := fingerprint.String()
	if result != expected {
		t.Errorf("String() failed: expected %s, got %s", expected, result)
	}
}

// TestSimHashFingerprintEqual tests the Equal method
func TestSimHashFingerprintEqual(t *testing.T) {
	hash1 := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	hash2 := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	hash3 := [16]byte{0x98, 0x76, 0x54, 0x32, 0x10, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0xfe, 0xdc, 0xba}

	fp1 := NewSimHashFingerprint(hash1)
	fp2 := NewSimHashFingerprint(hash2)
	fp3 := NewSimHashFingerprint(hash3)

	if !fp1.Equal(fp2) {
		t.Error("Equal() failed: identical hashes should be equal")
	}

	if fp1.Equal(fp3) {
		t.Error("Equal() failed: different hashes should not be equal")
	}
}

// TestSimHashFingerprintIsZero tests the IsZero method
func TestSimHashFingerprintIsZero(t *testing.T) {
	var zeroHash [16]byte
	nonZeroHash := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}

	zeroFp := NewSimHashFingerprint(zeroHash)
	nonZeroFp := NewSimHashFingerprint(nonZeroHash)

	if !zeroFp.IsZero() {
		t.Error("IsZero() failed: zero hash should return true")
	}

	if nonZeroFp.IsZero() {
		t.Error("IsZero() failed: non-zero hash should return false")
	}
}

// TestSimHashFingerprintHammingDistance tests the HammingDistance method
func TestSimHashFingerprintHammingDistance(t *testing.T) {
	hash1 := [16]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	hash2 := [16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	fp1 := NewSimHashFingerprint(hash1)
	fp2 := NewSimHashFingerprint(hash2)

	distance := fp1.HammingDistance(fp2)
	expected := uint8(128) // All bits different

	if distance != expected {
		t.Errorf("HammingDistance() failed: expected %d, got %d", expected, distance)
	}
}

// TestSimHashFingerprintMarshalUnmarshalText tests the text marshaling
func TestSimHashFingerprintMarshalUnmarshalText(t *testing.T) {
	original := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	fingerprint := NewSimHashFingerprint(original)
	// Test MarshalText
	marshaled, err := fingerprint.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText() failed: %v", err)
	}
	// Test UnmarshalText
	var unmarshaled SimHashFingerprint
	err = unmarshaled.UnmarshalText(marshaled)
	if err != nil {
		t.Fatalf("UnmarshalText() failed: %v", err)
	}
	if !fingerprint.Equal(unmarshaled) {
		t.Errorf("Marshal/Unmarshal round trip failed: expected %x, got %x", fingerprint.Hash, unmarshaled.Hash)
	}
}

// TestSimHashFingerprintSaveLoadDisk tests disk persistence
func TestSimHashFingerprintSaveLoadDisk(t *testing.T) {
	// Cleanup test files after test completes
	defer func() {
		os.Remove("sim_hash_fingerprint_test_fingerprint")
	}()

	original := NewSimHashFingerprintFromText("test document for persistence")

	// Test SaveToDisk
	err := original.SaveToDisk("test_fingerprint")
	if err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}
	// Test LoadSimHashFingerprintFromDisk
	loaded, err := LoadSimHashFingerprintFromDisk("test_fingerprint")
	if err != nil {
		t.Fatalf("LoadSimHashFingerprintFromDisk failed: %v", err)
	}

	if !original.Equal(loaded) {
		t.Errorf("Save/Load round trip failed: expected %x, got %x", original.Hash, loaded.Hash)
	}

	// Test LoadFromDisk method
	var instance SimHashFingerprint
	err = instance.LoadFromDisk("test_fingerprint")
	if err != nil {
		t.Fatalf("LoadFromDisk method failed: %v", err)
	}
	if !original.Equal(instance) {
		t.Errorf("LoadFromDisk method round trip failed: expected %x, got %x", original.Hash, instance.Hash)
	}
}

// TestSimHashFingerprintJSONMarshal tests JSON marshaling compatibility
func TestSimHashFingerprintJSONMarshal(t *testing.T) {
	type TestDoc struct {
		ID          string             `json:"id"`
		Title       string             `json:"title"`
		Fingerprint SimHashFingerprint `json:"simhash"`
	}
	original := TestDoc{
		ID:          "doc-123",
		Title:       "Test Document",
		Fingerprint: NewSimHashFingerprintFromText("this is a test document"),
	}
	// Marshal to JSON
	jsonData, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("JSON Marshal failed: %v", err)
	}
	// Unmarshal from JSON
	var unmarshaled TestDoc
	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("JSON Unmarshal failed: %v", err)
	}
	// Verify round trip
	if unmarshaled.ID != original.ID {
		t.Errorf("JSON round trip failed for ID: expected %s, got %s", original.ID, unmarshaled.ID)
	}

	if unmarshaled.Title != original.Title {
		t.Errorf("JSON round trip failed for Title: expected %s, got %s", original.Title, unmarshaled.Title)
	}

	if !original.Fingerprint.Equal(unmarshaled.Fingerprint) {
		t.Errorf("JSON round trip failed for Fingerprint: expected %x, got %x", original.Fingerprint.Hash, unmarshaled.Fingerprint.Hash)
	}
}
