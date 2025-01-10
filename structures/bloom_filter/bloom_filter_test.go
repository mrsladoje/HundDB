package bloom_filter

import (
	"bytes"
	"testing"
)

// The test returs false, due to rounding issues
func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		expectedElements  int
		falsePositiveRate float64
		expectedM         uint32
		expectedK         uint32
	}{
		{100, 0.01, 960, 7},
		{1000, 0.05, 6240, 5},
		{5000, 0.001, 71888, 10},
	}

	for _, test := range tests {
		bf := NewBloomFilter(test.expectedElements, test.falsePositiveRate)
		if bf.m != test.expectedM {
			t.Errorf("Expected m %d, got %d", test.expectedM, bf.m)
		}
		if bf.k != test.expectedK {
			t.Errorf("Expected k %d, got %d", test.expectedK, bf.k)
		}
		if len(bf.b)*8 != int(bf.m) {
			t.Errorf("Expected bit array size %d, got %d", bf.m, len(bf.b)*8)
		}
	}
}

func TestBloomFilterAddAndContains(t *testing.T) {
	tests := []struct {
		expectedElements  int
		falsePositiveRate float64
		elementsToAdd     []string
		nonExistent       []string
	}{
		{
			100, 0.01,
			[]string{"apple", "banana", "cherry"},
			[]string{"date", "elderberry"},
		},
		{
			200, 0.05,
			[]string{"grape", "kiwi", "lemon"},
			[]string{"mango", "nectarine"},
		},
	}

	for _, test := range tests {
		bf := NewBloomFilter(test.expectedElements, test.falsePositiveRate)

		for _, elem := range test.elementsToAdd {
			bf.Add([]byte(elem))
		}

		// Check if added elements are likely present
		for _, elem := range test.elementsToAdd {
			if !bf.Contains([]byte(elem)) {
				t.Errorf("Element %q was added but not found in the Bloom filter", elem)
			}
		}

		// Check if non-existent elements are not mistakenly marked as present
		falsePositiveCount := 0
		for _, elem := range test.nonExistent {
			if bf.Contains([]byte(elem)) {
				falsePositiveCount++
			}
		}
		if falsePositiveCount > 0 {
			t.Logf("False positives: %d out of %d", falsePositiveCount, len(test.nonExistent))
		}
	}
}

func TestBloomFilterSerialization(t *testing.T) {
	tests := []struct {
		expectedElements  int
		falsePositiveRate float64
		elementsToAdd     []string
	}{
		{
			100, 0.01,
			[]string{"apple", "banana", "cherry"},
		},
		{
			200, 0.05,
			[]string{"grape", "kiwi", "lemon"},
		},
	}

	for _, test := range tests {
		bf := NewBloomFilter(test.expectedElements, test.falsePositiveRate)

		// Add elements
		for _, elem := range test.elementsToAdd {
			bf.Add([]byte(elem))
		}

		// Serialize the Bloom Filter
		serializedData := bf.Serialize()

		// Deserialize the Bloom Filter
		deserializedBF, err := Deserialize(serializedData)
		if err != nil {
			t.Fatalf("Deserialization failed: %v", err)
		}

		// Check if added elements are still recognized
		for _, elem := range test.elementsToAdd {
			if !deserializedBF.Contains([]byte(elem)) {
				t.Errorf("Deserialized Bloom Filter did not contain element %q", elem)
			}
		}

		// Ensure the serialized data matches the deserialized structure
		deserializedData := deserializedBF.Serialize()
		if err != nil {
			t.Fatalf("Serialization of deserialized filter failed: %v", err)
		}
		if !bytes.Equal(serializedData, deserializedData) {
			t.Errorf("Serialized data mismatch: original vs deserialized")
		}
	}
}
