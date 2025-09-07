package bloom_filter

import (
	"bytes"
	"testing"
)

// TestNewBloomFilter tests the creation of a new Bloom Filter.
func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		expectedElements  int
		falsePositiveRate float64
		expectedM         uint32
		expectedK         uint32
	}{
		{100, 0.01, 959, 7},
		{1000, 0.05, 6236, 5},
		{5000, 0.001, 71888, 10},
	}

	for _, test := range tests {
		bf := NewBloomFilter(test.expectedElements, test.falsePositiveRate)
		if bf.m != test.expectedM {
			t.Errorf("expected m to be %d, got %d", test.expectedM, bf.m)
		}
		if bf.k != test.expectedK {
			t.Errorf("expected k to be %d, got %d", test.expectedK, bf.k)
		}
	}
}

// TestCalculateM tests the CalculateM function.
func TestCalculateM(t *testing.T) {
	tests := []struct {
		expectedElements  int
		falsePositiveRate float64
		expectedM         uint
	}{
		{100, 0.01, 959},
		{1000, 0.05, 6236},
		{5000, 0.001, 71888},
	}

	for _, test := range tests {
		m := CalculateM(test.expectedElements, test.falsePositiveRate)
		if m != test.expectedM {
			t.Errorf("expected m to be %d, got %d", test.expectedM, m)
		}
	}
}

// TestCalculateK tests the CalculateK function.
func TestCalculateK(t *testing.T) {
	tests := []struct {
		expectedElements int
		m                uint
		expectedK        uint
	}{
		{100, 960, 7},
		{1000, 6235, 5},
		{5000, 71899, 10},
	}

	for _, test := range tests {
		k := CalculateK(test.expectedElements, test.m)
		if k != test.expectedK {
			t.Errorf("expected k to be %d, got %d", test.expectedK, k)
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

		// Deserialize the B.loom Filter
		deserializedBF := Deserialize(serializedData)

		// Check if added elements are still recognized
		if bf.m != deserializedBF.m {
			t.Errorf("Bloom Filters have different m %d != %d", bf.m, deserializedBF.m)
		}
		if bf.k != deserializedBF.k {
			t.Errorf("Bloom Filters have different k %d != %d", bf.k, deserializedBF.k)
		}
		for i := range bf.h {
			for j := range 8 {
				if bf.h[i].Seed[j] != deserializedBF.h[i].Seed[j] {
					t.Errorf("Bloom Filters have different hash seeds at index %d: %d != %d", i, bf.h[i].Seed, deserializedBF.h[i].Seed)
				}
			}
		}
		for i := range len(bf.b) {
			if bf.b[i] != deserializedBF.b[i] {
				t.Errorf("Bloom Filters have different value at index %d: %d != %d", i, bf.b[i], deserializedBF.b[i])
			}
		}
		for _, elem := range test.elementsToAdd {
			if !deserializedBF.Contains([]byte(elem)) {
				t.Errorf("Deserialized Bloom Filter did not contain element %q", elem)
			}
		}

		// Ensure the serialized data matches the deserialized structure
		deserializedData := deserializedBF.Serialize()
		if !bytes.Equal(serializedData, deserializedData) {
			t.Errorf("Serialized data mismatch: original vs deserialized")
			t.Errorf("Serialized data len mismatch: original %d vs deserialized %d", len(serializedData), len(deserializedData))
		}
	}
}
