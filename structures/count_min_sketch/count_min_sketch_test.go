package count_min_sketch

import (
	"bytes"
	"testing"
)

// TestNewCMS tests the creation of a new Count-Min Sketch.
func TestNewCMS(t *testing.T) {
	tests := []struct {
		epsilon   float64
		delta     float64
		expectedM uint
		expectedK uint
	}{
		{0.01, 0.99, CalculateM(0.01), CalculateK(0.99)},
		{0.05, 0.95, CalculateM(0.05), CalculateK(0.95)},
		{0.001, 0.999, CalculateM(0.001), CalculateK(0.999)},
	}

	for _, test := range tests {
		cms := NewCMS(test.epsilon, test.delta)
		if cms.m != uint32(test.expectedM) {
			t.Errorf("Expected m = %d, got %d", test.expectedM, cms.m)
		}
		if cms.k != uint32(test.expectedK) {
			t.Errorf("Expected k = %d, got %d", test.expectedK, cms.k)
		}
	}
}

// TestCalculateM ensures the m value is calculated correctly.
func TestCalculateM(t *testing.T) {
	tests := []struct {
		epsilon   float64
		expectedM uint
	}{
		{0.01, uint(CalculateM(0.01))},
		{0.05, uint(CalculateM(0.05))},
		{0.001, uint(CalculateM(0.001))},
	}

	for _, test := range tests {
		m := CalculateM(test.epsilon)
		if m != test.expectedM {
			t.Errorf("Expected m = %d, got %d", test.expectedM, m)
		}
	}
}

// TestCalculateK ensures the k value is calculated correctly.
func TestCalculateK(t *testing.T) {
	tests := []struct {
		delta     float64
		expectedK uint
	}{
		{0.99, uint(CalculateK(0.99))},
		{0.95, uint(CalculateK(0.95))},
		{0.999, uint(CalculateK(0.999))},
	}

	for _, test := range tests {
		k := CalculateK(test.delta)
		if k != test.expectedK {
			t.Errorf("Expected k = %d, got %d", test.expectedK, k)
		}
	}
}

// TestCMSAddAndCount checks the accuracy of adding elements and estimating frequency.
func TestCMSAddAndCount(t *testing.T) {
	tests := []struct {
		epsilon       float64
		delta         float64
		elementsToAdd []string
	}{
		{0.01, 0.99, []string{"apple", "banana", "cherry"}},
		{0.05, 0.95, []string{"dog", "cat", "mouse"}},
	}

	for _, test := range tests {
		cms := NewCMS(test.epsilon, test.delta)

		// Add elements
		for _, elem := range test.elementsToAdd {
			cms.Add([]byte(elem))
		}

		// Ensure elements are counted correctly
		for _, elem := range test.elementsToAdd {
			if cms.Count([]byte(elem)) == 0 {
				t.Errorf("Element %q was added but count is 0", elem)
			}
		}

		// Ensure a non-existent element has a count of 0 or very low
		nonExistent := "non-existent"
		if cms.Count([]byte(nonExistent)) > 1 {
			t.Errorf("Non-existent element %q has an unexpected count: %d", nonExistent, cms.Count([]byte(nonExistent)))
		}
	}
}

// TestCMSSerialization ensures serialization and deserialization work correctly.
func TestCMSSerialization(t *testing.T) {
	tests := []struct {
		epsilon       float64
		delta         float64
		elementsToAdd []string
	}{
		{0.01, 0.99, []string{"apple", "banana", "cherry"}},
		{0.05, 0.95, []string{"dog", "cat", "mouse"}},
	}

	for _, test := range tests {
		cms := NewCMS(test.epsilon, test.delta)

		// Add elements
		for _, elem := range test.elementsToAdd {
			cms.Add([]byte(elem))
		}

		// Serialize CMS
		serializedData := cms.Serialize()

		// Deserialize into a new CMS instance
		deserializedCMS := Deserialize(serializedData)

		// Ensure the structure matches
		if cms.m != deserializedCMS.m {
			t.Errorf("Deserialized CMS has different m value: %d != %d", cms.m, deserializedCMS.m)
		}
		if cms.k != deserializedCMS.k {
			t.Errorf("Deserialized CMS has different k value: %d != %d", cms.k, deserializedCMS.k)
		}

		// Check hash function seeds
		for i := range cms.h {
			if !bytes.Equal(cms.h[i].Serialize(), deserializedCMS.h[i].Serialize()) {
				t.Errorf("Hash function mismatch at index %d", i)
			}
		}

		// Check table values
		for i := uint32(0); i < cms.k; i++ {
			for j := uint32(0); j < cms.m; j++ {
				if cms.table[i][j] != deserializedCMS.table[i][j] {
					t.Errorf("Table mismatch at (%d, %d): %d != %d", i, j, cms.table[i][j], deserializedCMS.table[i][j])
				}
			}
		}

		// Ensure elements are still counted correctly
		for _, elem := range test.elementsToAdd {
			originalCount := cms.Count([]byte(elem))
			deserializedCount := deserializedCMS.Count([]byte(elem))
			if originalCount != deserializedCount {
				t.Errorf("Deserialized CMS count mismatch for '%s': expected %d, got %d", elem, originalCount, deserializedCount)
			}
		}

		// Ensure serialized data is consistent
		deserializedData := deserializedCMS.Serialize()
		if !bytes.Equal(serializedData, deserializedData) {
			t.Errorf("Serialized data mismatch: original vs deserialized")
		}
	}
}
