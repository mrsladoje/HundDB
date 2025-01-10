package hyperloglog

import (
	"testing"
)

func TestNewHLL(t *testing.T) {
	tests := []struct {
		precision uint8
		expectedM uint32
		expectedP uint8
	}{
		{10, 1024, 10},
		{4, 16, 4},
		{16, 65536, 16},
	}

	for _, test := range tests {
		hll := NewHLL(test.precision)
		if hll.p != test.expectedP {
			t.Errorf("Expected precision %d, got %d", test.expectedP, hll.p)
		}
		if hll.m != test.expectedM {
			t.Errorf("Expected m %d, got %d", test.expectedM, hll.m)
		}
		if len(hll.reg) != int(hll.m) {
			t.Errorf("Expected reg length %d, got %d", hll.m, len(hll.reg))
		}
	}
}

func TestAddAndEstimate(t *testing.T) {
	tests := []struct {
		precision uint8
		elements  []string
	}{
		{10, []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}},
		{12, []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon"}},
	}

	for _, test := range tests {
		hll := NewHLL(test.precision)
		for _, elem := range test.elements {
			hll.Add([]byte(elem))
		}

		estimate := hll.Estimate()
		expectedMin := float64(len(test.elements)) * 0.9
		expectedMax := float64(len(test.elements)) * 1.1

		if estimate < expectedMin || estimate > expectedMax {
			t.Errorf("Estimate out of expected range: got %f, expected between %f and %f", estimate, expectedMin, expectedMax)
		}
	}
}

func TestEmptyCount(t *testing.T) {
	tests := []struct {
		precision          uint8
		elements           []string
		expectedEmptyCount int
	}{
		{10, []string{}, 1024},
		{10, []string{"apple"}, 1023},
	}

	for _, test := range tests {
		hll := NewHLL(test.precision)
		for _, elem := range test.elements {
			hll.Add([]byte(elem))
		}

		emptyCount := hll.emptyCount()
		if emptyCount != test.expectedEmptyCount {
			t.Errorf("Expected empty count %d, got %d", test.expectedEmptyCount, emptyCount)
		}

		for i := 0; i < int(hll.m); i++ {
			hll.reg[i] = 1
		}
		emptyCount = hll.emptyCount()
		if emptyCount != 0 {
			t.Errorf("Expected empty count 0, got %d", emptyCount)
		}
	}
}

func TestFirstKbits(t *testing.T) {
	tests := []struct {
		value    uint64
		k        uint8
		expected uint64
	}{
		{0xC5A5B1C000000000, 4, 0xC},
		{0xC5A5B1C000000000, 8, 0xC5},
		{0xC5A5B1C000000000, 16, 0xC5A5},
	}

	for _, test := range tests {
		result := firstKbits(test.value, test.k)
		if result != test.expected {
			t.Errorf("firstKbits(%064b, %d) = %064b; expected %064b", test.value, test.k, result, test.expected)
		}
	}
}

func TestTrailingZeroBits(t *testing.T) {
	tests := []struct {
		value    uint64
		expected uint8
	}{
		{0b0001, 0},
		{0b0010, 1},
		{0b0100, 2},
		{0b1000, 3},
		{0b0000, 64},
	}

	for _, test := range tests {
		result := trailingZeroBits(test.value)
		if result != test.expected {
			t.Errorf("trailingZeroBits(%b) = %d; expected %d", test.value, result, test.expected)
		}
	}
}
func TestSerializationDeserialization(t *testing.T) {
	hll := NewHLL(10)
	elements := []string{"apple", "banana", "cherry"}
	for _, elem := range elements {
		hll.Add([]byte(elem))
	}

	serialized := hll.Serialize()
	newHLL := &HLL{}
	err := newHLL.Deserialize(serialized)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if newHLL.p != hll.p || newHLL.m != hll.m {
		t.Errorf("Deserialized HLL does not match original: p=%d, m=%d", newHLL.p, newHLL.m)
	}

	if !equalRegisters(hll.reg, newHLL.reg) {
		t.Error("Registers do not match after deserialization")
	}
}

func equalRegisters(a, b []uint8) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestEstimateEmptyRegisters(t *testing.T) {
	hll := NewHLL(10)
	estimate := hll.Estimate()
	if estimate != 0 {
		t.Errorf("Expected estimate for empty HLL to be 0, got %f", estimate)
	}
}
