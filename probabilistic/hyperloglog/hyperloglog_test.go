package hyperloglog

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
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
		hll, err := NewHLL(test.precision)
		if err != nil {
			t.Fatalf("Failed to create HLL: %v", err)
		}
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
		hll, err := NewHLL(test.precision)
		if err != nil {
			t.Fatalf("Failed to create HLL: %v", err)
		}
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
		hll, err := NewHLL(test.precision)
		if err != nil {
			t.Fatalf("Failed to create HLL: %v", err)
		}
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

func TestHLLSerialization(t *testing.T) {
	// Create a new HyperLogLog instance with precision 10
	hllOriginal, err := NewHLL(10)
	if err != nil {
		t.Fatalf("Failed to create HLL: %v", err)
	}

	// Add some elements to it
	hllOriginal.Add([]byte("apple"))
	hllOriginal.Add([]byte("banana"))
	hllOriginal.Add([]byte("cherry"))

	// Serialize the HyperLogLog
	serializedData := hllOriginal.Serialize()

	// Create a new HyperLogLog instance and deserialize into it
	hllDeserialized := Deserialize(serializedData)

	// Check if precision and size match
	if hllOriginal.p != hllDeserialized.p {
		t.Errorf("Precision mismatch: expected %d, got %d", hllOriginal.p, hllDeserialized.p)
	}

	if hllOriginal.m != hllDeserialized.m {
		t.Errorf("Size mismatch: expected %d, got %d", hllOriginal.m, hllDeserialized.m)
	}

	// Check if registers match
	if !bytes.Equal(hllOriginal.reg, hllDeserialized.reg) {
		t.Errorf("Register mismatch: original and deserialized data are different")
	}

	// Ensure the serialized data matches the deserialized structure
	deserializedData := hllDeserialized.Serialize()
	if !bytes.Equal(serializedData, deserializedData) {
		t.Errorf("Serialized data mismatch: original vs deserialized")
		t.Errorf("Serialized data len mismatch: original %d vs deserialized %d", len(serializedData), len(deserializedData))
	}
}

// Test thread-safety for concurrent Add operations
func TestConcurrentAdd(t *testing.T) {
	hll, err := NewHLL(10)
	if err != nil {
		t.Fatalf("Failed to create HLL: %v", err)
	}

	numGoroutines := 100
	numElementsPerGoroutine := 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numElementsPerGoroutine; j++ {
				element := fmt.Sprintf("element_%d_%d", goroutineID, j)
				hll.Add([]byte(element))
			}
		}(i)
	}

	wg.Wait()

	// Check that the estimate is reasonable
	estimate := hll.Estimate()
	expectedElements := float64(numGoroutines * numElementsPerGoroutine)

	// Allow for 20% variance due to probabilistic nature
	if estimate < expectedElements*0.8 || estimate > expectedElements*1.2 {
		t.Errorf("Estimate out of expected range: got %f, expected around %f", estimate, expectedElements)
	}
}

// Test thread-safety for concurrent Add and Estimate operations
func TestConcurrentAddAndEstimate(t *testing.T) {
	hll, err := NewHLL(12)
	if err != nil {
		t.Fatalf("Failed to create HLL: %v", err)
	}

	numAdders := 50
	numReaders := 10
	duration := 100 * time.Millisecond
	var wg sync.WaitGroup

	// Start adder goroutines
	for i := 0; i < numAdders; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			start := time.Now()
			counter := 0
			for time.Since(start) < duration {
				element := fmt.Sprintf("element_%d_%d", goroutineID, counter)
				hll.Add([]byte(element))
				counter++
			}
		}(i)
	}

	// Start reader goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			for time.Since(start) < duration {
				_ = hll.Estimate()
				_ = hll.emptyCount()
				_ = hll.GetPrecision()
				_ = hll.GetSize()
				time.Sleep(time.Microsecond) // Small delay to allow more interleaving
			}
		}()
	}

	wg.Wait()

	// Final estimate should be reasonable
	finalEstimate := hll.Estimate()
	if finalEstimate <= 0 {
		t.Errorf("Final estimate should be positive, got %f", finalEstimate)
	}
}

// Test thread-safety for serialization during concurrent operations
func TestConcurrentSerialization(t *testing.T) {
	hll, err := NewHLL(10)
	if err != nil {
		t.Fatalf("Failed to create HLL: %v", err)
	}

	// Add some initial data
	for i := 0; i < 100; i++ {
		hll.Add([]byte(fmt.Sprintf("initial_%d", i)))
	}

	var wg sync.WaitGroup
	duration := 50 * time.Millisecond

	// Start serialization goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			for time.Since(start) < duration {
				data := hll.Serialize()
				if len(data) == 0 {
					t.Errorf("Serialization returned empty data")
				}
			}
		}()
	}

	// Start add goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			start := time.Now()
			counter := 0
			for time.Since(start) < duration {
				element := fmt.Sprintf("concurrent_%d_%d", goroutineID, counter)
				hll.Add([]byte(element))
				counter++
			}
		}(i)
	}

	wg.Wait()
}

// Benchmark thread-safe operations
func BenchmarkConcurrentAdd(b *testing.B) {
	hll, _ := NewHLL(12)

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			element := fmt.Sprintf("element_%d", counter)
			hll.Add([]byte(element))
			counter++
		}
	})
}

func BenchmarkConcurrentEstimate(b *testing.B) {
	hll, _ := NewHLL(12)

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		hll.Add([]byte(fmt.Sprintf("element_%d", i)))
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = hll.Estimate()
		}
	})
}

func BenchmarkConcurrentSerialization(b *testing.B) {
	hll, _ := NewHLL(12)

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		hll.Add([]byte(fmt.Sprintf("element_%d", i)))
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = hll.Serialize()
		}
	})
}

// TestSaveToDisk tests saving HyperLogLog to disk
func TestSaveToDisk(t *testing.T) {
	hll, err := NewHLL(10)
	if err != nil {
		t.Fatalf("Failed to create HLL: %v", err)
	}
	
	// Add some test data
	testElements := []string{"user1", "user2", "user3", "user1", "user4", "user2"}
	for _, element := range testElements {
		hll.Add([]byte(element))
	}
	
	// Save to disk
	err = hll.SaveToDisk("test_hll")
	if err != nil {
		t.Fatalf("Failed to save HLL to disk: %v", err)
	}
}

// TestLoadFromDisk tests loading HyperLogLog from disk
func TestLoadFromDisk(t *testing.T) {
	// Create and populate original HLL
	original, err := NewHLL(8)
	if err != nil {
		t.Fatalf("Failed to create original HLL: %v", err)
	}
	
	testElements := []string{"item1", "item2", "item3", "item1", "item4", "item5"}
	for _, element := range testElements {
		original.Add([]byte(element))
	}
	
	// Save to disk
	err = original.SaveToDisk("test_hll_load")
	if err != nil {
		t.Fatalf("Failed to save original HLL: %v", err)
	}
	
	// Load from disk
	loaded, err := LoadHyperLogLogFromDisk("test_hll_load")
	if err != nil {
		t.Fatalf("Failed to load HLL from disk: %v", err)
	}
	
	// Verify loaded HLL has same structure
	if loaded.m != original.m {
		t.Errorf("Loaded m=%d, expected %d", loaded.m, original.m)
	}
	if loaded.p != original.p {
		t.Errorf("Loaded p=%d, expected %d", loaded.p, original.p)
	}
	if len(loaded.reg) != len(original.reg) {
		t.Errorf("Loaded reg length=%d, expected %d", len(loaded.reg), len(original.reg))
	}
	
	// Verify registers are preserved
	for i := 0; i < len(original.reg); i++ {
		if loaded.reg[i] != original.reg[i] {
			t.Errorf("Register mismatch at index %d: original=%d, loaded=%d", 
				i, original.reg[i], loaded.reg[i])
		}
	}
	
	// Verify estimates are preserved
	originalEstimate := original.Estimate()
	loadedEstimate := loaded.Estimate()
	if originalEstimate != loadedEstimate {
		t.Errorf("Estimate mismatch: original=%.2f, loaded=%.2f", 
			originalEstimate, loadedEstimate)
	}
}

// TestSaveLoadRoundTrip tests complete save/load cycle for HyperLogLog
func TestSaveLoadRoundTrip(t *testing.T) {
	original, err := NewHLL(12)
	if err != nil {
		t.Fatalf("Failed to create original HLL: %v", err)
	}
	
	// Add a large diverse dataset
	for i := 0; i < 1000; i++ {
		element := fmt.Sprintf("element_%d", i)
		original.Add([]byte(element))
	}
	
	originalEstimate := original.Estimate()
	
	// Save to disk
	err = original.SaveToDisk("test_hll_roundtrip")
	if err != nil {
		t.Fatalf("Failed to save HLL: %v", err)
	}
	
	// Load from disk
	loaded, err := LoadHyperLogLogFromDisk("test_hll_roundtrip")
	if err != nil {
		t.Fatalf("Failed to load HLL: %v", err)
	}
	
	loadedEstimate := loaded.Estimate()
	
	// Verify estimates match exactly (they should be identical after load)
	if originalEstimate != loadedEstimate {
		t.Errorf("Estimate mismatch after roundtrip: original=%.2f, loaded=%.2f", 
			originalEstimate, loadedEstimate)
	}
	
	// Verify we can still add elements and get reasonable estimates
	loaded.Add([]byte("new_element"))
	newEstimate := loaded.Estimate()
	
	// Should be close to original + 1, allowing for HLL approximation
	expectedMin := originalEstimate
	expectedMax := originalEstimate + 10 // Allow some variance due to HLL approximation
	
	if newEstimate < expectedMin || newEstimate > expectedMax {
		t.Errorf("New estimate %.2f not in expected range [%.2f, %.2f]", 
			newEstimate, expectedMin, expectedMax)
	}
}
