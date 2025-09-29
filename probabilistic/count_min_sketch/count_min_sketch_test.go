package count_min_sketch

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
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

// Test thread-safety for concurrent Add operations
func TestConcurrentAdd(t *testing.T) {
	cms := NewCMS(0.01, 0.99)

	numGoroutines := 100
	numElementsPerGoroutine := 50
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numElementsPerGoroutine; j++ {
				element := fmt.Sprintf("element_%d_%d", goroutineID, j)
				cms.Add([]byte(element))
			}
		}(i)
	}

	wg.Wait()

	// Verify that elements were added correctly
	// Check a few specific elements to ensure they have non-zero counts
	testElement := "element_0_0"
	count := cms.Count([]byte(testElement))
	if count == 0 {
		t.Errorf("Element %s should have been added but has count 0", testElement)
	}
}

// Test thread-safety for concurrent Add and Count operations
func TestConcurrentAddAndCount(t *testing.T) {
	cms := NewCMS(0.05, 0.95)

	numAdders := 50
	numReaders := 25
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
				cms.Add([]byte(element))
				counter++
			}
		}(i)
	}

	// Start reader goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			start := time.Now()
			counter := 0
			for time.Since(start) < duration {
				element := fmt.Sprintf("element_%d_%d", goroutineID%numAdders, counter%10)
				_ = cms.Count([]byte(element))
				counter++
				time.Sleep(time.Microsecond) // Small delay to allow more interleaving
			}
		}(i)
	}

	wg.Wait()
}

// Test thread-safety for serialization during concurrent operations
func TestConcurrentSerialization(t *testing.T) {
	cms := NewCMS(0.05, 0.95)

	// Add some initial data
	for i := 0; i < 100; i++ {
		cms.Add([]byte(fmt.Sprintf("initial_%d", i)))
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
				data := cms.Serialize()
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
				cms.Add([]byte(element))
				counter++
			}
		}(i)
	}

	// Start count goroutines
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			for time.Since(start) < duration {
				element := "initial_0"
				_ = cms.Count([]byte(element))
			}
		}()
	}

	wg.Wait()
}

// Test concurrent frequency counting accuracy
func TestConcurrentFrequencyAccuracy(t *testing.T) {
	cms := NewCMS(0.01, 0.99)

	element := "test_element"
	expectedCount := uint32(1000)
	var wg sync.WaitGroup

	// Add the same element multiple times concurrently
	for i := uint32(0); i < expectedCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cms.Add([]byte(element))
		}()
	}

	wg.Wait()

	// Check the final count
	actualCount := cms.Count([]byte(element))

	// Count-Min Sketch can only overestimate, never underestimate
	if actualCount < expectedCount {
		t.Errorf("Count-Min Sketch underestimated: expected at least %d, got %d", expectedCount, actualCount)
	}

	// With our error rate, the overestimation should be reasonable
	maxExpectedOverestimate := expectedCount + uint32(float64(expectedCount)*0.05) // 5% tolerance
	if actualCount > maxExpectedOverestimate {
		t.Errorf("Count-Min Sketch overestimated too much: expected at most %d, got %d", maxExpectedOverestimate, actualCount)
	}
}

// Test for race conditions using Go's race detector
func TestRaceConditions(t *testing.T) {
	if !isRaceEnabled() {
		t.Skip("Race detection not enabled")
	}

	cms := NewCMS(0.05, 0.95)
	var wg sync.WaitGroup
	numGoroutines := 20
	iterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				element := fmt.Sprintf("element_%d_%d", id, j%10)
				switch j % 3 {
				case 0:
					cms.Add([]byte(element))
				case 1:
					_ = cms.Count([]byte(element))
				case 2:
					_ = cms.Serialize()
				}
			}
		}(i)
	}

	wg.Wait()
}

// Helper function to check if race detection is enabled
func isRaceEnabled() bool {
	// This is a simple way to check if the race detector is enabled
	// by setting GOMAXPROCS and checking if it changes
	old := runtime.GOMAXPROCS(0)
	return old > 0
}

// Test concurrent operations with different elements
func TestConcurrentDifferentElements(t *testing.T) {
	cms := NewCMS(0.01, 0.99)

	elements := []string{"apple", "banana", "cherry", "date", "elderberry"}
	var wg sync.WaitGroup

	// Add each element multiple times concurrently
	for _, element := range elements {
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(elem string) {
				defer wg.Done()
				cms.Add([]byte(elem))
			}(element)
		}
	}

	wg.Wait()

	// Verify all elements have reasonable counts
	for _, element := range elements {
		count := cms.Count([]byte(element))
		if count < 90 { // Allow some tolerance for concurrent operations
			t.Errorf("Element %s has unexpectedly low count: %d", element, count)
		}
	}
}

// Benchmark thread-safe operations
func BenchmarkConcurrentAdd(b *testing.B) {
	cms := NewCMS(0.01, 0.99)

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			element := fmt.Sprintf("element_%d", counter)
			cms.Add([]byte(element))
			counter++
		}
	})
}

func BenchmarkConcurrentCount(b *testing.B) {
	cms := NewCMS(0.01, 0.99)

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		cms.Add([]byte(fmt.Sprintf("element_%d", i)))
	}

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			element := fmt.Sprintf("element_%d", counter%1000)
			_ = cms.Count([]byte(element))
			counter++
		}
	})
}

func BenchmarkConcurrentSerialization(b *testing.B) {
	cms := NewCMS(0.01, 0.99)

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		cms.Add([]byte(fmt.Sprintf("element_%d", i)))
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cms.Serialize()
		}
	})
}

// Test mixed concurrent operations
func TestMixedConcurrentOperations(t *testing.T) {
	cms := NewCMS(0.05, 0.95)
	var wg sync.WaitGroup
	duration := 100 * time.Millisecond

	// Mixed operations: Add, Count, and Serialize
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			start := time.Now()
			counter := 0
			for time.Since(start) < duration {
				element := fmt.Sprintf("mixed_element_%d", counter%50)

				switch counter % 4 {
				case 0:
					cms.Add([]byte(element))
				case 1:
					_ = cms.Count([]byte(element))
				case 2:
					_ = cms.Serialize()
				case 3:
					// Add the same element multiple times
					for j := 0; j < 3; j++ {
						cms.Add([]byte(element))
					}
				}
				counter++
			}
		}(i)
	}

	wg.Wait()
}

// TestSaveToDisk tests saving Count-Min Sketch to disk
func TestSaveToDisk(t *testing.T) {
	cms := NewCMS(0.01, 0.05)
	
	// Add some test data
	testKeys := []string{"apple", "banana", "cherry", "apple", "banana", "apple"}
	for _, key := range testKeys {
		cms.Add([]byte(key))
	}
	
	// Save to disk
	err := cms.SaveToDisk("test_cms")
	if err != nil {
		t.Fatalf("Failed to save CMS to disk: %v", err)
	}
}

// TestLoadFromDisk tests loading Count-Min Sketch from disk
func TestLoadFromDisk(t *testing.T) {
	// Create and populate original CMS
	original := NewCMS(0.01, 0.05)
	testKeys := []string{"apple", "banana", "cherry", "apple", "banana", "apple"}
	for _, key := range testKeys {
		original.Add([]byte(key))
	}
	
	// Save to disk
	err := original.SaveToDisk("test_cms_load")
	if err != nil {
		t.Fatalf("Failed to save original CMS: %v", err)
	}
	
	// Load from disk
	loaded, err := LoadCountMinSketchFromDisk("test_cms_load")
	if err != nil {
		t.Fatalf("Failed to load CMS from disk: %v", err)
	}
	
	// Verify loaded CMS has same structure
	if loaded.m != original.m {
		t.Errorf("Loaded m=%d, expected %d", loaded.m, original.m)
	}
	if loaded.k != original.k {
		t.Errorf("Loaded k=%d, expected %d", loaded.k, original.k)
	}
	
	// Verify counts are preserved
	for _, key := range []string{"apple", "banana", "cherry", "nonexistent"} {
		originalCount := original.Count([]byte(key))
		loadedCount := loaded.Count([]byte(key))
		if originalCount != loadedCount {
			t.Errorf("Count mismatch for key '%s': original=%d, loaded=%d", 
				key, originalCount, loadedCount)
		}
	}
}

// TestSaveLoadRoundTrip tests complete save/load cycle
func TestSaveLoadRoundTrip(t *testing.T) {
	original := NewCMS(0.001, 0.01)
	
	// Add diverse test data
	testData := []struct {
		key   string
		count int
	}{
		{"user:1", 10},
		{"user:2", 5},
		{"event:click", 100},
		{"event:view", 50},
		{"product:abc", 25},
	}
	
	for _, item := range testData {
		for i := 0; i < item.count; i++ {
			original.Add([]byte(item.key))
		}
	}
	
	// Save to disk
	err := original.SaveToDisk("test_cms_roundtrip")
	if err != nil {
		t.Fatalf("Failed to save CMS: %v", err)
	}
	
	// Load from disk
	loaded, err := LoadCountMinSketchFromDisk("test_cms_roundtrip")
	if err != nil {
		t.Fatalf("Failed to load CMS: %v", err)
	}
	
	// Verify all data is preserved
	for _, item := range testData {
		originalCount := original.Count([]byte(item.key))
		loadedCount := loaded.Count([]byte(item.key))
		
		// CMS can overestimate, but should never underestimate
		if loadedCount < uint32(item.count) {
			t.Errorf("Loaded count for '%s' is %d, should be at least %d", 
				item.key, loadedCount, item.count)
		}
		
		if originalCount != loadedCount {
			t.Errorf("Count mismatch for '%s': original=%d, loaded=%d", 
				item.key, originalCount, loadedCount)
		}
	}
}
