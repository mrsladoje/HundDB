package independent_bloom_filter

import (
	"fmt"
	"os"
	"testing"
)

// TestNewIndependentBloomFilter tests creating a new independent bloom filter
func TestNewIndependentBloomFilter(t *testing.T) {
	ibf := NewIndependentBloomFilter(1000, 0.01)
	
	if ibf.BloomFilter == nil {
		t.Fatal("BloomFilter should not be nil")
	}
	
	// Test that the bloom filter works correctly
	ibf.Add([]byte("test"))
	if !ibf.Contains([]byte("test")) {
		t.Fatal("BloomFilter should contain added element")
	}
}

// TestBasicOperations tests that all basic bloom filter operations work
func TestBasicOperations(t *testing.T) {
	ibf := NewIndependentBloomFilter(100, 0.01)
	
	// Test Add and Contains
	testItems := []string{"apple", "banana", "cherry", "date"}
	
	// Add items
	for _, item := range testItems {
		ibf.Add([]byte(item))
	}
	
	// Test Contains for added items
	for _, item := range testItems {
		if !ibf.Contains([]byte(item)) {
			t.Errorf("IBF should contain '%s'", item)
		}
	}
	
	// Test Contains for non-existent items
	nonExistent := []string{"grape", "melon", "kiwi"}
	falsePositives := 0
	
	for _, item := range nonExistent {
		if ibf.Contains([]byte(item)) {
			falsePositives++
		}
	}
	
	// With a very small filter and few items, we shouldn't get many false positives
	if falsePositives == len(nonExistent) {
		t.Error("Too many false positives - something might be wrong")
	}
}

// TestSaveToDisk tests saving independent bloom filter to disk
func TestSaveToDisk(t *testing.T) {
	// Cleanup test files after test completes
	defer func() {
		os.Remove("independent_bloom_filter_test_ibf_save")
	}()
	
	ibf := NewIndependentBloomFilter(100, 0.01)
	
	// Add some test data
	testItems := []string{"save_test_1", "save_test_2", "save_test_3"}
	for _, item := range testItems {
		ibf.Add([]byte(item))
	}
	
	// Save to disk
	err := ibf.SaveToDisk("test_ibf_save")
	if err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}
}

// TestLoadFromDisk tests loading independent bloom filter from disk
func TestLoadFromDisk(t *testing.T) {
	// Cleanup test files after test completes
	defer func() {
		os.Remove("independent_bloom_filter_test_ibf_load")
	}()
	
	// First create and save a filter
	original := NewIndependentBloomFilter(100, 0.01)
	
	testItems := []string{"load_test_1", "load_test_2", "load_test_3"}
	for _, item := range testItems {
		original.Add([]byte(item))
	}
	
	err := original.SaveToDisk("test_ibf_load")
	if err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}
	
	// Now load it back
	loaded, err := LoadIndependentBloomFilterFromDisk("test_ibf_load")
	if err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}
	
	// Test that loaded filter contains original items
	for _, item := range testItems {
		if !loaded.Contains([]byte(item)) {
			t.Errorf("Loaded IBF should contain '%s'", item)
		}
	}
}

// TestLoadFromDiskMethod tests the instance method for loading from disk
func TestLoadFromDiskMethod(t *testing.T) {
	// Cleanup test files after test completes
	defer func() {
		os.Remove("independent_bloom_filter_test_ibf_method")
	}()
	
	// Create and save original filter
	original := NewIndependentBloomFilter(200, 0.01)
	
	testItems := []string{"method_test_1", "method_test_2", "method_test_3"}
	for _, item := range testItems {
		original.Add([]byte(item))
	}
	
	err := original.SaveToDisk("test_ibf_method")
	if err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}
	
	// Create new filter and load data into it
	newFilter := NewIndependentBloomFilter(200, 0.01)
	err = newFilter.LoadFromDisk("test_ibf_method")
	if err != nil {
		t.Fatalf("LoadFromDisk method failed: %v", err)
	}
	
	// Verify content
	for _, item := range testItems {
		if !newFilter.Contains([]byte(item)) {
			t.Errorf("Filter loaded via method should contain '%s'", item)
		}
	}
}

// TestSaveLoadRoundTrip tests complete save/load cycle
func TestSaveLoadRoundTrip(t *testing.T) {
	// Cleanup test files after test completes
	defer func() {
		os.Remove("independent_bloom_filter_test_ibf_roundtrip")
	}()
	
	// Create original filter with specific parameters
	original := NewIndependentBloomFilter(1000, 0.01)
	
	// Add a variety of items
	testItems := []string{
		"roundtrip_test_1", "roundtrip_test_2", "roundtrip_test_3",
		"unicode_тест", "emoji_🌟", "numbers_12345",
		"special_!@#$%", "long_item_with_many_characters_to_test_longer_keys",
	}
	
	for _, item := range testItems {
		original.Add([]byte(item))
	}
	
	// Save to disk
	err := original.SaveToDisk("test_ibf_roundtrip")
	if err != nil {
		t.Fatalf("SaveToDisk failed: %v", err)
	}
	
	// Load from disk
	loaded, err := LoadIndependentBloomFilterFromDisk("test_ibf_roundtrip")
	if err != nil {
		t.Fatalf("LoadFromDisk failed: %v", err)
	}
	
	// Verify all original items are present
	for _, item := range testItems {
		if !loaded.Contains([]byte(item)) {
			t.Errorf("Loaded IBF should contain '%s'", item)
		}
	}
	
	// Test adding new items to loaded filter
	newItem := "new_item_after_load"
	loaded.Add([]byte(newItem))
	
	if !loaded.Contains([]byte(newItem)) {
		t.Error("Should be able to add items to loaded IBF")
	}
}

// TestThreadSafety tests concurrent operations
func TestThreadSafety(t *testing.T) {
	ibf := NewIndependentBloomFilter(1000, 0.01)
	
	// This is a basic concurrency test
	done := make(chan bool, 2)
	
	// Goroutine 1: Add items
	go func() {
		for i := 0; i < 100; i++ {
			item := fmt.Sprintf("concurrent_item_%d", i)
			ibf.Add([]byte(item))
		}
		done <- true
	}()
	
	// Goroutine 2: Check items
	go func() {
		for i := 0; i < 50; i++ {
			item := fmt.Sprintf("concurrent_item_%d", i)
			_ = ibf.Contains([]byte(item)) // Result doesn't matter for this test
		}
		done <- true
	}()
	
	// Wait for both goroutines
	<-done
	<-done
	
	// If we get here without deadlock, the mutex is working
}