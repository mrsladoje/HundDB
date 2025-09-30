package cache

import (
	model "hunddb/model/record"
	"testing"
	"time"
)

// Helper function to create a new Record
func newRecord(key string, value string) *model.Record {
	return model.NewRecord(key, []byte(value), uint64(time.Now().UnixNano()), false)
}

func TestReadPathCache(t *testing.T) {
	cache := NewReadPathCache()

	t.Run("Put and Get", func(t *testing.T) {
		record := newRecord("key1", "value1")
		err := cache.Put("key1", record)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}

		retrievedRecord, err := cache.Get("key1")
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if retrievedRecord == nil {
			t.Fatalf("Get returned nil record")
		}
		if string(retrievedRecord.Value) != "value1" {
			t.Errorf("Expected value 'value1', got '%s'", string(retrievedRecord.Value))
		}
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		_, err := cache.Get("nonexistent")
		if err == nil {
			t.Errorf("Expected an error for non-existent key, but got nil")
		}
	})

	t.Run("Contains", func(t *testing.T) {
		cache.Put("key2", newRecord("key2", "value2"))
		if !cache.Contains("key2") {
			t.Errorf("Expected cache to contain 'key2', but it didn't")
		}
		if cache.Contains("nonexistent") {
			t.Errorf("Expected cache not to contain 'nonexistent', but it did")
		}
	})

	t.Run("Remove", func(t *testing.T) {
		cache.Put("key3", newRecord("key3", "value3"))
		err := cache.Remove("key3")
		if err != nil {
			t.Errorf("Remove failed: %v", err)
		}
		if cache.Contains("key3") {
			t.Errorf("Expected cache not to contain 'key3' after removal, but it did")
		}
	})

	t.Run("Invalidate", func(t *testing.T) {
		cache.Put("key4", newRecord("key4", "value4"))
		cache.Invalidate("key4")
		if cache.Contains("key4") {
			t.Errorf("Expected key 'key4' to be invalidated, but it wasn't")
		}
	})

	t.Run("Size and Capacity", func(t *testing.T) {
		newCache := NewReadPathCache()
		if newCache.Size() != 0 {
			t.Errorf("Expected initial size to be 0, got %d", newCache.Size())
		}
		if newCache.Capacity() != uint32(READ_PATH_CACHE_CAPACITY) {
			t.Errorf("Expected capacity to be %d, got %d", READ_PATH_CACHE_CAPACITY, newCache.Capacity())
		}
	})

	t.Run("LRU eviction", func(t *testing.T) {
		// Create a cache with a small capacity for easier testing
		capacity := cache.Capacity()
		cache.SetCapacity(2)
		defer cache.SetCapacity(capacity) // Restore original capacity after test

		cache.Put("a", newRecord("a", "1"))
		cache.Put("b", newRecord("b", "2"))
		// Access 'a' to make it recently used
		cache.Get("a")
		// Add a new item, 'b' should be evicted
		cache.Put("c", newRecord("c", "3"))

		if cache.Contains("b") {
			t.Errorf("Expected key 'b' to be evicted, but it was still in the cache")
		}
		if !cache.Contains("a") {
			t.Errorf("Expected key 'a' to remain, but it was evicted")
		}
	})
}
