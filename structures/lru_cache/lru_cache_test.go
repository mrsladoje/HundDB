package lru_cache

import (
	"testing"
)

func TestNewLRUCache(t *testing.T) {
	tests := []struct {
		capacity     uint32
		expectedSize uint32
		expectedCap  uint32
		shouldError  bool
		errorMessage string
	}{
		{10, 0, 10, false, ""},
		{1, 0, 1, false, ""},
		{100, 0, 100, false, ""},
	}

	for _, test := range tests {
		cache := NewLRUCache[string, int](test.capacity)

		if cache == nil {
			t.Fatal("Expected non-nil LRUCache")
		}

		if cache.Size() != test.expectedSize {
			t.Errorf("Expected size %d, got %d", test.expectedSize, cache.Size())
		}

		if cache.Capacity() != test.expectedCap {
			t.Errorf("Expected capacity %d, got %d", test.expectedCap, cache.Capacity())
		}
	}
}

func TestLRUCache_PutAndGet(t *testing.T) {
	tests := []struct {
		capacity   uint32
		operations []struct {
			operation string
			key       string
			value     int
			expected  int
			shouldErr bool
		}
	}{
		{
			capacity: 3,
			operations: []struct {
				operation string
				key       string
				value     int
				expected  int
				shouldErr bool
			}{
				{"put", "key1", 100, 0, false},
				{"put", "key2", 200, 0, false},
				{"get", "key1", 0, 100, false},
				{"get", "key2", 0, 200, false},
				{"get", "nonexistent", 0, 0, true},
			},
		},
	}

	for _, test := range tests {
		cache := NewLRUCache[string, int](test.capacity)

		for _, op := range test.operations {
			switch op.operation {
			case "put":
				err := cache.Put(op.key, op.value)
				if err != nil && !op.shouldErr {
					t.Errorf("Put operation failed: %v", err)
				}
			case "get":
				value, err := cache.Get(op.key)
				if op.shouldErr {
					if err != ErrKeyNotFound {
						t.Errorf("Expected ErrKeyNotFound, got %v", err)
					}
				} else {
					if err != nil {
						t.Errorf("Get operation failed: %v", err)
					}
					if value != op.expected {
						t.Errorf("Expected value %d, got %d", op.expected, value)
					}
				}
			}
		}
	}
}

func TestLRUCache_Capacity(t *testing.T) {
	tests := []struct {
		capacity      uint32
		keysToAdd     []string
		valesToAdd    []int
		expectedSize  uint32
		evictedKeys   []string
		remainingKeys []string
	}{
		{
			capacity:      2,
			keysToAdd:     []string{"key1", "key2", "key3"},
			valesToAdd:    []int{100, 200, 300},
			expectedSize:  2,
			evictedKeys:   []string{"key1"},
			remainingKeys: []string{"key2", "key3"},
		},
		{
			capacity:      1,
			keysToAdd:     []string{"key1", "key2"},
			valesToAdd:    []int{100, 200},
			expectedSize:  1,
			evictedKeys:   []string{"key1"},
			remainingKeys: []string{"key2"},
		},
		{
			capacity:      5,
			keysToAdd:     []string{"key1", "key2", "key3"},
			valesToAdd:    []int{100, 200, 300},
			expectedSize:  3,
			evictedKeys:   []string{},
			remainingKeys: []string{"key1", "key2", "key3"},
		},
	}

	for _, test := range tests {
		cache := NewLRUCache[string, int](test.capacity)

		// Add all keys
		for i, key := range test.keysToAdd {
			err := cache.Put(key, test.valesToAdd[i])
			if err != nil {
				t.Errorf("Failed to put key %s: %v", key, err)
			}
		}

		// Check size
		if cache.Size() != test.expectedSize {
			t.Errorf("Expected size %d, got %d", test.expectedSize, cache.Size())
		}

		// Check evicted keys
		for _, evictedKey := range test.evictedKeys {
			_, err := cache.Get(evictedKey)
			if err != ErrKeyNotFound {
				t.Errorf("Expected key %s to be evicted", evictedKey)
			}
		}

		// Check remaining keys
		for _, remainingKey := range test.remainingKeys {
			_, err := cache.Get(remainingKey)
			if err != nil {
				t.Errorf("Expected key %s to remain in cache, got error: %v", remainingKey, err)
			}
		}
	}
}

func TestLRUCache_LRUBehavior(t *testing.T) {
	cache := NewLRUCache[string, int](3)

	// Add three items
	cache.Put("key1", 100)
	cache.Put("key2", 200)
	cache.Put("key3", 300)

	// Access key1 to make it recently used
	_, err := cache.Get("key1")
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}

	// Add key4, should evict key2 (least recently used)
	cache.Put("key4", 400)

	// key2 should be evicted
	_, err = cache.Get("key2")
	if err != ErrKeyNotFound {
		t.Error("Expected key2 to be evicted")
	}

	// key1, key3, key4 should remain
	remainingKeys := []string{"key1", "key3", "key4"}
	for _, key := range remainingKeys {
		_, err := cache.Get(key)
		if err != nil {
			t.Errorf("Expected key %s to remain in cache", key)
		}
	}
}

func TestLRUCache_Remove(t *testing.T) {
	tests := []struct {
		keysToAdd    []string
		valesToAdd   []int
		keyToRemove  string
		shouldError  bool
		expectedSize uint32
	}{
		{
			keysToAdd:    []string{"key1", "key2", "key3"},
			valesToAdd:   []int{100, 200, 300},
			keyToRemove:  "key2",
			shouldError:  false,
			expectedSize: 2,
		},
		{
			keysToAdd:    []string{"key1"},
			valesToAdd:   []int{100},
			keyToRemove:  "nonexistent",
			shouldError:  true,
			expectedSize: 1,
		},
	}

	for _, test := range tests {
		cache := NewLRUCache[string, int](10)

		// Add keys
		for i, key := range test.keysToAdd {
			cache.Put(key, test.valesToAdd[i])
		}

		// Remove key
		err := cache.Remove(test.keyToRemove)
		if test.shouldError {
			if err != ErrKeyNotFound {
				t.Errorf("Expected ErrKeyNotFound, got %v", err)
			}
		} else {
			if err != nil {
				t.Errorf("Remove operation failed: %v", err)
			}
		}

		// Check size
		if cache.Size() != test.expectedSize {
			t.Errorf("Expected size %d, got %d", test.expectedSize, cache.Size())
		}

		// Verify removed key doesn't exist
		if !test.shouldError {
			_, err := cache.Get(test.keyToRemove)
			if err != ErrKeyNotFound {
				t.Errorf("Expected removed key %s to not exist", test.keyToRemove)
			}
		}
	}
}

func TestLRUCache_Contains(t *testing.T) {
	cache := NewLRUCache[string, int](5)

	// Add some keys
	keys := []string{"key1", "key2", "key3"}
	values := []int{100, 200, 300}

	for i, key := range keys {
		cache.Put(key, values[i])
	}

	// Test existing keys
	for _, key := range keys {
		if !cache.Contains(key) {
			t.Errorf("Expected key %s to exist in cache", key)
		}
	}

	// Test non-existing key
	if cache.Contains("nonexistent") {
		t.Error("Expected nonexistent key to not be in cache")
	}
}

func TestLRUCache_Peek(t *testing.T) {
	cache := NewLRUCache[string, int](2)

	cache.Put("key1", 100)
	cache.Put("key2", 200)

	// Peek at key1 (should not affect LRU order)
	value, err := cache.Peek("key1")
	if err != nil {
		t.Errorf("Peek operation failed: %v", err)
	}
	if value != 100 {
		t.Errorf("Expected value 100, got %d", value)
	}

	// Add key3, key1 should still be evicted (since Peek didn't move it to front)
	cache.Put("key3", 300)

	_, err = cache.Get("key1")
	if err != ErrKeyNotFound {
		t.Error("Expected key1 to be evicted after Peek")
	}

	// Test peek on non-existent key
	_, err = cache.Peek("nonexistent")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestLRUCache_DifferentTypes(t *testing.T) {
	// Test with string keys and byte slice values
	byteCache := NewLRUCache[string, []byte](2)

	byteCache.Put("data1", []byte("hello"))
	byteCache.Put("data2", []byte("world"))

	value, err := byteCache.Get("data1")
	if err != nil {
		t.Errorf("Get operation failed: %v", err)
	}
	if string(value) != "hello" {
		t.Errorf("Expected 'hello', got %s", string(value))
	}

	// Test with int keys and string values
	intCache := NewLRUCache[string, string](2)

	intCache.Put("1", "one")
	intCache.Put("2", "two")

	value2, err := intCache.Get("1")
	if err != nil {
		t.Errorf("Get operation failed: %v", err)
	}
	if value2 != "one" {
		t.Errorf("Expected 'one', got %s", value2)
	}
}

func TestLRUCache_UpdateExistingKey(t *testing.T) {
	cache := NewLRUCache[string, int](3)

	cache.Put("key1", 100)
	cache.Put("key1", 150) // Update existing key

	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	value, err := cache.Get("key1")
	if err != nil {
		t.Errorf("Get operation failed: %v", err)
	}
	if value != 150 {
		t.Errorf("Expected updated value 150, got %d", value)
	}
}

func TestLRUCache_EdgeCases(t *testing.T) {
	// Test with capacity of 1
	cache := NewLRUCache[string, int](1)

	cache.Put("key1", 100)
	cache.Put("key2", 200) // Should evict key1

	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	_, err := cache.Get("key1")
	if err != ErrKeyNotFound {
		t.Error("Expected key1 to be evicted")
	}

	value, err := cache.Get("key2")
	if err != nil {
		t.Errorf("Get operation failed: %v", err)
	}
	if value != 200 {
		t.Errorf("Expected value 200, got %d", value)
	}
}
