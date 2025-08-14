package lru_cache

import (
	mdl "hunddb/model"
	"testing"
)

// TestNewLRUCache_RecordCache tests cache creation for record based caching
func TestNewLRUCache_RecordCache(t *testing.T) {
	tests := []struct {
		capacity     uint32
		expectedSize uint32
		expectedCap  uint32
	}{
		{10, 0, 10},
		{1, 0, 1},
		{100, 0, 100},
	}

	for _, test := range tests {
		cache := NewLRUCache[string, *mdl.Record](test.capacity)

		if cache == nil {
			t.Fatal("Expected non nil LRUCache")
		}

		if cache.Size() != test.expectedSize {
			t.Errorf("Expected size %d, got %d", test.expectedSize, cache.Size())
		}

		if cache.Capacity() != test.expectedCap {
			t.Errorf("Expected capacity %d, got %d", test.expectedCap, cache.Capacity())
		}
	}
}

// TestNewLRUCache_BlockCache tests cache creation for block based caching
func TestNewLRUCache_BlockCache(t *testing.T) {
	cache := NewLRUCache[mdl.BlockLocation, []byte](50)

	if cache == nil {
		t.Fatal("Expected non nil LRUCache")
	}

	if cache.Size() != 0 {
		t.Errorf("Expected initial size 0, got %d", cache.Size())
	}

	if cache.Capacity() != 50 {
		t.Errorf("Expected capacity 50, got %d", cache.Capacity())
	}
}

// TestLRUCache_RecordCaching tests typical record caching scenarios
func TestLRUCache_RecordCaching(t *testing.T) {
	cache := NewLRUCache[string, *mdl.Record](3)

	tests := []struct {
		operation   string
		key         string
		record      *mdl.Record
		expectedKey string
		expectedVal []byte
		shouldErr   bool
	}{
		{"put", "user:123", mdl.NewRecord("user:123", []byte("John Doe"), 1000, false), "", nil, false},
		{"put", "user:456", mdl.NewRecord("user:456", []byte("Jane Smith"), 1001, false), "", nil, false},
		{"put", "user:789", mdl.NewRecord("user:789", []byte("Bob Johnson"), 1002, false), "", nil, false},
		{"get", "user:123", nil, "user:123", []byte("John Doe"), false},
		{"get", "user:456", nil, "user:456", []byte("Jane Smith"), false},
		{"get", "user:999", nil, "", nil, true},
	}

	for _, test := range tests {
		switch test.operation {
		case "put":
			err := cache.Put(test.key, test.record)
			if err != nil && !test.shouldErr {
				t.Errorf("Put operation failed for key %s: %v", test.key, err)
			}
		case "get":
			record, err := cache.Get(test.key)
			if test.shouldErr {
				if err != ErrKeyNotFound {
					t.Errorf("Expected ErrKeyNotFound for key %s, got %v", test.key, err)
				}
			} else {
				if err != nil {
					t.Errorf("Get operation failed for key %s: %v", test.key, err)
				}
				if record.Key != test.expectedKey {
					t.Errorf("Expected key '%s' for key %s, got '%s'", test.expectedKey, test.key, record.Key)
				}
				if string(record.Value) != string(test.expectedVal) {
					t.Errorf("Expected value '%s' for key %s, got '%s'", string(test.expectedVal), test.key, string(record.Value))
				}
			}
		}
	}
}

// TestLRUCache_BlockCaching tests typical block caching scenarios
func TestLRUCache_BlockCaching(t *testing.T) {
	cache := NewLRUCache[mdl.BlockLocation, []byte](2)

	loc1 := mdl.BlockLocation{FilePath: "/data/file1.db", BlockIndex: 0}
	loc2 := mdl.BlockLocation{FilePath: "/data/file1.db", BlockIndex: 1}
	loc3 := mdl.BlockLocation{FilePath: "/data/file2.db", BlockIndex: 0}

	block1Data := []byte("block1 data")
	block2Data := []byte("block2 data")
	block3Data := []byte("block3 data")

	err := cache.Put(loc1, block1Data)
	if err != nil {
		t.Fatalf("Failed to put block1: %v", err)
	}

	err = cache.Put(loc2, block2Data)
	if err != nil {
		t.Fatalf("Failed to put block2: %v", err)
	}

	retrievedBlock1, err := cache.Get(loc1)
	if err != nil {
		t.Fatalf("Failed to get block1: %v", err)
	}

	if string(retrievedBlock1) != "block1 data" {
		t.Errorf("Expected 'block1 data', got '%s'", string(retrievedBlock1))
	}

	// Add third block that should evict block2 since block1 was recently accessed
	err = cache.Put(loc3, block3Data)
	if err != nil {
		t.Fatalf("Failed to put block3: %v", err)
	}

	// block2 should be evicted
	_, err = cache.Get(loc2)
	if err != ErrKeyNotFound {
		t.Error("Expected block2 to be evicted")
	}

	// block1 and block3 should still be present
	_, err = cache.Get(loc1)
	if err != nil {
		t.Error("Expected block1 to still be in cache")
	}

	_, err = cache.Get(loc3)
	if err != nil {
		t.Error("Expected block3 to still be in cache")
	}
}

// TestLRUCache_RecordEviction tests record cache eviction behavior
func TestLRUCache_RecordEviction(t *testing.T) {
	cache := NewLRUCache[string, *mdl.Record](2)

	aliceRecord := mdl.NewRecord("user:001", []byte("Alice"), 1000, false)
	bobRecord := mdl.NewRecord("user:002", []byte("Bob"), 1001, false)
	charlieRecord := mdl.NewRecord("user:003", []byte("Charlie"), 1002, false)

	cache.Put("user:001", aliceRecord)
	cache.Put("user:002", bobRecord)

	alice, err := cache.Get("user:001")
	if err != nil || string(alice.Value) != "Alice" {
		t.Errorf("Expected Alice, got %v, error: %v", string(alice.Value), err)
	}

	cache.Put("user:003", charlieRecord)

	_, err = cache.Get("user:002")
	if err != ErrKeyNotFound {
		t.Error("Expected user:002 to be evicted")
	}

	remainingKeys := []string{"user:001", "user:003"}
	expectedValues := []string{"Alice", "Charlie"}

	for i, key := range remainingKeys {
		record, err := cache.Get(key)
		if err != nil {
			t.Errorf("Expected key %s to remain in cache", key)
		}
		if string(record.Value) != expectedValues[i] {
			t.Errorf("Expected value %s for key %s, got %s", expectedValues[i], key, string(record.Value))
		}
	}
}

// TestLRUCache_BlockEviction tests block cache eviction behavior
func TestLRUCache_BlockEviction(t *testing.T) {
	cache := NewLRUCache[mdl.BlockLocation, []byte](3)

	locations := []mdl.BlockLocation{
		{FilePath: "/data/users.db", BlockIndex: 0},
		{FilePath: "/data/users.db", BlockIndex: 1},
		{FilePath: "/data/orders.db", BlockIndex: 0},
		{FilePath: "/data/products.db", BlockIndex: 0},
	}

	blocks := [][]byte{
		[]byte("user block 0"),
		[]byte("user block 1"),
		[]byte("orders block 0"),
		[]byte("products block 0"),
	}

	for i := range 3 {
		err := cache.Put(locations[i], blocks[i])
		if err != nil {
			t.Fatalf("Failed to put block %d: %v", i, err)
		}
	}

	_, err := cache.Get(locations[0])
	if err != nil {
		t.Fatalf("Failed to get block 0: %v", err)
	}

	err = cache.Put(locations[3], blocks[3])
	if err != nil {
		t.Fatalf("Failed to put fourth block: %v", err)
	}

	_, err = cache.Get(locations[1])
	if err != ErrKeyNotFound {
		t.Error("Expected block 1 to be evicted")
	}

	remainingIndices := []int{0, 2, 3}
	for _, idx := range remainingIndices {
		retrievedBlock, err := cache.Get(locations[idx])
		if err != nil {
			t.Errorf("Expected block %d to remain in cache", idx)
		} else {
			expectedData := blocks[idx]
			if string(retrievedBlock) != string(expectedData) {
				t.Errorf("Block %d data mismatch", idx)
			}
		}
	}
}

// TestLRUCache_RecordRemoval tests removing records from cache
func TestLRUCache_RecordRemoval(t *testing.T) {
	cache := NewLRUCache[string, *mdl.Record](10)

	testRecords := map[string]*mdl.Record{
		"user:001":    mdl.NewRecord("user:001", []byte("John Smith"), 1000, false),
		"user:002":    mdl.NewRecord("user:002", []byte("Jane Doe"), 1001, false),
		"order:12345": mdl.NewRecord("order:12345", []byte("Order details"), 1002, false),
	}

	for key, record := range testRecords {
		err := cache.Put(key, record)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	err := cache.Remove("user:001")
	if err != nil {
		t.Errorf("Failed to remove user:001: %v", err)
	}

	_, err = cache.Get("user:001")
	if err != ErrKeyNotFound {
		t.Error("Expected user:001 to be removed")
	}

	if cache.Size() != 2 {
		t.Errorf("Expected size 2 after removal, got %d", cache.Size())
	}

	err = cache.Remove("user:999")
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound when removing non-existent record, got %v", err)
	}

	if cache.Size() != 2 {
		t.Errorf("Expected size to remain 2, got %d", cache.Size())
	}
}

// TestLRUCache_BlockRemoval tests removing blocks from cache
func TestLRUCache_BlockRemoval(t *testing.T) {
	cache := NewLRUCache[mdl.BlockLocation, []byte](5)

	loc1 := mdl.BlockLocation{FilePath: "/data/test.db", BlockIndex: 0}
	loc2 := mdl.BlockLocation{FilePath: "/data/test.db", BlockIndex: 1}

	block1Data := []byte("test block 0")
	block2Data := []byte("test block 1")

	cache.Put(loc1, block1Data)
	cache.Put(loc2, block2Data)

	err := cache.Remove(loc1)
	if err != nil {
		t.Errorf("Failed to remove block: %v", err)
	}

	_, err = cache.Get(loc1)
	if err != ErrKeyNotFound {
		t.Error("Expected block to be removed")
	}

	retrievedBlock, err := cache.Get(loc2)
	if err != nil {
		t.Error("Expected other block to remain")
	}
	if string(retrievedBlock) != "test block 1" {
		t.Error("Remaining block data corrupted")
	}
}
