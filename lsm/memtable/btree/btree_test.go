package btree

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	memtable "hunddb/lsm/memtable/memtable_interface"
	record "hunddb/model/record"
)

// --- Helper Functions ---

// createTestRecord creates a standard, active record.
func createTestRecord(key, value string) *record.Record {
	return &record.Record{
		Key:       key,
		Value:     []byte(value),
		Tombstone: false,
		Timestamp: uint64(time.Now().UnixNano()),
	}
}

// createTombstoneRecord creates a record marked as deleted.
func createTombstoneRecord(key string) *record.Record {
	return &record.Record{
		Key:       key,
		Value:     nil,
		Tombstone: true,
		Timestamp: uint64(time.Now().UnixNano()),
	}
}

// --- Test Cases ---

func TestNewBTree(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		order         int
		capacity      int
		expectedOrder int
		expectedCap   int
	}{
		{"Default values", 0, 0, DefaultOrder, math.MaxInt},
		{"Negative values", -1, -5, DefaultOrder, math.MaxInt},
		{"Custom values", 10, 100, 10, 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			btree := NewBTree(tc.order, tc.capacity)
			if btree.order != tc.expectedOrder {
				t.Errorf("expected order %d, got %d", tc.expectedOrder, btree.order)
			}
			if btree.capacity != tc.expectedCap {
				t.Errorf("expected capacity %d, got %d", tc.expectedCap, btree.capacity)
			}
			if btree.root != nil {
				t.Error("root should be nil on initialization")
			}
			if btree.totalRecords != 0 || btree.activeRecords != 0 {
				t.Error("record counts should be zero on initialization")
			}
		})
	}
}

func TestBTree_PutAndGet_Single(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 10)
	rec := createTestRecord("key1", "value1")

	// Test Put
	if err := btree.Put(rec); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	retrieved := btree.Get("key1")
	if retrieved == nil {
		t.Fatal("record not found after Put")
	}
	if retrieved.Key != "key1" || string(retrieved.Value) != "value1" {
		t.Errorf("retrieved record doesn't match: got key %s, value %s", retrieved.Key, string(retrieved.Value))
	}

	// Test Get non-existent key
	if btree.Get("nonexistent") != nil {
		t.Error("should return nil for a non-existent key")
	}
}

func TestBTree_Put_InvalidRecord(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 10)
	tests := []struct {
		name   string
		record *record.Record
	}{
		{"Nil record", nil},
		{"Empty key", &record.Record{Key: "", Value: []byte("value")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := btree.Put(tc.record); err == nil {
				t.Error("expected an error for invalid record, but got nil")
			}
		})
	}
}

func TestBTree_MultipleInsertions_TriggerSplit(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 10) // Order 3 splits after 3 keys in a node

	keys := []string{"d", "b", "f", "a", "c", "e", "g"}
	for _, key := range keys {
		if err := btree.Put(createTestRecord(key, "val_"+key)); err != nil {
			t.Fatalf("failed to insert %s: %v", key, err)
		}
	}

	// Verify all records can be retrieved
	for _, key := range keys {
		retrieved := btree.Get(key)
		if retrieved == nil {
			t.Errorf("record %s not found", key)
			continue
		}
		if string(retrieved.Value) != "val_"+key {
			t.Errorf("value mismatch for %s: expected %s, got %s", key, "val_"+key, string(retrieved.Value))
		}
	}

	if btree.totalRecords != len(keys) {
		t.Errorf("expected %d total records, got %d", len(keys), btree.totalRecords)
	}
	if btree.activeRecords != len(keys) {
		t.Errorf("expected %d active records, got %d", len(keys), btree.activeRecords)
	}
	if btree.Height() <= 1 {
		t.Errorf("expected height to be > 1 after splits, got %d", btree.Height())
	}
}

func TestBTree_Update_And_StateTransition(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 10)

	// 1. Initial Insert (Active)
	_ = btree.Put(createTestRecord("key1", "value1"))
	if btree.totalRecords != 1 || btree.activeRecords != 1 {
		t.Fatalf("state mismatch after initial insert: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// 2. Update Value (Active -> Active)
	_ = btree.Put(createTestRecord("key1", "newValue"))
	retrieved := btree.Get("key1")
	if string(retrieved.Value) != "newValue" {
		t.Errorf("expected value to be updated to 'newValue', got '%s'", string(retrieved.Value))
	}
	if btree.totalRecords != 1 || btree.activeRecords != 1 {
		t.Errorf("state should not change on active->active update: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// 3. Update to Tombstone (Active -> Tombstone)
	_ = btree.Put(createTombstoneRecord("key1"))
	if btree.Get("key1") != nil {
		t.Error("record should not be retrievable after being marked as tombstone")
	}
	if btree.totalRecords != 1 || btree.activeRecords != 0 {
		t.Errorf("state mismatch on active->tombstone: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// 4. Update Tombstone (Tombstone -> Tombstone)
	_ = btree.Put(createTombstoneRecord("key1"))
	if btree.totalRecords != 1 || btree.activeRecords != 0 {
		t.Errorf("state should not change on tombstone->tombstone update: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// 5. Revive Record (Tombstone -> Active)
	_ = btree.Put(createTestRecord("key1", "revivedValue"))
	retrieved = btree.Get("key1")
	if string(retrieved.Value) != "revivedValue" {
		t.Errorf("expected value to be 'revivedValue', got '%s'", string(retrieved.Value))
	}
	if btree.totalRecords != 1 || btree.activeRecords != 1 {
		t.Errorf("state mismatch on tombstone->active: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}
}

func TestBTree_Delete(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 10)
	_ = btree.Put(createTestRecord("key1", "value1"))
	_ = btree.Put(createTestRecord("key2", "value2"))

	// 1. Delete an existing key
	if deleted := btree.Delete(createTombstoneRecord("key1")); !deleted {
		t.Error("Delete should return true for an existing key")
	}
	if btree.Get("key1") != nil {
		t.Error("deleted record should not be retrievable via Get")
	}
	if btree.totalRecords != 2 || btree.activeRecords != 1 {
		t.Errorf("state mismatch after deleting existing key: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// 2. Delete a non-existent key (creates a blind tombstone)
	if deleted := btree.Delete(createTombstoneRecord("key3")); deleted {
		t.Error("Delete should return false for a non-existent key")
	}
	if btree.Get("key3") != nil {
		t.Error("blind tombstone should not be retrievable via Get")
	}
	// A new distinct key (tombstone) was added
	if btree.totalRecords != 3 || btree.activeRecords != 1 {
		t.Errorf("state mismatch after blind tombstone: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// 3. Delete an already deleted key
	if deleted := btree.Delete(createTombstoneRecord("key1")); !deleted {
		t.Error("Delete should return true for an already tombstoned key")
	}
	if btree.totalRecords != 3 || btree.activeRecords != 1 {
		t.Errorf("state should not change when deleting an already deleted key: total=%d, active=%d", btree.totalRecords, btree.activeRecords)
	}

	// key2 should still be there
	if btree.Get("key2") == nil {
		t.Error("key2 should still be retrievable")
	}
}

func TestBTree_Capacity_IsFull(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 3)

	if btree.IsFull() {
		t.Fatal("empty tree should not be full")
	}

	// Fill to capacity
	_ = btree.Put(createTestRecord("k1", "v1"))
	_ = btree.Put(createTestRecord("k2", "v2"))
	_ = btree.Put(createTestRecord("k3", "v3"))

	if !btree.IsFull() {
		t.Error("tree should be full after 3 inserts")
	}

	// Attempt to insert another new key
	err := btree.Put(createTestRecord("k4", "v4"))
	if err == nil {
		t.Error("expected error when inserting into a full tree, got nil")
	}

	// Updating an existing key should still work
	err = btree.Put(createTestRecord("k1", "new_v1"))
	if err != nil {
		t.Errorf("should be able to update a record in a full tree, got error: %v", err)
	}

	// Deleting a non-existent key should fail if tree is full
	err = btree.Put(createTombstoneRecord("k5"))
	if err == nil {
		t.Error("expected error when adding a blind tombstone to a full tree")
	}

	// Check final state
	if btree.TotalEntries() != 3 {
		t.Errorf("total entries should remain 3, got %d", btree.TotalEntries())
	}
}

func TestBTree_LargeDataset(t *testing.T) {
	t.Parallel()
	btree := NewBTree(50, 2000)
	numRecords := 1000

	// Insert a large number of records
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%04d", i) // Padded for consistent sorting
		value := "value" + strconv.Itoa(i)
		if err := btree.Put(createTestRecord(key, value)); err != nil {
			t.Fatalf("failed to insert record %d: %v", i, err)
		}
	}

	// Verify all records can be retrieved
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%04d", i)
		retrieved := btree.Get(key)
		if retrieved == nil {
			t.Errorf("record %s not found", key)
		}
	}

	if btree.TotalEntries() != numRecords {
		t.Errorf("expected %d total records, got %d", numRecords, btree.TotalEntries())
	}

	// Verify tree height is reasonable (logarithmic)
	height := btree.Height()
	// log50(1000) is approx 1.7. A height of 2 or 3 is reasonable.
	if height > 4 {
		t.Errorf("tree height %d seems too large for %d records with order %d", height, numRecords, btree.order)
	}
}

func TestBTree_InterfaceCompliance(t *testing.T) {
	// This test doesn't run code, but it will fail to compile if BTree
	// does not satisfy the MemtableInterface.
	var _ memtable.MemtableInterface = (*BTree)(nil)
}

func TestBTree_Height(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 100)
	if btree.Height() != 0 {
		t.Errorf("expected height 0 for empty tree, got %d", btree.Height())
	}

	_ = btree.Put(createTestRecord("k1", "v1"))
	if btree.Height() != 1 {
		t.Errorf("expected height 1 for tree with one record, got %d", btree.Height())
	}

	// Add enough to cause splits
	for i := 2; i <= 10; i++ {
		_ = btree.Put(createTestRecord("k"+strconv.Itoa(i), "v"))
	}
	height := btree.Height()
	if height < 2 {
		t.Errorf("expected height to be at least 2, got %d", height)
	}
}

func TestBTree_RetrieveSortedRecords_EmptyTree(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 100)
	records := btree.RetrieveSortedRecords()

	if len(records) != 0 {
		t.Errorf("Expected 0 records from an empty tree, but got %d", len(records))
	}
}

func TestBTree_RetrieveSortedRecords_SimpleTree(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 100)

	_ = btree.Put(createTestRecord("c", "val_c"))
	_ = btree.Put(createTestRecord("a", "val_a"))
	_ = btree.Put(createTestRecord("b", "val_b"))

	records := btree.RetrieveSortedRecords()
	expectedKeys := []string{"a", "b", "c"}
	if len(records) != len(expectedKeys) {
		t.Fatalf("Expected %d records, but got %d", len(expectedKeys), len(records))
	}

	for i, rec := range records {
		if rec.Key != expectedKeys[i] {
			t.Errorf("Mismatch at index %d: expected key '%s', got '%s'", i, expectedKeys[i], rec.Key)
		}
	}
}

func TestBTree_RetrieveSortedRecords_WithSplits(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 100) // Order 3 splits after 3 keys in a node.

	keys := []string{"d", "b", "f", "a", "c", "e", "g", "z", "y", "x"}
	expectedKeys := make([]string, len(keys))
	copy(expectedKeys, keys)
	sort.Strings(expectedKeys)

	for _, key := range keys {
		_ = btree.Put(createTestRecord(key, "val_"+key))
	}

	records := btree.RetrieveSortedRecords()
	if len(records) != len(expectedKeys) {
		t.Fatalf("Expected %d records, but got %d", len(expectedKeys), len(records))
	}

	for i, rec := range records {
		if rec.Key != expectedKeys[i] {
			t.Errorf("Mismatch at index %d: expected key '%s', got '%s'", i, expectedKeys[i], rec.Key)
		}
	}
}

func TestBTree_RetrieveSortedRecords_WithDeletions(t *testing.T) {
	t.Parallel()
	btree := NewBTree(3, 100)

	_ = btree.Put(createTestRecord("c", "val_c"))
	_ = btree.Put(createTestRecord("a", "val_a"))
	_ = btree.Put(createTestRecord("b", "val_b"))

	// Delete record 'b'
	_ = btree.Put(createTombstoneRecord("b"))

	// The traversal should still return all 3 records, including the tombstone
	records := btree.RetrieveSortedRecords()

	if len(records) != 3 {
		t.Fatalf("Expected 3 records (including tombstone), got %d", len(records))
	}

	// Check the order and status of the retrieved records
	if records[0].Key != "a" || records[1].Key != "b" || records[2].Key != "c" {
		t.Errorf("Keys are not in correct sorted order: %s, %s, %s", records[0].Key, records[1].Key, records[2].Key)
	}

	// The record for 'b' should be a tombstone
	if records[1].Key == "b" && !records[1].Tombstone {
		t.Errorf("Record 'b' should be a tombstone, but it's not.")
	}
}

// --- Benchmark Tests ---

func BenchmarkBTree_Put_Sequential(b *testing.B) {
	btree := NewBTree(64, b.N)
	records := make([]*record.Record, b.N)
	for i := 0; i < b.N; i++ {
		records[i] = createTestRecord(fmt.Sprintf("key%09d", i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = btree.Put(records[i])
	}
}

func BenchmarkBTree_Put_Random(b *testing.B) {
	btree := NewBTree(64, b.N)
	records := make([]*record.Record, b.N)
	// Using a simple pseudo-random sequence for determinism
	for i := 0; i < b.N; i++ {
		keyIndex := (i * 1361) % b.N // Pseudo-random order
		records[i] = createTestRecord(fmt.Sprintf("key%09d", keyIndex), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = btree.Put(records[i])
	}
}

func BenchmarkBTree_Get(b *testing.B) {
	numRecords := 100_000
	btree := NewBTree(64, numRecords)
	keys := make([]string, numRecords)
	for i := 0; i < numRecords; i++ {
		keys[i] = fmt.Sprintf("key%09d", i)
		_ = btree.Put(createTestRecord(keys[i], "value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = btree.Get(keys[i%numRecords])
	}
}

func BenchmarkBTree_Delete(b *testing.B) {
	numRecords := 100_000
	btree := NewBTree(64, numRecords)
	keys := make([]string, numRecords)
	for i := 0; i < numRecords; i++ {
		keys[i] = fmt.Sprintf("key%09d", i)
		_ = btree.Put(createTestRecord(keys[i], "value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cycle through keys to avoid cache effects from deleting non-existent keys
		_ = btree.Delete(createTombstoneRecord(keys[i%numRecords]))
	}
}
