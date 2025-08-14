package btree

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"hunddb/model"
)

// Helper function to create a test record
func createTestRecord(key, value string) *model.Record {
	return &model.Record{
		Key:       key,
		Value:     []byte(value),
		Tombstone: false,
		Timestamp: uint64(time.Now().UnixNano()),
	}
}

// Helper function to create a tombstoned record
func createTombstonedRecord(key, value string) *model.Record {
	record := createTestRecord(key, value)
	record.Tombstone = true
	return record
}

func TestNewBTree(t *testing.T) {
	tests := []struct {
		name          string
		order         int
		expectedOrder int
	}{
		{"Default order", 0, DefaultOrder},
		{"Negative order", -1, DefaultOrder},
		{"Custom order", 10, 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			btree := NewBTree(tc.order)
			if btree.order != tc.expectedOrder {
				t.Errorf("Expected order %d, got %d", tc.expectedOrder, btree.order)
			}
			if btree.stats == nil {
				t.Error("Stats should be initialized")
			}
		})
	}
}

func TestBTree_PutAndGet_SingleRecord(t *testing.T) {
	btree := NewBTree(3)
	record := createTestRecord("key1", "value1")

	// Test put
	err := btree.Put(record)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test get
	retrieved := btree.Get("key1")
	if retrieved == nil {
		t.Fatal("Record not found")
	}

	if retrieved.Key != "key1" || string(retrieved.Value) != "value1" {
		t.Errorf("Retrieved record doesn't match. Expected key1/value1, got %s/%s",
			retrieved.Key, string(retrieved.Value))
	}

	// Test non-existent key
	nonExistent := btree.Get("nonexistent")
	if nonExistent != nil {
		t.Error("Should return nil for non-existent key")
	}
}

func TestBTree_PutInvalidRecord(t *testing.T) {
	btree := NewBTree(3)

	tests := []struct {
		name   string
		record *model.Record
	}{
		{"Nil record", nil},
		{"Empty key", &model.Record{Key: "", Value: []byte("value")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := btree.Put(tc.record)
			if err == nil {
				t.Error("Expected error for invalid record")
			}
		})
	}
}

func TestBTree_MultipleInsertions(t *testing.T) {
	btree := NewBTree(3)

	// Insert records that will trigger node splits
	records := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key6", "value6"},
		{"key7", "value7"},
		{"key8", "value8"},
	}

	// Insert all records
	for _, r := range records {
		record := createTestRecord(r.key, r.value)
		err := btree.Put(record)
		if err != nil {
			t.Fatalf("Failed to insert %s: %v", r.key, err)
		}
	}

	// Verify all records can be retrieved
	for _, r := range records {
		retrieved := btree.Get(r.key)
		if retrieved == nil {
			t.Errorf("Record %s not found", r.key)
			continue
		}
		if string(retrieved.Value) != r.value {
			t.Errorf("Value mismatch for %s: expected %s, got %s",
				r.key, r.value, string(retrieved.Value))
		}
	}

	// Check tree statistics
	stats := btree.GetStats()
	if stats.TotalRecords != len(records) {
		t.Errorf("Expected %d total records, got %d", len(records), stats.TotalRecords)
	}
	if stats.ActiveRecords != len(records) {
		t.Errorf("Expected %d active records, got %d", len(records), stats.ActiveRecords)
	}
}

func TestBTree_UpdateExistingRecord(t *testing.T) {
	btree := NewBTree(3)

	// Insert initial record
	record1 := createTestRecord("key1", "value1")
	btree.Put(record1)

	// Update with new value
	record2 := createTestRecord("key1", "new value1")
	btree.Put(record2)

	// Verify updated value
	retrieved := btree.Get("key1")
	if retrieved == nil {
		t.Fatal("Record not found after update")
	}
	if string(retrieved.Value) != "new value1" {
		t.Errorf("Expected new value1, got %s", string(retrieved.Value))
	}

	// Check that total records count didn't change
	stats := btree.GetStats()
	if stats.TotalRecords != 1 {
		t.Errorf("Expected 1 total record after update, got %d", stats.TotalRecords)
	}
}

func TestBTree_Delete(t *testing.T) {
	btree := NewBTree(3)

	// Insert records
	records := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range records {
		record := createTestRecord(key, "value_"+key)
		btree.Put(record)
	}

	// Delete a record
	deleted := btree.Delete("key3")
	if !deleted {
		t.Error("Delete should return true for existing key")
	}

	// Verify record is not retrievable
	retrieved := btree.Get("key3")
	if retrieved != nil {
		t.Error("Deleted record should not be retrievable")
	}

	// Verify other records are still accessible
	for _, key := range []string{"key1", "key2", "key4", "key5"} {
		retrieved := btree.Get(key)
		if retrieved == nil {
			t.Errorf("Record %s should still be accessible", key)
		}
	}

	// Test deleting non-existent key
	deleted = btree.Delete("nonexistent")
	if deleted {
		t.Error("Delete should return false for non-existent key")
	}

	// Check statistics
	stats := btree.GetStats()
	if stats.TombstonedRecords != 1 {
		t.Errorf("Expected 1 tombstoned record, got %d", stats.TombstonedRecords)
	}
	if stats.ActiveRecords != 4 {
		t.Errorf("Expected 4 active records, got %d", stats.ActiveRecords)
	}
}

func TestBTree_Compaction(t *testing.T) {
	btree := NewBTree(3)

	// Insert many records
	numRecords := 10
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%d", i)
		record := createTestRecord(key, "value"+strconv.Itoa(i))
		btree.Put(record)
	}

	// Delete enough records to trigger compaction (> 30%)
	recordsToDelete := int(float64(numRecords) * 0.4) // 40% to ensure threshold is exceeded
	for i := 0; i < recordsToDelete; i++ {
		key := fmt.Sprintf("key%d", i)
		btree.Delete(key)
	}

	// Force compaction check by adding another record
	btree.Put(createTestRecord("trigger", "compaction"))

	// Verify deleted records are no longer retrievable
	for i := 0; i < recordsToDelete; i++ {
		key := fmt.Sprintf("key%d", i)
		retrieved := btree.Get(key)
		if retrieved != nil {
			t.Errorf("Deleted record %s should not be retrievable after compaction", key)
		}
	}

	// Verify remaining records are still accessible
	for i := recordsToDelete; i < numRecords; i++ {
		key := fmt.Sprintf("key%d", i)
		retrieved := btree.Get(key)
		if retrieved == nil {
			t.Errorf("Record %s should still be accessible after compaction", key)
		}
	}

	// Check that tombstoned records were cleaned up
	stats := btree.GetStats()
	if stats.TombstonedRecords > 0 {
		t.Errorf("Expected 0 tombstoned records after compaction, got %d", stats.TombstonedRecords)
	}
}

func TestBTree_LargeDataset(t *testing.T) {
	btree := NewBTree(5)

	// Insert a large number of records to test tree balancing
	numRecords := 1000
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%05d", i) // Zero-padded for consistent ordering
		record := createTestRecord(key, fmt.Sprintf("value%d", i))
		err := btree.Put(record)
		if err != nil {
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	// Verify all records can be retrieved
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%05d", i)
		retrieved := btree.Get(key)
		if retrieved == nil {
			t.Errorf("Record %s not found", key)
		}
	}

	// Check tree statistics
	stats := btree.GetStats()
	if stats.TotalRecords != numRecords {
		t.Errorf("Expected %d total records, got %d", numRecords, stats.TotalRecords)
	}

	// Verify tree height is reasonable (should be logarithmic)
	height := btree.Height()
	if height > 10 { // For 1000 records with order 5, height should be much less than 10
		t.Errorf("Tree height %d seems too large for %d records", height, numRecords)
	}
}

func TestBTree_TombstonedRecordInsertion(t *testing.T) {
	btree := NewBTree(3)

	// Insert a tombstoned record directly
	tombstonedRecord := createTombstonedRecord("key1", "value1")
	err := btree.Put(tombstonedRecord)
	if err != nil {
		t.Fatalf("Failed to insert tombstoned record: %v", err)
	}

	// Should not be retrievable via Get
	retrieved := btree.Get("key1")
	if retrieved != nil {
		t.Error("Tombstoned record should not be retrievable via Get")
	}

	// Check statistics
	stats := btree.GetStats()
	if stats.TombstonedRecords != 1 {
		t.Errorf("Expected 1 tombstoned record, got %d", stats.TombstonedRecords)
	}
	if stats.ActiveRecords != 0 {
		t.Errorf("Expected 0 active records, got %d", stats.ActiveRecords)
	}
}

func TestBTree_Stats(t *testing.T) {
	btree := NewBTree(3)

	// Initially empty
	stats := btree.GetStats()
	if stats.TotalRecords != 0 || stats.ActiveRecords != 0 || stats.TombstonedRecords != 0 {
		t.Error("Initial stats should be zero")
	}

	// Insert records
	for i := 0; i < 5; i++ {
		record := createTestRecord(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		btree.Put(record)
	}

	stats = btree.GetStats()
	if stats.TotalRecords != 5 || stats.ActiveRecords != 5 || stats.TombstonedRecords != 0 {
		t.Errorf("After insertion: expected 5/5/0, got %d/%d/%d",
			stats.TotalRecords, stats.ActiveRecords, stats.TombstonedRecords)
	}

	// Delete some records
	btree.Delete("key0")
	btree.Delete("key1")

	stats = btree.GetStats()
	if stats.TotalRecords != 5 || stats.ActiveRecords != 3 || stats.TombstonedRecords != 2 {
		t.Errorf("After deletion: expected 5/3/2, got %d/%d/%d",
			stats.TotalRecords, stats.ActiveRecords, stats.TombstonedRecords)
	}
}

func TestBTree_SizeAndActiveSize(t *testing.T) {
	btree := NewBTree(3)

	// Insert records
	for i := 0; i < 10; i++ {
		record := createTestRecord(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		btree.Put(record)
	}

	if btree.Size() != 10 {
		t.Errorf("Expected size 10, got %d", btree.Size())
	}
	if btree.ActiveSize() != 10 {
		t.Errorf("Expected active size 10, got %d", btree.ActiveSize())
	}

	// Delete some records
	for i := 0; i < 3; i++ {
		btree.Delete(fmt.Sprintf("key%d", i))
	}

	if btree.Size() != 10 {
		t.Errorf("Expected size 10 after deletion, got %d", btree.Size())
	}
	if btree.ActiveSize() != 7 {
		t.Errorf("Expected active size 7 after deletion, got %d", btree.ActiveSize())
	}
}

func TestBTree_Height(t *testing.T) {
	btree := NewBTree(3)

	// Empty tree
	if btree.Height() != 0 {
		t.Errorf("Expected height 0 for empty tree, got %d", btree.Height())
	}

	// Single record
	btree.Put(createTestRecord("key1", "value1"))
	if btree.Height() != 1 {
		t.Errorf("Expected height 1 for single record, got %d", btree.Height())
	}

	// Add more records to increase height
	for i := 2; i <= 10; i++ {
		btree.Put(createTestRecord(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	height := btree.Height()
	if height < 1 || height > 4 { // Should be reasonable for 10 records with order 3
		t.Errorf("Unexpected height %d for 10 records", height)
	}
}

// Benchmark tests
func BenchmarkBTree_Put(b *testing.B) {
	btree := NewBTree(5)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		record := createTestRecord(key, "value")
		btree.Put(record)
	}
}

func BenchmarkBTree_Get(b *testing.B) {
	btree := NewBTree(5)

	// Prepare data
	numRecords := 10000
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%05d", i)
		record := createTestRecord(key, "value")
		btree.Put(record)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%05d", i%numRecords)
		btree.Get(key)
	}
}

func BenchmarkBTree_Delete(b *testing.B) {
	numRecords := b.N * 2 // Prepare more records than we'll delete
	btree := NewBTree(5)

	// Prepare data
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("key%05d", i)
		record := createTestRecord(key, "value")
		btree.Put(record)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%05d", i)
		btree.Delete(key)
	}
}
