package skip_list

import (
	"testing"
	"time"

	model "hunddb/model/record"
)

// helper to create records with current timestamp
func rec(key string, val []byte, tomb bool) *model.Record {
	return model.NewRecord(key, val, uint64(time.Now().UnixNano()), tomb)
}

// TestNew verifies basic construction
func TestNewSkipList(t *testing.T) {
	maxHeight := uint64(5)
	capacity := 10
	sl := New(maxHeight, capacity)

	if sl == nil {
		t.Fatal("SkipList is nil")
	}
	if sl.maxHeight != maxHeight {
		t.Errorf("Expected maxHeight=%d, got %d", maxHeight, sl.maxHeight)
	}
	if sl.currentHeight != 1 {
		t.Errorf("Expected currentHeight=1, got %d", sl.currentHeight)
	}
	if sl.head == nil {
		t.Fatal("Head node is nil")
	}
	if sl.Capacity() != capacity {
		t.Errorf("Expected Capacity=%d, got %d", capacity, sl.Capacity())
	}
	if sl.Size() != 0 || sl.TotalEntries() != 0 {
		t.Errorf("Expected Size=0, Total=0; got Size=%d Total=%d", sl.Size(), sl.TotalEntries())
	}
}

// TestAddAndGet verifies Add and Get (no Check)
func TestAddAndGet(t *testing.T) {
	sl := New(5, 100)

	if err := sl.Put(rec("key1", []byte("value1"), false)); err != nil {
		t.Fatalf("Add key1 failed: %v", err)
	}
	if err := sl.Put(rec("key2", []byte("value2"), false)); err != nil {
		t.Fatalf("Add key2 failed: %v", err)
	}
	if err := sl.Put(rec("key3", []byte("value3"), false)); err != nil {
		t.Fatalf("Add key3 failed: %v", err)
	}

	if sl.Size() != 3 || sl.TotalEntries() != 3 {
		t.Errorf("Expected Size=3, Total=3; got Size=%d Total=%d", sl.Size(), sl.TotalEntries())
	}

	if got := sl.Get("key1"); got == nil || string(got.Value) != "value1" || got.Tombstone {
		t.Errorf("Get key1 mismatch: %+v", got)
	}
	if got := sl.Get("key2"); got == nil || string(got.Value) != "value2" || got.Tombstone {
		t.Errorf("Get key2 mismatch: %+v", got)
	}
	if got := sl.Get("key3"); got == nil || string(got.Value) != "value3" || got.Tombstone {
		t.Errorf("Get key3 mismatch: %+v", got)
	}
	if got := sl.Get("missing"); got != nil {
		t.Errorf("Expected nil for missing key, got %+v", got)
	}
}

// TestUpdateSameKey ensures updating same key replaces value and keeps counts
func TestUpdateSameKey(t *testing.T) {
	sl := New(5, 100)

	if err := sl.Put(rec("k", []byte("v1"), false)); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := sl.Put(rec("k", []byte("v2"), false)); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if sl.Size() != 1 || sl.TotalEntries() != 1 {
		t.Errorf("Expected Size=1, Total=1; got Size=%d Total=%d", sl.Size(), sl.TotalEntries())
	}
	if got := sl.Get("k"); got == nil || string(got.Value) != "v2" || got.Tombstone {
		t.Errorf("Expected v2 active, got %+v", got)
	}
}

// TestDeleteTombstone verifies logical deletion
func TestDeleteTombstone(t *testing.T) {
	sl := New(5, 100)

	_ = sl.Put(rec("key1", []byte("v1"), false))
	_ = sl.Put(rec("key2", []byte("v2"), false))
	_ = sl.Put(rec("key3", []byte("v3"), false))

	// delete key2
	existed := sl.Delete(rec("key2", nil, true))
	if !existed {
		t.Errorf("Expected existed=true for deleting existing key2")
	}

	// invisible now
	if got := sl.Get("key2"); got != nil {
		t.Errorf("Expected key2 tombstoned (nil), got %+v", got)
	}

	// other keys intact
	if got := sl.Get("key1"); got == nil || string(got.Value) != "v1" || got.Tombstone {
		t.Errorf("key1 mismatch after delete: %+v", got)
	}
	if got := sl.Get("key3"); got == nil || string(got.Value) != "v3" || got.Tombstone {
		t.Errorf("key3 mismatch after delete: %+v", got)
	}

	// counts: total distinct stays 3, active becomes 2
	if sl.TotalEntries() != 3 || sl.Size() != 2 {
		t.Errorf("Expected Total=3, Size=2; got Total=%d Size=%d", sl.TotalEntries(), sl.Size())
	}
}

// TestDeleteNonExistent inserts a tombstone for unseen key (if capacity allows)
func TestDeleteNonExistent(t *testing.T) {
	sl := New(5, 100)

	_ = sl.Put(rec("key1", []byte("v1"), false))
	_ = sl.Put(rec("key2", []byte("v2"), false))

	// delete missing -> inserts tombstone, returns false
	existed := sl.Delete(rec("key3", nil, true))
	if existed {
		t.Errorf("Expected existed=false when tombstoning unseen key")
	}

	// key3 stays invisible
	if got := sl.Get("key3"); got != nil {
		t.Errorf("Expected nil for key3 after tombstone insert, got %+v", got)
	}

	// counts: total +1 (tombstone), active unchanged
	if sl.TotalEntries() != 3 || sl.Size() != 2 {
		t.Errorf("Expected Total=3, Size=2; got Total=%d Size=%d", sl.TotalEntries(), sl.Size())
	}
}

// TestCapacity enforces ErrCapacityExceeded for new distinct keys
func TestCapacity(t *testing.T) {
	sl := New(5, 2) // capacity for 2 distinct keys

	if err := sl.Put(rec("a", []byte("1"), false)); err != nil {
		t.Fatalf("Add a failed: %v", err)
	}
	if err := sl.Put(rec("b", []byte("2"), false)); err != nil {
		t.Fatalf("Add b failed: %v", err)
	}

	// updating existing key is OK
	if err := sl.Put(rec("a", []byte("1b"), false)); err != nil {
		t.Fatalf("Update a failed: %v", err)
	}

	// inserting third distinct key should fail
	if err := sl.Put(rec("c", []byte("3"), false)); err == nil {
		t.Fatalf("Expected ErrCapacityExceeded, got nil")
	} else if err != ErrCapacityExceeded {
		t.Fatalf("Expected ErrCapacityExceeded, got %v", err)
	}

	// tombstoning unseen key also consumes capacity; here should also fail
	if ok := sl.Delete(rec("x", nil, true)); ok {
		t.Fatalf("Expected delete of unseen 'x' to fail due to capacity (return false), got true")
	}

	// State intact
	if sl.TotalEntries() != 2 || sl.Size() != 2 {
		t.Errorf("Expected Total=2, Size=2; got Total=%d Size=%d", sl.TotalEntries(), sl.Size())
	}
}

// TestTombstoneTransitions checks activeCount updates on transitions
func TestTombstoneTransitions(t *testing.T) {
	sl := New(5, 10)

	_ = sl.Put(rec("k", []byte("v1"), false)) // active
	if sl.Size() != 1 {
		t.Fatalf("Expected Size=1, got %d", sl.Size())
	}

	// tombstone -> active--
	_ = sl.Delete(rec("k", nil, true))
	if sl.Size() != 0 || sl.TotalEntries() != 1 {
		t.Fatalf("After tombstone: Size=0, Total=1 expected; got %d, %d", sl.Size(), sl.TotalEntries())
	}
	if got := sl.Get("k"); got != nil {
		t.Fatalf("Get should be nil after tombstone, got %+v", got)
	}

	// re-add (remove tombstone) -> active++
	if err := sl.Put(rec("k", []byte("v2"), false)); err != nil {
		t.Fatalf("Re-add failed: %v", err)
	}
	if sl.Size() != 1 || sl.TotalEntries() != 1 {
		t.Fatalf("After re-add: Size=1, Total=1 expected; got %d, %d", sl.Size(), sl.TotalEntries())
	}
	if got := sl.Get("k"); got == nil || string(got.Value) != "v2" || got.Tombstone {
		t.Fatalf("Expected v2 active, got %+v", got)
	}
}

// TestRetrieveSortedRecords_Empty verifies empty SkipList returns empty slice
func TestRetrieveSortedRecords_Empty(t *testing.T) {
	sl := New(5, 100)

	records := sl.RetrieveSortedRecords()
	if len(records) != 0 {
		t.Errorf("Expected empty slice, got %d records", len(records))
	}
}

// TestRetrieveSortedRecords_SingleRecord verifies single record retrieval
func TestRetrieveSortedRecords_SingleRecord(t *testing.T) {
	sl := New(5, 100)

	original := rec("key1", []byte("value1"), false)
	if err := sl.Put(original); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	records := sl.RetrieveSortedRecords()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	r := records[0]
	if r.Key != "key1" || string(r.Value) != "value1" || r.Tombstone {
		t.Errorf("Record mismatch: got %+v", r)
	}
}

// TestRetrieveSortedRecords_SortedOrder verifies records are returned in sorted key order
func TestRetrieveSortedRecords_SortedOrder(t *testing.T) {
	sl := New(5, 100)

	// Insert keys in non-sorted order
	keys := []string{"zebra", "apple", "dog", "cat", "banana"}
	values := []string{"z-val", "a-val", "d-val", "c-val", "b-val"}

	for i, key := range keys {
		if err := sl.Put(rec(key, []byte(values[i]), false)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	records := sl.RetrieveSortedRecords()
	if len(records) != 5 {
		t.Fatalf("Expected 5 records, got %d", len(records))
	}

	// Verify sorted order
	expectedKeys := []string{"apple", "banana", "cat", "dog", "zebra"}
	expectedValues := []string{"a-val", "b-val", "c-val", "d-val", "z-val"}

	for i, record := range records {
		if record.Key != expectedKeys[i] {
			t.Errorf("Key at index %d: expected %s, got %s", i, expectedKeys[i], record.Key)
		}
		if string(record.Value) != expectedValues[i] {
			t.Errorf("Value at index %d: expected %s, got %s", i, expectedValues[i], string(record.Value))
		}
		if record.Tombstone {
			t.Errorf("Record at index %d should not be tombstoned", i)
		}
	}
}

// TestRetrieveSortedRecords_WithTombstones verifies tombstones are included in results
func TestRetrieveSortedRecords_WithTombstones(t *testing.T) {
	sl := New(5, 100)

	// Add some records
	_ = sl.Put(rec("a", []byte("val-a"), false))
	_ = sl.Put(rec("b", []byte("val-b"), false))
	_ = sl.Put(rec("c", []byte("val-c"), false))
	_ = sl.Put(rec("d", []byte("val-d"), false))

	// Delete middle records
	_ = sl.Delete(rec("b", nil, true))
	_ = sl.Delete(rec("c", nil, true))

	// Add a tombstone for non-existing key
	_ = sl.Delete(rec("z", nil, true))

	// All records including tombstones
	allRecords := sl.RetrieveSortedRecords()
	expectedCount := 5 // a, b(tomb), c(tomb), d, z(tomb)
	if len(allRecords) != expectedCount {
		t.Fatalf("Expected %d total records, got %d", expectedCount, len(allRecords))
	}

	// Verify order and tombstone status
	expected := []struct {
		key       string
		tombstone bool
	}{
		{"a", false},
		{"b", true},
		{"c", true},
		{"d", false},
		{"z", true},
	}

	for i, exp := range expected {
		record := allRecords[i]
		if record.Key != exp.key {
			t.Errorf("Record[%d]: expected key %s, got %s", i, exp.key, record.Key)
		}
		if record.Tombstone != exp.tombstone {
			t.Errorf("Record[%d]: expected tombstone %v, got %v", i, exp.tombstone, record.Tombstone)
		}
	}
}

// TestRetrieveSortedRecords_UpdatedRecords verifies updated records show latest values
func TestRetrieveSortedRecords_UpdatedRecords(t *testing.T) {
	sl := New(5, 100)

	// Insert initial records
	_ = sl.Put(rec("key1", []byte("value1"), false))
	_ = sl.Put(rec("key2", []byte("value2"), false))

	// Update records
	_ = sl.Put(rec("key1", []byte("updated1"), false))
	_ = sl.Put(rec("key2", []byte("updated2"), false))

	records := sl.RetrieveSortedRecords()
	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}

	// Verify updated values
	if string(records[0].Value) != "updated1" {
		t.Errorf("Expected updated1, got %s", string(records[0].Value))
	}
	if string(records[1].Value) != "updated2" {
		t.Errorf("Expected updated2, got %s", string(records[1].Value))
	}
}

// TestRetrieveSortedRecords_RecordCopy verifies returned records are copies
func TestRetrieveSortedRecords_RecordCopy(t *testing.T) {
	sl := New(5, 100)

	original := rec("key1", []byte("original"), false)
	_ = sl.Put(original)

	records := sl.RetrieveSortedRecords()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	retrieved := records[0]

	// Modify the retrieved record's value
	retrieved.Value[0] = 'X'
	retrieved.Tombstone = true

	// Verify original in SkipList is unchanged
	fromSL := sl.Get("key1")
	if fromSL == nil {
		t.Fatal("Original record should still exist and be active")
	}
	if string(fromSL.Value) != "original" {
		t.Errorf("Original value modified: expected 'original', got %s", string(fromSL.Value))
	}
	if fromSL.Tombstone {
		t.Error("Original record should not be tombstoned")
	}
}

// TestRetrieveSortedRecords_TombstoneResurrection verifies tombstone -> active transitions
func TestRetrieveSortedRecords_TombstoneResurrection(t *testing.T) {
	sl := New(5, 100)

	// Add a record
	_ = sl.Put(rec("key1", []byte("value1"), false))

	// Delete it (tombstone)
	_ = sl.Delete(rec("key1", nil, true))

	// Verify tombstone exists
	records := sl.RetrieveSortedRecords()
	if len(records) != 1 || !records[0].Tombstone {
		t.Fatalf("Expected 1 tombstoned record, got %d records, tombstone=%v",
			len(records), len(records) > 0 && records[0].Tombstone)
	}

	// Resurrect the key
	_ = sl.Put(rec("key1", []byte("resurrected"), false))

	// Verify record is now active
	records = sl.RetrieveSortedRecords()
	if len(records) != 1 || records[0].Tombstone {
		t.Fatalf("Expected 1 active record, got %d records, tombstone=%v",
			len(records), len(records) > 0 && records[0].Tombstone)
	}
	if string(records[0].Value) != "resurrected" {
		t.Errorf("Expected 'resurrected', got %s", string(records[0].Value))
	}
}

// TestRetrieveSortedRecords_MixedOperations verifies complex scenario with multiple operations
func TestRetrieveSortedRecords_MixedOperations(t *testing.T) {
	sl := New(5, 100)

	// Mixed operations: inserts, updates, deletes
	_ = sl.Put(rec("c", []byte("val-c"), false))
	_ = sl.Put(rec("a", []byte("val-a"), false))
	_ = sl.Put(rec("b", []byte("val-b"), false))
	_ = sl.Delete(rec("d", nil, true))               // tombstone for non-existing key
	_ = sl.Put(rec("a", []byte("updated-a"), false)) // update existing
	_ = sl.Delete(rec("b", nil, true))               // delete existing
	_ = sl.Put(rec("e", []byte("val-e"), false))

	records := sl.RetrieveSortedRecords()
	expectedCount := 5 // a(updated), b(tomb), c, d(tomb), e
	if len(records) != expectedCount {
		t.Fatalf("Expected %d records, got %d", expectedCount, len(records))
	}

	// Verify sorted order and correct states
	expected := []struct {
		key       string
		value     string
		tombstone bool
	}{
		{"a", "updated-a", false},
		{"b", "", true}, // tombstoned, value may be empty
		{"c", "val-c", false},
		{"d", "", true}, // tombstoned, value may be empty
		{"e", "val-e", false},
	}

	for i, exp := range expected {
		record := records[i]
		if record.Key != exp.key {
			t.Errorf("Record[%d]: expected key %s, got %s", i, exp.key, record.Key)
		}
		if record.Tombstone != exp.tombstone {
			t.Errorf("Record[%d]: expected tombstone %v, got %v", i, exp.tombstone, record.Tombstone)
		}
		if !exp.tombstone && string(record.Value) != exp.value {
			t.Errorf("Record[%d]: expected value %s, got %s", i, exp.value, string(record.Value))
		}
	}
}

// TestRetrieveSortedRecords_NilValueHandling verifies handling of nil values in tombstones
func TestRetrieveSortedRecords_NilValueHandling(t *testing.T) {
	sl := New(5, 100)

	// Add a record and then delete it with nil value
	_ = sl.Put(rec("key1", []byte("value1"), false))
	tombstoneRec := rec("key1", nil, true)
	_ = sl.Delete(tombstoneRec)

	records := sl.RetrieveSortedRecords()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	record := records[0]
	if !record.Tombstone {
		t.Error("Record should be tombstoned")
	}
	if record.Key != "key1" {
		t.Errorf("Expected key 'key1', got %s", record.Key)
	}
	// Value should be handled gracefully (empty slice, not nil)
	if record.Value == nil {
		t.Error("Value should not be nil (should be empty slice)")
	}
}

// TestRetrieveSortedRecords_LargeDataset verifies performance with many records
func TestRetrieveSortedRecords_LargeDataset(t *testing.T) {
	sl := New(16, 1000) // Higher max height for better performance

	// Insert many records in random order
	numRecords := 100
	for i := 0; i < numRecords; i++ {
		// Create keys that will sort differently than insertion order
		key := string(rune('A'+i%26)) + string(rune('A'+(i/26)%26)) + string(rune('0'+i%10))
		value := []byte("value-" + key)
		if err := sl.Put(rec(key, value, false)); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// Delete every 5th record by walking through and marking some for deletion
	recordsToDelete := []string{}
	tempRecords := sl.RetrieveSortedRecords()
	for i := 0; i < len(tempRecords); i += 5 {
		recordsToDelete = append(recordsToDelete, tempRecords[i].Key)
	}
	for _, key := range recordsToDelete {
		_ = sl.Delete(rec(key, nil, true))
	}

	allRecords := sl.RetrieveSortedRecords()

	if len(allRecords) != numRecords {
		t.Errorf("Expected %d total records, got %d", numRecords, len(allRecords))
	}

	// Verify records are in sorted order
	for i := 1; i < len(allRecords); i++ {
		if allRecords[i-1].Key >= allRecords[i].Key {
			t.Errorf("Records not in sorted order: %s >= %s at indices %d, %d",
				allRecords[i-1].Key, allRecords[i].Key, i-1, i)
		}
	}

	// Count tombstones and verify they match our deletions
	tombstoneCount := 0
	for _, record := range allRecords {
		if record.Tombstone {
			tombstoneCount++
		}
	}

	if tombstoneCount != len(recordsToDelete) {
		t.Errorf("Expected %d tombstones, got %d", len(recordsToDelete), tombstoneCount)
	}
}
