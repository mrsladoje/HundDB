package hashmap

import (
	"fmt"
	"math"
	"testing"
	"time"

	model "hunddb/model/record"
)

// ---- helpers ----

func makeRec(key, val string) *model.Record {
	return &model.Record{
		Key:       key,
		Value:     []byte(val),
		Tombstone: false,
		Timestamp: uint64(time.Now().UnixNano()),
	}
}

func makeTomb(key string) *model.Record {
	r := makeRec(key, "")
	r.Tombstone = true
	return r
}

func rec(key string, val []byte, tomb bool) *model.Record {
	return model.NewRecord(key, val, uint64(time.Now().UnixNano()), tomb)
}

// ---- tests ----

func TestHashMap_NewHashMap(t *testing.T) {
	hm := NewHashMap(0) // <=0 -> unbounded
	if hm.Capacity() != math.MaxInt {
		t.Fatalf("expected unbounded capacity=%d, got %d", math.MaxInt, hm.Capacity())
	}

	hm2 := NewHashMap(5)
	if hm2.Capacity() != 5 {
		t.Fatalf("expected capacity=5, got %d", hm2.Capacity())
	}
}

func TestHashMap_AddAndGet_Single(t *testing.T) {
	hm := NewHashMap(math.MaxInt)

	if err := hm.Put(makeRec("k1", "v1")); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	got := hm.Get("k1")
	if got == nil || got.Key != "k1" || string(got.Value) != "v1" {
		t.Fatalf("Get mismatch: want k1/v1, got %#v", got)
	}

	if hm.Get("nope") != nil {
		t.Fatalf("expected nil for non-existent key")
	}
}

func TestHashMap_AddInvalid(t *testing.T) {
	hm := NewHashMap(math.MaxInt)

	if err := hm.Put(nil); err == nil {
		t.Fatalf("expected error for nil record")
	}
	if err := hm.Put(&model.Record{Key: ""}); err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestHashMap_UpdateExisting(t *testing.T) {
	hm := NewHashMap(math.MaxInt)

	_ = hm.Put(makeRec("k1", "v1"))
	_ = hm.Put(makeRec("k1", "v2")) // update

	got := hm.Get("k1")
	if got == nil || string(got.Value) != "v2" {
		t.Fatalf("expected updated value v2, got %#v", got)
	}

	if hm.TotalEntries() != 1 {
		t.Fatalf("expected TotalEntries=1 after update, got %d", hm.TotalEntries())
	}
	if hm.Size() != 1 {
		t.Fatalf("expected Size=1 after update, got %d", hm.Size())
	}
}

func TestHashMap_Delete_ExistingAndBlind(t *testing.T) {
	hm := NewHashMap(math.MaxInt)

	// Insert 5 keys
	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	for _, k := range keys {
		_ = hm.Put(makeRec(k, "v_"+k))
	}

	// Delete existing
	if ok := hm.Delete(makeTomb("k3")); !ok {
		t.Fatalf("delete existing should return true")
	}
	if hm.Get("k3") != nil {
		t.Fatalf("deleted key should not be retrievable")
	}

	// Others still retrievable
	for _, k := range []string{"k1", "k2", "k4", "k5"} {
		if hm.Get(k) == nil {
			t.Fatalf("key %s should be retrievable", k)
		}
	}

	// Delete non-existent -> blind tombstone (returns false), increases TotalEntries
	if ok := hm.Delete(makeTomb("ghost")); ok {
		t.Fatalf("delete of non-existent should return false (blind tombstone)")
	}

	// After: active=4 (k1,k2,k4,k5), total=6 (5 originals + ghost tombstone)
	if hm.Size() != 4 {
		t.Fatalf("expected Size=4, got %d", hm.Size())
	}
	if hm.TotalEntries() != 6 {
		t.Fatalf("expected TotalEntries=6, got %d", hm.TotalEntries())
	}
	// ghost is tombstoned, not retrievable
	if hm.Get("ghost") != nil {
		t.Fatalf("ghost should be tombstoned and not retrievable")
	}
}

func TestHashMap_IsFullAndCapacity(t *testing.T) {
	hm := NewHashMap(2)

	// Fill with 2 distinct keys
	if err := hm.Put(makeRec("a", "1")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := hm.Put(makeRec("b", "2")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !hm.IsFull() {
		t.Fatalf("expected IsFull=true")
	}

	// New distinct key should fail (capacity applies to NEW keys)
	if err := hm.Put(makeRec("c", "3")); err == nil {
		t.Fatalf("expected capacity error for new key when full")
	}

	// Update existing should still succeed when "full"
	if err := hm.Put(makeRec("b", "22")); err != nil {
		t.Fatalf("update existing should not be blocked by capacity: %v", err)
	}
	if got := hm.Get("b"); got == nil || string(got.Value) != "22" {
		t.Fatalf("expected updated b=22, got %#v", got)
	}

	// Delete non-existent when full -> blind tombstone should NOT be inserted
	if ok := hm.Delete(makeTomb("zzz")); ok {
		t.Fatalf("delete of non-existent should return false")
	}
	if hm.TotalEntries() != 2 {
		t.Fatalf("total should remain 2, got %d", hm.TotalEntries())
	}
}

func TestHashMap_SizeAndTotals(t *testing.T) {
	hm := NewHashMap(math.MaxInt)

	// Add 10
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		_ = hm.Put(makeRec(key, "v"))
	}
	if hm.Size() != 10 || hm.TotalEntries() != 10 {
		t.Fatalf("after add: expected size=10 total=10, got %d/%d", hm.Size(), hm.TotalEntries())
	}

	// Delete 3 existing keys
	for _, k := range []string{"k0", "k1", "k2"} {
		_ = hm.Delete(makeTomb(k))
	}
	if hm.Size() != 7 {
		t.Fatalf("after deletes: expected size=7, got %d", hm.Size())
	}
	if hm.TotalEntries() != 10 {
		t.Fatalf("after deletes: expected total still 10, got %d", hm.TotalEntries())
	}

	// Blind tombstone one missing key -> total +1, size unchanged
	_ = hm.Delete(makeTomb("missing"))
	if hm.Size() != 7 || hm.TotalEntries() != 11 {
		t.Fatalf("after blind tombstone: expected size=7 total=11, got %d/%d", hm.Size(), hm.TotalEntries())
	}
}

// TestRetrieveSortedRecords_Empty verifies empty HashMap returns empty slice
func TestHashMapRetrieveSortedRecords_Empty(t *testing.T) {
	hm := NewHashMap(100)

	records := hm.RetrieveSortedRecords()
	if len(records) != 0 {
		t.Errorf("Expected empty slice, got %d records", len(records))
	}
}

// TestRetrieveSortedRecords_SingleRecord verifies single record retrieval
func TestHashMapRetrieveSortedRecords_SingleRecord(t *testing.T) {
	hm := NewHashMap(100)

	original := rec("key1", []byte("value1"), false)
	if err := hm.Put(original); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	records := hm.RetrieveSortedRecords()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	r := records[0]
	if r.Key != "key1" || string(r.Value) != "value1" || r.Tombstone {
		t.Errorf("Record mismatch: got %+v", r)
	}
}

// TestRetrieveSortedRecords_SortedOrder verifies records are returned in sorted key order
func TestHashMapRetrieveSortedRecords_SortedOrder(t *testing.T) {
	hm := NewHashMap(100)

	// Insert keys in non-sorted order
	keys := []string{"zebra", "apple", "dog", "cat", "banana"}
	values := []string{"z-val", "a-val", "d-val", "c-val", "b-val"}

	for i, key := range keys {
		if err := hm.Put(rec(key, []byte(values[i]), false)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
	}

	records := hm.RetrieveSortedRecords()
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
func TestHashMapRetrieveSortedRecords_WithTombstones(t *testing.T) {
	hm := NewHashMap(100)

	// Add some records
	_ = hm.Put(rec("a", []byte("val-a"), false))
	_ = hm.Put(rec("b", []byte("val-b"), false))
	_ = hm.Put(rec("c", []byte("val-c"), false))
	_ = hm.Put(rec("d", []byte("val-d"), false))

	// Delete middle records
	_ = hm.Delete(rec("b", nil, true))
	_ = hm.Delete(rec("c", nil, true))

	// Add a tombstone for non-existing key
	_ = hm.Delete(rec("z", nil, true))

	// All records including tombstones
	allRecords := hm.RetrieveSortedRecords()
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
func TestHashMapRetrieveSortedRecords_UpdatedRecords(t *testing.T) {
	hm := NewHashMap(100)

	// Insert initial records
	_ = hm.Put(rec("key1", []byte("value1"), false))
	_ = hm.Put(rec("key2", []byte("value2"), false))

	// Update records
	_ = hm.Put(rec("key1", []byte("updated1"), false))
	_ = hm.Put(rec("key2", []byte("updated2"), false))

	records := hm.RetrieveSortedRecords()
	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}

	// Verify updated values (key1 comes before key2 in sorted order)
	if string(records[0].Value) != "updated1" {
		t.Errorf("Expected updated1, got %s", string(records[0].Value))
	}
	if string(records[1].Value) != "updated2" {
		t.Errorf("Expected updated2, got %s", string(records[1].Value))
	}
}

// TestRetrieveSortedRecords_RecordCopy verifies returned records are copies
func TestHashMapRetrieveSortedRecords_RecordCopy(t *testing.T) {
	hm := NewHashMap(100)

	original := rec("key1", []byte("original"), false)
	_ = hm.Put(original)

	records := hm.RetrieveSortedRecords()
	if len(records) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(records))
	}

	retrieved := records[0]

	// Modify the retrieved record's value
	retrieved.Value[0] = 'X'

	// Verify original in HashMap is unchanged
	fromHM := hm.Get("key1")
	if fromHM == nil {
		t.Fatal("Original record should still exist and be active")
	}
	if string(fromHM.Value) != "original" {
		t.Errorf("Original value modified: expected 'original', got %s", string(fromHM.Value))
	}
	if fromHM.Tombstone {
		t.Error("Original record should not be tombstoned")
	}
}

// TestRetrieveSortedRecords_TombstoneResurrection verifies tombstone -> active transitions
func TestHashMapRetrieveSortedRecords_TombstoneResurrection(t *testing.T) {
	hm := NewHashMap(100)

	// Add a record
	_ = hm.Put(rec("key1", []byte("value1"), false))

	// Delete it (tombstone)
	_ = hm.Delete(rec("key1", nil, true))

	// Verify tombstone exists
	records := hm.RetrieveSortedRecords()
	if len(records) != 1 || !records[0].Tombstone {
		t.Fatalf("Expected 1 tombstoned record, got %d records, tombstone=%v",
			len(records), len(records) > 0 && records[0].Tombstone)
	}

	// Resurrect the key
	_ = hm.Put(rec("key1", []byte("resurrected"), false))

	// Verify record is now active
	records = hm.RetrieveSortedRecords()
	if len(records) != 1 || records[0].Tombstone {
		t.Fatalf("Expected 1 active record, got %d records, tombstone=%v",
			len(records), len(records) > 0 && records[0].Tombstone)
	}
	if string(records[0].Value) != "resurrected" {
		t.Errorf("Expected 'resurrected', got %s", string(records[0].Value))
	}
}

// TestRetrieveSortedRecords_MixedOperations verifies complex scenario with multiple operations
func TestHashMapRetrieveSortedRecords_MixedOperations(t *testing.T) {
	hm := NewHashMap(100)

	// Mixed operations: inserts, updates, deletes
	_ = hm.Put(rec("c", []byte("val-c"), false))
	_ = hm.Put(rec("a", []byte("val-a"), false))
	_ = hm.Put(rec("b", []byte("val-b"), false))
	_ = hm.Delete(rec("d", nil, true))               // tombstone for non-existing key
	_ = hm.Put(rec("a", []byte("updated-a"), false)) // update existing
	_ = hm.Delete(rec("b", nil, true))               // delete existing
	_ = hm.Put(rec("e", []byte("val-e"), false))

	records := hm.RetrieveSortedRecords()
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
func TestHashMapRetrieveSortedRecords_NilValueHandling(t *testing.T) {
	hm := NewHashMap(100)

	// Add a record and then delete it with nil value
	_ = hm.Put(rec("key1", []byte("value1"), false))
	tombstoneRec := rec("key1", nil, true)
	_ = hm.Delete(tombstoneRec)

	records := hm.RetrieveSortedRecords()
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

// TestRetrieveSortedRecords_Capacity verifies behavior with capacity constraints
func TestHashMapRetrieveSortedRecords_Capacity(t *testing.T) {
	hm := NewHashMap(3) // Small capacity

	// Fill to capacity
	_ = hm.Put(rec("c", []byte("val-c"), false))
	_ = hm.Put(rec("a", []byte("val-a"), false))
	_ = hm.Put(rec("b", []byte("val-b"), false))

	// Attempt to add one more (should fail)
	err := hm.Put(rec("d", []byte("val-d"), false))
	if err == nil {
		t.Fatal("Expected capacity error when adding 4th record")
	}

	// Verify only 3 records in sorted order
	records := hm.RetrieveSortedRecords()
	if len(records) != 3 {
		t.Fatalf("Expected 3 records, got %d", len(records))
	}

	expectedKeys := []string{"a", "b", "c"}
	for i, expected := range expectedKeys {
		if records[i].Key != expected {
			t.Errorf("Record[%d]: expected key %s, got %s", i, expected, records[i].Key)
		}
	}

	// Update existing record (should work even at capacity)
	_ = hm.Put(rec("a", []byte("updated-a"), false))

	records = hm.RetrieveSortedRecords()
	if len(records) != 3 {
		t.Fatalf("Expected 3 records after update, got %d", len(records))
	}

	if string(records[0].Value) != "updated-a" {
		t.Errorf("Expected updated value, got %s", string(records[0].Value))
	}
}

// TestRetrieveSortedRecords_LargeDataset verifies performance with many records
func TestHashMapRetrieveSortedRecords_LargeDataset(t *testing.T) {
	hm := NewHashMap(1000)

	// Insert many records in random order
	numRecords := 100
	for i := 0; i < numRecords; i++ {
		// Create keys that will sort differently than insertion order
		key := string(rune('A'+i%26)) + string(rune('A'+(i/26)%26)) + string(rune('0'+i%10))
		value := []byte("value-" + key)
		if err := hm.Put(rec(key, value, false)); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// Delete every 5th record
	recordsToDelete := []string{}
	tempRecords := hm.RetrieveSortedRecords()
	for i := 0; i < len(tempRecords); i += 5 {
		recordsToDelete = append(recordsToDelete, tempRecords[i].Key)
	}
	for _, key := range recordsToDelete {
		_ = hm.Delete(rec(key, nil, true))
	}

	allRecords := hm.RetrieveSortedRecords()

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

// TestRetrieveSortedRecords_EmptyValuesAndKeys verifies edge cases
func TestHashMapRetrieveSortedRecords_EmptyValuesAndKeys(t *testing.T) {
	hm := NewHashMap(100)

	// Add records with empty values
	_ = hm.Put(rec("key1", []byte(""), false))
	_ = hm.Put(rec("key2", []byte{}, false))
	_ = hm.Put(rec("key3", nil, false))

	records := hm.RetrieveSortedRecords()
	if len(records) != 3 {
		t.Fatalf("Expected 3 records, got %d", len(records))
	}

	// All should be in sorted order
	expectedKeys := []string{"key1", "key2", "key3"}
	for i, expected := range expectedKeys {
		if records[i].Key != expected {
			t.Errorf("Record[%d]: expected key %s, got %s", i, expected, records[i].Key)
		}
		// Values should be handled gracefully (empty slice, not nil)
		if records[i].Value == nil {
			t.Errorf("Record[%d]: Value should not be nil", i)
		}
	}
}

func TestHashMap_GetNextForPrefix_WithKey_FirstRecord(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("prefix123", "value1"))
	_ = hm.Put(makeRec("prefix456", "value2"))
	_ = hm.Put(makeRec("prefix789", "value3"))

	tombstoned := []string{}
	// Start iteration from beginning (empty key should return first match)
	result := hm.GetNextForPrefix("prefix", "", &tombstoned)

	if result == nil {
		t.Fatal("GetNextForPrefix should find first match")
	}
	if result.Key != "prefix123" {
		t.Errorf("Expected key 'prefix123', got '%s'", result.Key)
	}
}

func TestHashMap_GetNextForPrefix_WithKey_IterateNext(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("prefix123", "value1"))
	_ = hm.Put(makeRec("prefix456", "value2"))
	_ = hm.Put(makeRec("prefix789", "value3"))

	tombstoned := []string{}
	// Get next after prefix123
	result := hm.GetNextForPrefix("prefix", "prefix123", &tombstoned)

	if result == nil {
		t.Fatal("GetNextForPrefix should find next match")
	}
	if result.Key != "prefix456" {
		t.Errorf("Expected key 'prefix456', got '%s'", result.Key)
	}

	// Get next after prefix456
	result = hm.GetNextForPrefix("prefix", "prefix456", &tombstoned)
	if result == nil {
		t.Fatal("GetNextForPrefix should find next match")
	}
	if result.Key != "prefix789" {
		t.Errorf("Expected key 'prefix789', got '%s'", result.Key)
	}

	// Get next after prefix789 (should be nil)
	result = hm.GetNextForPrefix("prefix", "prefix789", &tombstoned)
	if result != nil {
		t.Errorf("GetNextForPrefix should return nil after last match, got %v", result)
	}
}

func TestHashMap_GetNextForPrefix_WithKey_SkipTombstoned(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("prefix123", "value1"))
	_ = hm.Put(makeRec("prefix456", "value2"))
	_ = hm.Put(makeRec("prefix789", "value3"))

	// Mark prefix456 as tombstoned locally
	_ = hm.Delete(makeTomb("prefix456"))

	tombstoned := []string{}
	// Get next after prefix123 (should skip tombstoned prefix456)
	result := hm.GetNextForPrefix("prefix", "prefix123", &tombstoned)

	if result == nil {
		t.Fatal("GetNextForPrefix should find next non-tombstoned match")
	}
	if result.Key != "prefix789" {
		t.Errorf("Expected key 'prefix789', got '%s'", result.Key)
	}
	// Tombstoned slice should contain prefix456
	if len(tombstoned) != 1 || tombstoned[0] != "prefix456" {
		t.Errorf("Expected tombstoned slice to contain 'prefix456', got %v", tombstoned)
	}
}

func TestHashMap_GetNextForPrefix_WithKey_ExternalTombstones(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("prefix123", "value1"))
	_ = hm.Put(makeRec("prefix456", "value2"))
	_ = hm.Put(makeRec("prefix789", "value3"))

	// Simulate external tombstones
	tombstoned := []string{"prefix456"}
	result := hm.GetNextForPrefix("prefix", "prefix123", &tombstoned)

	if result == nil {
		t.Fatal("GetNextForPrefix should find next non-tombstoned match")
	}
	if result.Key != "prefix789" {
		t.Errorf("Expected key 'prefix789', got '%s'", result.Key)
	}
}

func TestHashMap_GetNextForPrefix_WithKey_NoMatch(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("other123", "value1"))
	_ = hm.Put(makeRec("other456", "value2"))

	tombstoned := []string{}
	result := hm.GetNextForPrefix("prefix", "", &tombstoned)

	if result != nil {
		t.Errorf("GetNextForPrefix should return nil when no prefix match, got %v", result)
	}
}

func TestHashMap_GetNextForPrefix_WithKey_FullIteration(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	expectedKeys := []string{"user001", "user003", "user005", "user007", "user009"}
	for _, key := range expectedKeys {
		_ = hm.Put(makeRec(key, "value"))
	}

	// Also add some with different prefix
	_ = hm.Put(makeRec("admin001", "value"))
	_ = hm.Put(makeRec("admin002", "value"))

	tombstoned := []string{}
	var foundKeys []string

	// Iterate through all user keys
	currentKey := ""
	for {
		result := hm.GetNextForPrefix("user", currentKey, &tombstoned)
		if result == nil {
			break
		}
		foundKeys = append(foundKeys, result.Key)
		currentKey = result.Key
	}

	if len(foundKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, found %d: %v", len(expectedKeys), len(foundKeys), foundKeys)
	}

	for i, expected := range expectedKeys {
		if foundKeys[i] != expected {
			t.Errorf("Key at index %d: expected %s, got %s", i, expected, foundKeys[i])
		}
	}
}

func TestHashMap_GetNextForPrefix_WithKey_MixedTombstones(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("prefix123", "value1"))
	_ = hm.Put(makeRec("prefix456", "value2"))
	_ = hm.Put(makeRec("prefix789", "value3"))
	_ = hm.Put(makeRec("prefix999", "value4"))

	// Mark prefix456 as tombstoned locally
	_ = hm.Delete(makeTomb("prefix456"))

	// Simulate that prefix789 was tombstoned in a more recent memtable
	tombstoned := []string{"prefix789"}
	result := hm.GetNextForPrefix("prefix", "prefix123", &tombstoned)

	if result == nil {
		t.Fatal("GetNextForPrefix should find next non-tombstoned match")
	}
	if result.Key != "prefix999" {
		t.Errorf("Expected key 'prefix999', got '%s'", result.Key)
	}
	// Tombstoned slice should now contain both keys
	expectedTombstoned := []string{"prefix789", "prefix456"}
	if len(tombstoned) != 2 {
		t.Fatalf("Expected 2 tombstoned keys, got %d: %v", len(tombstoned), tombstoned)
	}
	for _, expected := range expectedTombstoned {
		found := false
		for _, actual := range tombstoned {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected tombstoned key '%s' not found in %v", expected, tombstoned)
		}
	}
}

func TestHashMap_ScanForPrefix_EmptyHashMap(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("prefix", &tombstoned, &bestKeys, 10, 0)

	if len(bestKeys) != 0 {
		t.Errorf("Expected no keys from empty hashmap, got %d keys", len(bestKeys))
	}
}

func TestHashMap_ScanForPrefix_BasicScan(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	// Add records with matching prefix
	_ = hm.Put(makeRec("user123", "value1"))
	_ = hm.Put(makeRec("user456", "value2"))
	_ = hm.Put(makeRec("user789", "value3"))
	// Add records with different prefix
	_ = hm.Put(makeRec("admin001", "value4"))

	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	expectedKeys := []string{"user123", "user456", "user789"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	for i, expected := range expectedKeys {
		if bestKeys[i] != expected {
			t.Errorf("Key at index %d: expected %s, got %s", i, expected, bestKeys[i])
		}
	}
}

func TestHashMap_ScanForPrefix_SkipLocalTombstones(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("user123", "value1"))
	_ = hm.Put(makeRec("user456", "value2"))
	_ = hm.Put(makeRec("user789", "value3"))

	// Mark user456 as tombstoned locally
	_ = hm.Delete(makeTomb("user456"))

	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	// Should only get non-tombstoned keys
	expectedKeys := []string{"user123", "user789"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	// Should have added tombstoned key to the slice
	if len(tombstoned) != 1 || tombstoned[0] != "user456" {
		t.Errorf("Expected tombstoned slice to contain 'user456', got %v", tombstoned)
	}
}

func TestHashMap_ScanForPrefix_SkipExternalTombstones(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("user123", "value1"))
	_ = hm.Put(makeRec("user456", "value2"))
	_ = hm.Put(makeRec("user789", "value3"))

	// Simulate external tombstones
	tombstoned := []string{"user456"}
	bestKeys := []string{}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	// Should skip externally tombstoned keys
	expectedKeys := []string{"user123", "user789"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}
}

func TestHashMap_ScanForPrefix_AvoidDuplicates(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("user123", "value1"))
	_ = hm.Put(makeRec("user456", "value2"))
	_ = hm.Put(makeRec("user789", "value3"))

	// Simulate existing best keys from previous memtables
	tombstoned := []string{}
	bestKeys := []string{"user123", "user999"}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	// Should maintain sorted order and avoid duplicates
	expectedKeys := []string{"user123", "user456", "user789", "user999"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	for i, expected := range expectedKeys {
		if bestKeys[i] != expected {
			t.Errorf("Key at index %d: expected %s, got %s", i, expected, bestKeys[i])
		}
	}
}

func TestHashMap_ScanForPrefix_MaintainsSortedOrder(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	// Insert keys in random order to HashMap
	keys := []string{"user789", "user123", "user456", "user001", "user999"}
	for _, key := range keys {
		_ = hm.Put(makeRec(key, "value"))
	}

	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	// Should be returned in sorted order
	expectedKeys := []string{"user001", "user123", "user456", "user789", "user999"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	for i, expected := range expectedKeys {
		if bestKeys[i] != expected {
			t.Errorf("Key at index %d: expected %s, got %s", i, expected, bestKeys[i])
		}
	}
}

func TestHashMap_ScanForPrefix_NoMatches(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("admin123", "value1"))
	_ = hm.Put(makeRec("admin456", "value2"))

	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	if len(bestKeys) != 0 {
		t.Errorf("Expected no keys for non-matching prefix, got %d keys: %v", len(bestKeys), bestKeys)
	}
}

func TestHashMap_ScanForPrefix_NilParameters(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("user123", "value1"))
	_ = hm.Put(makeRec("user456", "value2"))

	// Test with nil parameters (should not panic)
	hm.ScanForPrefix("user", nil, nil, 10, 0)

	// Test with nil tombstoned only
	bestKeys := []string{}
	hm.ScanForPrefix("user", nil, &bestKeys, 10, 0)

	expectedKeys := []string{"user123", "user456"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}
}

func TestHashMap_ScanForPrefix_MixedOperations(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	// Mixed operations: inserts, updates, deletes
	_ = hm.Put(makeRec("user003", "value3"))
	_ = hm.Put(makeRec("user001", "value1"))
	_ = hm.Put(makeRec("user002", "value2"))
	_ = hm.Put(makeRec("user001", "updated1")) // update existing
	_ = hm.Delete(makeTomb("user002"))         // delete existing
	_ = hm.Put(makeRec("user004", "value4"))

	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	// Should only get non-tombstoned keys in sorted order
	expectedKeys := []string{"user001", "user003", "user004"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	for i, expected := range expectedKeys {
		if bestKeys[i] != expected {
			t.Errorf("Key at index %d: expected %s, got %s", i, expected, bestKeys[i])
		}
	}

	// Should have added tombstoned key to the slice
	if len(tombstoned) != 1 || tombstoned[0] != "user002" {
		t.Errorf("Expected tombstoned slice to contain 'user002', got %v", tombstoned)
	}
}

func TestHashMap_ScanForPrefix_CombinedWithPreviousResults(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("user003", "value3"))
	_ = hm.Put(makeRec("user007", "value7"))
	_ = hm.Put(makeRec("user009", "value9"))

	// Simulate previous results from newer memtables
	tombstoned := []string{"user005"}                     // tombstoned in newer memtable
	bestKeys := []string{"user001", "user005", "user011"} // from newer memtables
	hm.ScanForPrefix("user", &tombstoned, &bestKeys, 10, 0)

	// Should merge and maintain sorted order
	expectedKeys := []string{"user001", "user003", "user005", "user007", "user009", "user011"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	for i, expected := range expectedKeys {
		if bestKeys[i] != expected {
			t.Errorf("Key at index %d: expected %s, got %s", i, expected, bestKeys[i])
		}
	}
}

func TestHashMap_ScanForPrefix_EdgeCasePrefixes(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	_ = hm.Put(makeRec("a", "value"))
	_ = hm.Put(makeRec("ab", "value"))
	_ = hm.Put(makeRec("abc", "value"))
	_ = hm.Put(makeRec("abcd", "value"))
	_ = hm.Put(makeRec("abd", "value"))
	_ = hm.Put(makeRec("b", "value"))

	// Test empty prefix (should match all)
	tombstoned := []string{}
	bestKeys := []string{}
	hm.ScanForPrefix("", &tombstoned, &bestKeys, 10, 0)

	expectedKeys := []string{"a", "ab", "abc", "abcd", "abd", "b"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys for empty prefix, got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	// Test single character prefix
	tombstoned = []string{}
	bestKeys = []string{}
	hm.ScanForPrefix("a", &tombstoned, &bestKeys, 10, 0)

	expectedKeys = []string{"a", "ab", "abc", "abcd", "abd"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys for prefix 'a', got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}

	// Test longer prefix
	tombstoned = []string{}
	bestKeys = []string{}
	hm.ScanForPrefix("abc", &tombstoned, &bestKeys, 10, 0)

	expectedKeys = []string{"abc", "abcd"}
	if len(bestKeys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys for prefix 'abc', got %d: %v", len(expectedKeys), len(bestKeys), bestKeys)
	}
}

func TestHashMap_GetNextForRange_WithKey_FirstRecord(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key123", "value1"))
	_ = hm.Put(makeRec("key456", "value2"))
	_ = hm.Put(makeRec("key789", "value3"))

	tombstoned := []string{}
	// Start iteration from beginning (empty key should return first match in range)
	result := hm.GetNextForRange("key100", "key800", "", &tombstoned)

	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key123" {
		t.Errorf("Expected key123, got %s", result.Key)
	}
}

func TestHashMap_GetNextForRange_WithKey_IterateNext(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key123", "value1"))
	_ = hm.Put(makeRec("key456", "value2"))
	_ = hm.Put(makeRec("key789", "value3"))

	tombstoned := []string{}
	// Get next after key123 within range
	result := hm.GetNextForRange("key100", "key800", "key123", &tombstoned)

	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key456" {
		t.Errorf("Expected key456, got %s", result.Key)
	}

	// Get next after key456 within range
	result = hm.GetNextForRange("key100", "key800", "key456", &tombstoned)
	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key789" {
		t.Errorf("Expected key789, got %s", result.Key)
	}

	// Get next after key789 (should be nil)
	result = hm.GetNextForRange("key100", "key800", "key789", &tombstoned)
	if result != nil {
		t.Errorf("Expected nil, got %s", result.Key)
	}
}

func TestHashMap_GetNextForRange_WithKey_RangeConstraints(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key100", "value1"))
	_ = hm.Put(makeRec("key200", "value2"))
	_ = hm.Put(makeRec("key300", "value3"))
	_ = hm.Put(makeRec("key400", "value4"))
	_ = hm.Put(makeRec("key500", "value5"))

	tombstoned := []string{}
	// Range [key150, key350) should only include key200 and key300
	result := hm.GetNextForRange("key150", "key350", "", &tombstoned)

	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key200" {
		t.Errorf("Expected key200, got %s", result.Key)
	}

	// Get next after key200 within range
	result = hm.GetNextForRange("key150", "key350", "key200", &tombstoned)
	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key300" {
		t.Errorf("Expected key300, got %s", result.Key)
	}

	// Get next after key300 within range (should be nil, key400 is out of range)
	result = hm.GetNextForRange("key150", "key350", "key300", &tombstoned)
	if result != nil {
		t.Errorf("Expected nil (key out of range), got %s", result.Key)
	}
}

func TestHashMap_GetNextForRange_WithKey_SkipTombstoned(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key123", "value1"))
	_ = hm.Put(makeRec("key456", "value2"))
	_ = hm.Put(makeRec("key789", "value3"))

	// Mark key456 as tombstoned locally
	_ = hm.Delete(makeTomb("key456"))

	tombstoned := []string{}
	// Get next after key123 (should skip tombstoned key456)
	result := hm.GetNextForRange("key100", "key800", "key123", &tombstoned)

	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key789" {
		t.Errorf("Expected key789, got %s", result.Key)
	}
	// Tombstoned slice should contain key456
	if len(tombstoned) != 1 || tombstoned[0] != "key456" {
		t.Errorf("Expected tombstoned slice to contain key456, got %v", tombstoned)
	}
}

func TestHashMap_GetNextForRange_WithKey_ExternalTombstones(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key123", "value1"))
	_ = hm.Put(makeRec("key456", "value2"))
	_ = hm.Put(makeRec("key789", "value3"))

	// Simulate external tombstones
	tombstoned := []string{"key456"}
	result := hm.GetNextForRange("key100", "key800", "key123", &tombstoned)

	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key789" {
		t.Errorf("Expected key789, got %s", result.Key)
	}
}

func TestHashMap_GetNextForRange_WithKey_NoMatch(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key100", "value1"))
	_ = hm.Put(makeRec("key900", "value2"))

	tombstoned := []string{}
	// Range [key200, key800) should not match any records
	result := hm.GetNextForRange("key200", "key800", "", &tombstoned)

	if result != nil {
		t.Errorf("Expected nil (no records in range), got %s", result.Key)
	}
}

func TestHashMap_GetNextForRange_WithKey_EmptyRange(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key456", "value1"))

	tombstoned := []string{}
	// Empty range should return nil
	result := hm.GetNextForRange("key500", "key400", "", &tombstoned)

	if result != nil {
		t.Errorf("Expected nil (empty range), got %s", result.Key)
	}
}

func TestHashMap_GetNextForRange_WithKey_FullIteration(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)

	expectedKeys := []string{"user001", "user003", "user005", "user007", "user009"}
	for _, key := range expectedKeys {
		_ = hm.Put(makeRec(key, "value"))
	}

	// Also add some keys outside the range
	_ = hm.Put(makeRec("admin001", "value"))
	_ = hm.Put(makeRec("zuser001", "value"))

	tombstoned := []string{}
	var foundKeys []string

	// Iterate through all user keys in range [user000, user999)
	currentKey := ""
	for {
		result := hm.GetNextForRange("user000", "user999", currentKey, &tombstoned)
		if result == nil {
			break
		}
		foundKeys = append(foundKeys, result.Key)
		currentKey = result.Key
	}

	if len(foundKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(foundKeys))
	}

	for i, expected := range expectedKeys {
		if i >= len(foundKeys) || foundKeys[i] != expected {
			t.Errorf("At index %d: expected %s, got %s", i, expected, foundKeys[i])
		}
	}
}

func TestHashMap_GetNextForRange_WithKey_MixedTombstones(t *testing.T) {
	t.Parallel()
	hm := NewHashMap(100)
	_ = hm.Put(makeRec("key123", "value1"))
	_ = hm.Put(makeRec("key456", "value2"))
	_ = hm.Put(makeRec("key789", "value3"))
	_ = hm.Put(makeRec("key999", "value4"))

	// Mark key456 as tombstoned locally
	_ = hm.Delete(makeTomb("key456"))

	// Simulate that key789 was tombstoned in a more recent memtable
	tombstoned := []string{"key789"}
	result := hm.GetNextForRange("key100", "key999", "key123", &tombstoned)

	if result == nil {
		t.Fatal("Expected to find a record, but got nil")
	}
	if result.Key != "key999" {
		t.Errorf("Expected key999, got %s", result.Key)
	}
	// Tombstoned slice should now contain both keys
	if len(tombstoned) != 2 {
		t.Errorf("Expected 2 tombstoned keys, got %d", len(tombstoned))
	}
	// Check both keys are in tombstoned slice (order may vary)
	found456 := false
	found789 := false
	for _, key := range tombstoned {
		if key == "key456" {
			found456 = true
		}
		if key == "key789" {
			found789 = true
		}
	}
	if !found456 || !found789 {
		t.Errorf("Expected both key456 and key789 in tombstoned slice, got %v", tombstoned)
	}
}

func BenchmarkHashMap_ScanForPrefix(b *testing.B) {
	hm := NewHashMap(100000)

	// Setup data
	numRecords := 10000
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("user%06d", i)
		_ = hm.Put(makeRec(key, "value"))
	}

	// Add some non-matching records
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("admin%06d", i)
		_ = hm.Put(makeRec(key, "value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tombstoned := []string{}
		bestKeys := []string{}
		hm.ScanForPrefix("user", &tombstoned, &bestKeys, 50, 0)
	}
}
