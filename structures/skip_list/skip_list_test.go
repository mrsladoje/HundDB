package skip_list

import (
	"testing"
	"time"

	model "hunddb/model/record"
)

// helper to create records with current timestamp
func rec(key string, val []byte, tomb bool) *model.Record {
	return model.NewRecord(key, val, uint64(time.Now().UnixNano()), tomb, false)
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

	if err := sl.Add(rec("key1", []byte("value1"), false)); err != nil {
		t.Fatalf("Add key1 failed: %v", err)
	}
	if err := sl.Add(rec("key2", []byte("value2"), false)); err != nil {
		t.Fatalf("Add key2 failed: %v", err)
	}
	if err := sl.Add(rec("key3", []byte("value3"), false)); err != nil {
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

	if err := sl.Add(rec("k", []byte("v1"), false)); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := sl.Add(rec("k", []byte("v2"), false)); err != nil {
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

	_ = sl.Add(rec("key1", []byte("v1"), false))
	_ = sl.Add(rec("key2", []byte("v2"), false))
	_ = sl.Add(rec("key3", []byte("v3"), false))

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

	_ = sl.Add(rec("key1", []byte("v1"), false))
	_ = sl.Add(rec("key2", []byte("v2"), false))

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

	if err := sl.Add(rec("a", []byte("1"), false)); err != nil {
		t.Fatalf("Add a failed: %v", err)
	}
	if err := sl.Add(rec("b", []byte("2"), false)); err != nil {
		t.Fatalf("Add b failed: %v", err)
	}

	// updating existing key is OK
	if err := sl.Add(rec("a", []byte("1b"), false)); err != nil {
		t.Fatalf("Update a failed: %v", err)
	}

	// inserting third distinct key should fail
	if err := sl.Add(rec("c", []byte("3"), false)); err == nil {
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

	_ = sl.Add(rec("k", []byte("v1"), false)) // active
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
	if err := sl.Add(rec("k", []byte("v2"), false)); err != nil {
		t.Fatalf("Re-add failed: %v", err)
	}
	if sl.Size() != 1 || sl.TotalEntries() != 1 {
		t.Fatalf("After re-add: Size=1, Total=1 expected; got %d, %d", sl.Size(), sl.TotalEntries())
	}
	if got := sl.Get("k"); got == nil || string(got.Value) != "v2" || got.Tombstone {
		t.Fatalf("Expected v2 active, got %+v", got)
	}
}
