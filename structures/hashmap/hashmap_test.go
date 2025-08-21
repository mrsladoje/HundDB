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
		Key:        key,
		Value:      []byte(val),
		Tombstone:  false,
		Timestamp:  uint64(time.Now().UnixNano()),
		Compressed: false,
	}
}

func makeTomb(key string) *model.Record {
	r := makeRec(key, "")
	r.Tombstone = true
	return r
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

	if err := hm.Add(makeRec("k1", "v1")); err != nil {
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

	if err := hm.Add(nil); err == nil {
		t.Fatalf("expected error for nil record")
	}
	if err := hm.Add(&model.Record{Key: ""}); err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestHashMap_UpdateExisting(t *testing.T) {
	hm := NewHashMap(math.MaxInt)

	_ = hm.Add(makeRec("k1", "v1"))
	_ = hm.Add(makeRec("k1", "v2")) // update

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
		_ = hm.Add(makeRec(k, "v_"+k))
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
	if err := hm.Add(makeRec("a", "1")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := hm.Add(makeRec("b", "2")); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !hm.IsFull() {
		t.Fatalf("expected IsFull=true")
	}

	// New distinct key should fail (capacity applies to NEW keys)
	if err := hm.Add(makeRec("c", "3")); err == nil {
		t.Fatalf("expected capacity error for new key when full")
	}

	// Update existing should still succeed when "full"
	if err := hm.Add(makeRec("b", "22")); err != nil {
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
		_ = hm.Add(makeRec(key, "v"))
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

func TestHashMap_FlushStub(t *testing.T) {
	hm := NewHashMap(4)
	if err := hm.Flush(); err != nil {
		t.Fatalf("Flush should be a no-op stub for now, got %v", err)
	}
}
