// structures/hashmap/hashmap.go
package hashmap

import (
	"fmt"
	"math"

	"hunddb/model"
	mt "hunddb/structures/memtable"
)

// Compile-time assertion that HashMap implements the Memtable interface.
var _ mt.Memtable = (*HashMap)(nil)

// HashMap is a minimal Memtable implementation backed by a Go map.
// It stores the latest record per key, including tombstones.
type HashMap struct {
	data     map[string]*model.Record
	capacity int
}

// NewHashMap creates a new HashMap with the given capacity.
// If capacity <= 0, capacity is treated as unbounded.
func NewHashMap(capacity int) *HashMap {
	if capacity <= 0 {
		capacity = math.MaxInt
	}
	return &HashMap{
		data:     make(map[string]*model.Record),
		capacity: capacity,
	}
}

// Add inserts or updates the record for its key.
// Capacity applies only when inserting a NEW distinct key.
func (hm *HashMap) Add(record *model.Record) error {
	if record == nil || record.Key == "" {
		return fmt.Errorf("invalid record: record and key cannot be nil/empty")
	}

	if _, ok := hm.data[record.Key]; ok {
		// Update existing (allowed even when "full")
		hm.data[record.Key] = record
		return nil
	}

	// New distinct key → enforce capacity.
	if hm.IsFull() {
		return fmt.Errorf("memtable is full (capacity=%d)", hm.capacity)
	}
	hm.data[record.Key] = record
	return nil
}

func (hm *HashMap) Delete(record *model.Record) bool {
	if record == nil || record.Key == "" {
		return false
	}
	// Force tombstone
	record.Tombstone = true

	// Existing key: update in place
	if existing, ok := hm.data[record.Key]; ok {
		if existing != nil && !existing.Tombstone {
		}
		hm.data[record.Key] = record
		return true
	}

	// Missing key: delegate to Add (handles capacity + counters).
	if err := hm.Add(record); err != nil {
		return false // capacity exceeded → no blind tombstone
	}
	return false
}


// Get returns the latest non-tombstoned record by key, or nil if absent/tombstoned.
func (hm *HashMap) Get(key string) *model.Record {
	rec, ok := hm.data[key]
	if !ok || rec.IsDeleted() {
		return nil
	}
	return rec
}

// Size returns the number of active (non-tombstoned) keys.
// Minimal implementation: compute on the fly.
func (hm *HashMap) Size() int {
	n := 0
	for _, rec := range hm.data {
		if rec != nil && !rec.IsDeleted() {
			n++
		}
	}
	return n
}

// Capacity returns the maximum number of distinct keys (active + tombstoned) allowed.
func (hm *HashMap) Capacity() int {
	return hm.capacity
}

// TotalEntries returns the number of distinct keys currently stored (active + tombstoned).
func (hm *HashMap) TotalEntries() int {
	return len(hm.data)
}

// IsFull reports whether inserting a NEW distinct key would exceed capacity.
func (hm *HashMap) IsFull() bool {
	return len(hm.data) >= hm.capacity
}

// Flush persists the memtable contents to disk (implementation-specific).
// Minimal stub to satisfy the interface.
func (hm *HashMap) Flush() error {
	// TODO: Implement SSTable flush logic if/when needed.
	return nil
}
