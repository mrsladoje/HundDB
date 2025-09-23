// structures/hashmap/hashmap.go
package hashmap

import (
	"fmt"
	memtable "hunddb/lsm/memtable/memtable_interface"
	sstable "hunddb/lsm/sstable"
	model "hunddb/model/record"
	"math"
	"sort"
)

// Compile-time assertion that HashMap implements the Memtable interface.
var _ memtable.MemtableInterface = (*HashMap)(nil)

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

// Put inserts or updates the record for its key.
// Capacity applies only when inserting a NEW distinct key.
func (hm *HashMap) Put(record *model.Record) error {
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
	if err := hm.Put(record); err != nil {
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

// GetNextForPrefix returns the next record in lexicographical order after the given key,
// constrained to the given prefix, or nil if none exists.
// tombstonedKeys is used to track keys that have been tombstoned in more recent structures.
func (hm *HashMap) GetNextForPrefix(prefix string, key string, tombstonedKeys *[]string) *model.Record {

	// Get all keys and sort them
	keys := make([]string, 0, len(hm.data))
	for k := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Find the first key > afterKey that matches the prefix
	for _, k := range keys {
		// Skip keys that are <= afterKey
		if k <= key {
			continue
		}

		// Skip keys that don't match the prefix
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			// If key > prefix and doesn't match prefix, we've gone too far
			if k > prefix {
				break
			}
			continue
		}

		record := hm.data[k]
		if record == nil {
			continue
		}

		// Check if record is tombstoned locally
		if record.IsDeleted() {
			addToTombstoned(k, tombstonedKeys)
			continue
		}

		// Check if key is tombstoned in more recent structures
		if isKeyTombstoned(k, tombstonedKeys) {
			continue
		}

		// Found a valid, non-tombstoned record
		return record
	}

	return nil
}

// isKeyTombstoned checks if a key is in the tombstoned keys slice
func isKeyTombstoned(key string, tombstonedKeys *[]string) bool {
	if tombstonedKeys == nil {
		return false
	}
	for _, tombKey := range *tombstonedKeys {
		if tombKey == key {
			return true
		}
	}
	return false
}

// addToTombstoned adds a key to the tombstoned keys slice if it's not already there
func addToTombstoned(key string, tombstonedKeys *[]string) {
	if tombstonedKeys == nil {
		return
	}
	// Check if already exists to avoid duplicates
	for _, tombKey := range *tombstonedKeys {
		if tombKey == key {
			return
		}
	}
	*tombstonedKeys = append(*tombstonedKeys, key)
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

// RetrieveSortedRecords returns all records (including tombstones) in sorted key order.
// This is used for flushing the memtable to an SSTable.
func (hm *HashMap) RetrieveSortedRecords() []model.Record {
	if len(hm.data) == 0 {
		return []model.Record{}
	}

	// Extract keys and sort them
	keys := make([]string, 0, len(hm.data))
	for key := range hm.data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Build result slice in sorted order
	records := make([]model.Record, 0, len(hm.data))
	for _, key := range keys {
		rec := hm.data[key]
		if rec != nil {
			// Create a copy of the record to prevent external modification
			recordCopy := model.Record{
				Key:       rec.Key,
				Value:     make([]byte, len(rec.Value)),
				Timestamp: rec.Timestamp,
				Tombstone: rec.Tombstone,
			}
			copy(recordCopy.Value, rec.Value)
			records = append(records, recordCopy)
		}
	}

	return records
}

// Flush persists the memtable contents to disk (SSTable).
func (hm *HashMap) Flush(index int) error {

	sortedRecords := hm.RetrieveSortedRecords()

	err := sstable.PersistMemtable(sortedRecords, index)
	if err != nil {
		return fmt.Errorf("failed to flush HashMap memtable to SSTable: %v", err)
	}

	return nil
}
