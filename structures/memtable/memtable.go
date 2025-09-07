package memtable

import model "hunddb/model/record"

// Memtable is the in-memory KV interface used by the engine.
// It stores the latest value per key, including logical deletions via tombstones.
// Capacity is measured in distinct keys present (active + tombstoned).
type MemtableInterface interface {
	// Add inserts or updates the record for its key.
	// For a NEW key, return a non-nil error if the memtable is full.
	// Updates MUST succeed even when the memtable is full.
	Add(record *model.Record) error

	// Delete permors logi (implementations may force record.Tombstone = true).
	// If the key exists (active or already tombstoned), replace the record and return true.
	// If the key does not exist, insert a “blind tombstone” if capacity allows and return false.
	// If capacity is full, do nothing and return false.
	Delete(record *model.Record) bool

	// Get returns the latest non-tombstoned record by key, or nil if absent/tombstoned.
	Get(key string) *model.Record

	// Size returns the number of active (non-tombstoned) keys currently stored.
	Size() int

	// Capacity returns the maximum size allowed.
	Capacity() int

	// TotalEntries returns the number of distinct keys present (active + tombstoned).
	TotalEntries() int

	// IsFull reports whether inserting a NEW distinct key would exceed capacity.
	IsFull() bool

	// Flush persists the memtable contents to disk (e.g., write an SSTable).
	// Exact semantics are implementation-specific.
	Flush() error
}
