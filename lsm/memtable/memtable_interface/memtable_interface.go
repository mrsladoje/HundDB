package memtable_interface

import model "hunddb/model/record"

// MemtableInterface defines the operations for an in-memory memtable structure.
type MemtableInterface interface {

	// Put inserts or updates the record for its key.
	Put(record *model.Record) error

	// Delete performs logical deletion by inserting a tombstone record for the key.
	Delete(record *model.Record) bool

	// Get returns the latest non-tombstoned record by key, or nil if absent/tombstoned.
	Get(key string) *model.Record

	// GetNextForPrefix returns the next record in lexicographical order for the given key,
	// with the prefix as a constraint, or nil if none exists
	// tombstonedKeys is used to track keys that have been tombstoned in more recent structures.
	GetNextForPrefix(prefix string, key string, tombstonedKeys *[]string) *model.Record

	// GetNextForRange returns the next record in lexicographical order for the given key,
	// with the range [rangeStart, rangeEnd] as a constraint, or nil if none exists
	// tombstonedKeys is used to track keys that have been tombstoned in more recent structures.
	GetNextForRange(rangeStart string, rangeEnd string, key string, tombstonedKeys *[]string) *model.Record

	// Only keys are returned for memory efficiency - use Get() to retrieve full records.
	// Parameters:
	// - prefix: the key prefix to search for
	// - tombstonedKeys: keys that have been tombstoned in more recent structures
	// - bestKeys: best keys found so far from previous memtables (will be modified)
	// - pageSize: maximum number of results per page (typically <= 50)
	// - pageNumber: which page to return (0-based)
	ScanForPrefix(
		prefix string,
		tombstonedKeys *[]string,
		bestKeys *[]string,
		pageSize int,
		pageNumber int,
	)

	// Size returns the number of active (non-tombstoned) keys currently stored.
	Size() int

	// Capacity returns the maximum size allowed.
	Capacity() int

	// TotalEntries returns the number of distinct keys present (active + tombstoned).
	TotalEntries() int

	// IsFull reports whether inserting a NEW distinct key would exceed capacity.
	IsFull() bool

	// Flush persists the memtable contents to disk (SSTable).
	Flush(index int) error
}
