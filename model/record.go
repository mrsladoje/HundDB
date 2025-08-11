package model

// Record represents a key-value pair with metadata for the storage engine.
// It includes tombstone marking for logical deletion and timestamp for versioning.
type Record struct {
	// Key is the unique identifier for the record
	Key []byte

	// Value contains the actual data associated with the key
	Value []byte

	// Tombstone indicates whether this record has been logically deleted.
	// When true, the record is marked for deletion but not physically removed
	// until compaction occurs.
	Tombstone bool

	// Timestamp represents when this record was created or last modified.
	Timestamp uint64
}

// IsDeleted returns true if the record is marked as deleted (tombstoned).
func (r *Record) IsDeleted() bool {
	return r.Tombstone
}

// IsActive returns true if the record is not marked as deleted.
func (r *Record) IsActive() bool {
	return !r.Tombstone
}

// MarkDeleted sets the tombstone flag to true, marking the record as deleted.
func (r *Record) MarkDeleted() {
	r.Tombstone = true
}

// Size returns the approximate size of the record in bytes.
func (r *Record) Size() int {
	return len(r.Key) + len(r.Value) + 1 + 8 // key + value + tombstone + timestamp
}
