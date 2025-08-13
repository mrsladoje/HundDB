package model

// Record represents a key-value pair with metadata for the storage engine.
// It includes tombstone marking for logical deletion and timestamp for versioning.
type Record struct {
	Key       string // Key is the unique identifier for the record
	Value     []byte // Value contains the actual data associated with the key
	Tombstone bool   // Tombstone indicates whether this record has been logically deleted.
	Timestamp uint64 // Timestamp represents when this record was created or last modified.
}

func NewRecord(key string, value []byte, timestamp uint64, tombstone bool) *Record {
	return &Record{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Tombstone: tombstone,
	}
}

// IsDeleted returns true if the record is marked as deleted (tombstoned).
func (r *Record) IsDeleted() bool {
	return r.Tombstone
}

// MarkDeleted sets the tombstone flag to true, marking the record as deleted.
func (r *Record) MarkDeleted() {
	r.Tombstone = true
	r.Value = []byte{0}
}

// Size returns the approximate size of the record in bytes.
func (r *Record) Size() int {
	return len(r.Key) + len(r.Value) + 1 + 8 // key + value + tombstone + timestamp
}
