package model

import (
	"encoding/binary"
)

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

// Consts for serialization and deserialization
const (
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	TIMESTAMP_START  = 0
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

/*
   +-------------------+---------------+---------------+-----------------+-...-+--...--+
   | Timestamp (8B)   | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +-------------------+---------------+---------------+-----------------+-...-+--...--+
   Timestamp = Timestamp of the operation in seconds
   Tombstone = If this record was deleted and has a value
   Key Size = Length of the Key data
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data

   // TODO: CRC is handled at the fragment level in WALHeader for now, in future SSTable will also need fragmentation
*/

// Serialize serializes a Record into a byte array. The byte array contains the following fields:
// - Timestamp: 8 bytes for the timestamp
// - Tombstone: 1 byte for the tombstone flag
// - KeySize: 8 bytes for the size of the key
// - ValueSize: 8 bytes for the size of the value
// - Key: variable length for the key data
// - Value: variable length for the value data
// Note: CRC is handled at the fragment level, not here
func (rec *Record) Serialize() []byte {
	data := make([]byte, rec.Size())

	binary.LittleEndian.PutUint64(data[TIMESTAMP_START:], rec.Timestamp)
	if rec.Tombstone {
		data[TOMBSTONE_START] = 1
	} else {
		data[TOMBSTONE_START] = 0
	}
	key_size := uint64(len(rec.Key))
	binary.LittleEndian.PutUint64(data[KEY_SIZE_START:], key_size)
	binary.LittleEndian.PutUint64(data[VALUE_SIZE_START:], uint64(len(rec.Value)))
	copy(data[KEY_START:], rec.Key)
	copy(data[KEY_START+key_size:], rec.Value)

	return data
}

// Deserialize takes a byte array and reconstructs its Record.
// It reads the data in the format defined by the Serialize function.
// Note: CRC validation is handled at the fragment level, not here
func Deserialize(data []byte) *Record {
	timestamp := binary.LittleEndian.Uint64(data[TIMESTAMP_START:])
	tombstone := data[TOMBSTONE_START] != 0
	keySize := binary.LittleEndian.Uint64(data[KEY_SIZE_START:])
	valueSize := binary.LittleEndian.Uint64(data[VALUE_SIZE_START:])
	key := string(data[KEY_START : KEY_START+keySize])
	value := data[KEY_START+keySize : KEY_START+keySize+valueSize]

	return &Record{
		Timestamp: timestamp,
		Tombstone: tombstone,
		Key:       key,
		Value:     value,
	}
}
