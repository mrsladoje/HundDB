package wal

import (
	"encoding/binary"
	mdl "hunddb/model"
)

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
   Tombstone = If this payload was deleted and has a value
   Key Size = Length of the Key data
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data

   Note: CRC is handled at the fragment level in WALHeader, not here
*/

// WALPayload represents a Write-Ahead Log payload containing record data.
// CRC integrity checking is handled at the fragment level by WALHeader.
type WALPayload struct {
	Timestamp uint64 // 8 bytes (Unix timestamp in seconds)
	Tombstone bool   // 1 byte (deleted flag)
	KeySize   uint64 // 8 bytes (size of key)
	ValueSize uint64 // 8 bytes (size of value)
	Key       []byte // Key data
	Value     []byte // Value data
}

// NewWALPayload creates a WAL payload from a record.
func NewWALPayload(record *mdl.Record) *WALPayload {
	return &WALPayload{
		Timestamp: record.Timestamp,
		Tombstone: record.Tombstone,
		KeySize:   uint64(len(record.Key)),
		ValueSize: uint64(len(record.Value)),
		Key:       []byte(record.Key),
		Value:     record.Value,
	}
}

// Serialize serializes a WALPayload into a byte array. The byte array contains the following fields:
// - Timestamp: 8 bytes for the timestamp
// - Tombstone: 1 byte for the tombstone flag
// - KeySize: 8 bytes for the size of the key
// - ValueSize: 8 bytes for the size of the value
// - Key: variable length for the key data
// - Value: variable length for the value data
// Note: CRC is handled at the fragment level, not here
func (rec *WALPayload) Serialize() []byte {
	totalSize := TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + rec.KeySize + rec.ValueSize
	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint64(data[TIMESTAMP_START:], rec.Timestamp)
	if rec.Tombstone {
		data[TOMBSTONE_START] = 1
	} else {
		data[TOMBSTONE_START] = 0
	}
	binary.LittleEndian.PutUint64(data[KEY_SIZE_START:], rec.KeySize)
	binary.LittleEndian.PutUint64(data[VALUE_SIZE_START:], rec.ValueSize)
	copy(data[KEY_START:], rec.Key)
	copy(data[KEY_START+rec.KeySize:], rec.Value)

	return data
}

// Deserialize takes a byte array and reconstructs a WALPayload from it.
// It reads the data in the format defined by the Serialize function.
// Note: CRC validation is handled at the fragment level, not here
func Deserialize(data []byte) *WALPayload {
	timestamp := binary.LittleEndian.Uint64(data[TIMESTAMP_START:])
	tombstone := data[TOMBSTONE_START] == 1
	keySize := binary.LittleEndian.Uint64(data[KEY_SIZE_START:])
	valueSize := binary.LittleEndian.Uint64(data[VALUE_SIZE_START:])
	key := data[KEY_START : KEY_START+keySize]
	value := data[KEY_START+keySize : KEY_START+keySize+valueSize]

	return &WALPayload{
		Timestamp: timestamp,
		Tombstone: tombstone,
		KeySize:   keySize,
		ValueSize: valueSize,
		Key:       key,
		Value:     value,
	}
}
