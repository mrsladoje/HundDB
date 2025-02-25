package wal

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

// WALRecord represents a Write-Ahead Log record.
type WALRecord struct {
	CRC       uint32 // 4 bytes (computed over the payload)
	Timestamp uint64 // 8 bytes (Unix timestamp in seconds)
	Tombstone bool   // 1 byte (deleted flag)
	KeySize   uint64 // 8 bytes (size of key)
	ValueSize uint64 // 8 bytes (size of value)
	Key       []byte // Key data
	Value     []byte // Value data
}

// NewWALRecord creates a new WAL record.
func NewWALRecord(key, value []byte, tombstone bool) *WALRecord {
	return &WALRecord{
		Timestamp: uint64(time.Now().Unix()),
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
}

// Calculates CRC over a byte array.
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// Serialize serializes a WALRecord into a byte array. The byte array contains the following fields:
// - CRC: 4 bytes for the CRC value
// - Timestamp: 8 bytes for the timestamp
// - Tombstone: 1 byte for the tombstone flag
// - KeySize: 8 bytes for the size of the key
// - ValueSize: 8 bytes for the size of the value
// - Key: variable length for the key data
// - Value: variable length for the value data
func (rec *WALRecord) Serialize() []byte {
	totalSize := CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + rec.KeySize + rec.ValueSize
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

	if rec.CRC == 0 {
		rec.CRC = CRC32(data[TIMESTAMP_START:])
	}
	binary.LittleEndian.PutUint32(data[CRC_START:], rec.CRC)

	return data
}

// Deserialize takes a byte array and reconstructs a WALRecord from it.
// It reads the data in the format defined by the Serialize function.
func Deserialize(data []byte) *WALRecord {
	CRC := binary.LittleEndian.Uint32(data[CRC_START:])
	Timestamp := binary.LittleEndian.Uint64(data[TIMESTAMP_START:])
	Tombstone := false
	if data[TOMBSTONE_START] == 1 {
		Tombstone = true
	}
	KeySize := binary.LittleEndian.Uint64(data[KEY_SIZE_START:])
	ValueSize := binary.LittleEndian.Uint64(data[VALUE_SIZE_START:])
	Key := data[KEY_START : KEY_START+KeySize]
	Value := data[KEY_START+KeySize:]

	return &WALRecord{
		CRC:       CRC,
		Timestamp: Timestamp,
		Tombstone: Tombstone,
		KeySize:   KeySize,
		ValueSize: ValueSize,
		Key:       Key,
		Value:     Value,
	}
}
