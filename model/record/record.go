package model

import (
	"encoding/binary"
	byte_util "hunddb/utils/byte_util"
	global_key_dict "hunddb/utils/global_key_dict"
	"strconv"
)

// Record represents a key-value pair with metadata for the storage engine.
// It includes tombstone marking for deletion and timestamp for versioning.
type Record struct {
	Key       string // Key is the unique identifier for the record
	Value     []byte // Value contains the actual data associated with the key
	Tombstone bool   // Tombstone marks a record as deleted.
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

// MarkDeleted sets the tombstone flag to true, and removes the value.
func (r *Record) MarkDeleted() {
	r.Tombstone = true
	r.Value = nil
}

/*
Serialization format for Record:

- WAL and Uncompressed SSTable:
   +-------------------+---------------+---------------+-----------------+-...-+--...--+
   | Timestamp (8B)    | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +-------------------+---------------+---------------+-----------------+-...-+--...--+

   Timestamp = Timestamp of the operation in seconds
   Tombstone = If this record was deleted and has a value
   Key Size = Length of the Key data
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data

- Compressed SSTable:
   +-----------------+---------------+-------------+-----------------+--...--+
   | Timestamp (8B)  | Tombstone(1B) |  Index (8B) | Value Size (8B) | Value |
   +-----------------+---------------+-------------+-----------------+--...--+

   Index = Index in the global dictionary for the Key - A compressed numerical value instead of the string
   Value is OPTIONAL in the compressed format - if Tombstone is true, ValueSize and Value are not present.

*/

// Field size consts - used for serialization and deserialization
const (
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8
	// For SSTable compressed Records
	INDEX_SIZE = 8

	TIMESTAMP_START  = 0
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE

	// For SSTable compressed Records
	INDEX_START                 = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_COMPRESSED_START = INDEX_START + INDEX_SIZE
)

// Size returns the size of the serialized record in bytes. Used for WAL records.
func (r *Record) Size() int {
	return TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + len(r.Key) + len(r.Value)
}

// Size returns the size of the serialized record in bytes. Used for SSTable records. Varies if compressed or not and if tombstoned.
func (r *Record) SizeSSTable(compressed bool) int {
	if compressed {
		if r.Tombstone {
			return TIMESTAMP_SIZE + TOMBSTONE_SIZE + INDEX_SIZE
		}
		return TIMESTAMP_SIZE + TOMBSTONE_SIZE + INDEX_SIZE + VALUE_SIZE_SIZE + len(r.Value)
	}
	return r.Size()
}

// Serialize serializes a Record into a byte array. The byte array contains the following fields:
// - Timestamp: 8 bytes for the timestamp
// - Tombstone: 1 byte for the tombstone flag
// - KeySize: 8 bytes for the size of the key
// - ValueSize: 8 bytes for the size of the value
// - Key: variable length for the key data
// - Value: variable length for the value data
func (rec *Record) Serialize() []byte {
	data := make([]byte, rec.Size())

	binary.LittleEndian.PutUint64(data[TIMESTAMP_START:], rec.Timestamp)
	data[TOMBSTONE_START] = byte_util.BoolToByte(rec.Tombstone)
	keySize := uint64(len(rec.Key))
	binary.LittleEndian.PutUint64(data[KEY_SIZE_START:], keySize)
	binary.LittleEndian.PutUint64(data[VALUE_SIZE_START:], uint64(len(rec.Value)))
	copy(data[KEY_START:], rec.Key)
	copy(data[KEY_START+keySize:], rec.Value)

	return data
}

// SerializeForSSTable serializes a Record into a byte array for SSTable storage.
func (rec *Record) SerializeForSSTable(compressed bool) []byte {
	if compressed {
		return rec.serializeCompressed()
	}
	return rec.Serialize()
}

// serializeCompressed serializes a Record into a byte array with compression.
// The byte array contains the following fields:
// - Timestamp: 8 bytes for the timestamp
// - Tombstone: 1 byte for the tombstone flag
// - Index: 8 bytes for the index of the key in the global dictionary
// - ValueSize: 8 bytes for the size of the value (if not tombstoned)
// - Value: variable length for the value data (if not tombstoned)
func (rec *Record) serializeCompressed() []byte {
	data := make([]byte, rec.SizeSSTable(true))

	binary.LittleEndian.PutUint64(data[TIMESTAMP_START:], rec.Timestamp)
	data[TOMBSTONE_START] = byte_util.BoolToByte(rec.Tombstone)
	globalKeyDict := global_key_dict.GetGlobalKeyDict("global_key_dict.db")
	index, exists := globalKeyDict.GetEntryID(rec.Key)
	if !exists {
		indexNew, err := globalKeyDict.AddEntry(rec.Key)
		if err != nil {
			panic("Failed to add entry to global key dictionary: " + err.Error())
		}
		index = indexNew
	}
	binary.LittleEndian.PutUint64(data[INDEX_START:], index)

	if !rec.Tombstone {
		binary.LittleEndian.PutUint64(data[VALUE_SIZE_COMPRESSED_START:], uint64(len(rec.Value)))
		copy(data[VALUE_SIZE_COMPRESSED_START+VALUE_SIZE_SIZE:], rec.Value)
	}

	return data
}

// Deserialize takes a byte array and reconstructs its Record.
// It reads the data in the format defined by the Serialize function.
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

// DeserializeForSSTable takes a byte array and reconstructs its Record for SSTable.
func DeserializeForSSTable(data []byte, compressed bool) *Record {
	if compressed {
		return deserializeCompressed(data)
	}
	return Deserialize(data)
}

// DeserializeForSSTableCompressed takes a byte array and reconstructs its Record for compressed settings.
func deserializeCompressed(data []byte) *Record {
	timestamp := binary.LittleEndian.Uint64(data[TIMESTAMP_START:])
	tombstone := data[TOMBSTONE_START] != 0
	index := binary.LittleEndian.Uint64(data[INDEX_START:])
	globalKeyDict := global_key_dict.GetGlobalKeyDict("global_key_dict.db")
	key, exists := globalKeyDict.GetKey(index)
	if !exists {
		panic("Failed to find key in global key dictionary for index: " + strconv.FormatUint(index, 10))
	}
	valueSize := uint64(0)
	var value []byte
	if !tombstone {
		valueSize = binary.LittleEndian.Uint64(data[VALUE_SIZE_COMPRESSED_START:])
		value = data[VALUE_SIZE_COMPRESSED_START+VALUE_SIZE_SIZE : VALUE_SIZE_COMPRESSED_START+VALUE_SIZE_SIZE+valueSize]
	}
	return &Record{
		Timestamp: timestamp,
		Tombstone: tombstone,
		Key:       key,
		Value:     value,
	}
}
