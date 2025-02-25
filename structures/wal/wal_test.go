package wal

import (
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"testing"
)

// TestNewWALRecord tests the creation of a new WALRecord.
func TestNewWALRecord(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	tombstone := false

	record := NewWALRecord(key, value, tombstone)

	// Verify the WALRecord fields
	if record.Timestamp == 0 {
		t.Errorf("Expected non-zero timestamp, got: %d", record.Timestamp)
	}

	if record.Tombstone != tombstone {
		t.Errorf("Expected Tombstone flag %v, got: %v", tombstone, record.Tombstone)
	}

	if !reflect.DeepEqual(record.Key, key) {
		t.Errorf("Expected Key %v, got: %v", key, record.Key)
	}

	if !reflect.DeepEqual(record.Value, value) {
		t.Errorf("Expected Value %v, got: %v", value, record.Value)
	}
}

// TestCRC32 tests the CRC32 checksum computation.
func TestCRC32(t *testing.T) {
	data := []byte("test-data")
	expectedCRC := crc32.ChecksumIEEE(data)

	computedCRC := CRC32(data)

	if computedCRC != expectedCRC {
		t.Errorf("Expected CRC32 %v, got: %v", expectedCRC, computedCRC)
	}
}

// TestSerialize tests the serialization of WALRecord.
func TestSerialize(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	tombstone := false

	record := NewWALRecord(key, value, tombstone)
	serialized := record.Serialize()

	// Calculate the expected length of the serialized data
	expectedSize := CRC_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + record.KeySize + record.ValueSize
	if uint64(len(serialized)) != expectedSize {
		t.Errorf("Expected serialized data size %v, got: %v", expectedSize, len(serialized))
	}

	// Verify CRC in serialized data
	expectedCRC := CRC32(serialized[TIMESTAMP_START:])
	serializedCRC := binary.LittleEndian.Uint32(serialized[CRC_START:])
	if expectedCRC != serializedCRC {
		t.Errorf("Expected CRC %v, got: %v", expectedCRC, serializedCRC)
	}
}

// TestDeserialize tests the deserialization of WALRecord.
func TestDeserialize(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")
	tombstone := false

	record := NewWALRecord(key, value, tombstone)
	serialized := record.Serialize()

	deserialized := Deserialize(serialized)

	// Compare the fields between the original record and the deserialized one
	if deserialized.Timestamp != record.Timestamp {
		t.Errorf("Expected Timestamp %v, got: %v", record.Timestamp, deserialized.Timestamp)
	}

	if deserialized.Tombstone != record.Tombstone {
		t.Errorf("Expected Tombstone %v, got: %v", record.Tombstone, deserialized.Tombstone)
	}

	if !reflect.DeepEqual(deserialized.Key, record.Key) {
		t.Errorf("Expected Key %v, got: %v", record.Key, deserialized.Key)
	}

	if !reflect.DeepEqual(deserialized.Value, record.Value) {
		t.Errorf("Expected Value %v, got: %v", record.Value, deserialized.Value)
	}

	if deserialized.CRC != record.CRC {
		t.Errorf("Expected CRC %v, got: %v", record.CRC, deserialized.CRC)
	}
}

// TestSerializeDeserializeRoundtrip tests round-trip serialization and deserialization.
func TestSerializeDeserializeRoundtrip(t *testing.T) {
	key := []byte("roundtrip-key")
	value := []byte("roundtrip-value")
	tombstone := true

	record := NewWALRecord(key, value, tombstone)
	serialized := record.Serialize()

	// Deserialize and re-serialize the data
	deserialized := Deserialize(serialized)
	roundtripSerialized := deserialized.Serialize()

	// Compare the original serialized data and round-trip serialized data
	if !reflect.DeepEqual(serialized, roundtripSerialized) {
		t.Errorf("Expected round-trip serialized data to be equal to original, got different")
	}
}

// TestEmptyKeyAndValue tests the creation and serialization of a WALRecord with empty key and value.
func TestEmptyKeyAndValue(t *testing.T) {
	key := []byte("")
	value := []byte("")
	tombstone := false

	record := NewWALRecord(key, value, tombstone)
	serialized := record.Serialize()

	// Check that key and value sizes are correctly serialized as 0
	if binary.LittleEndian.Uint64(serialized[KEY_SIZE_START:]) != 0 {
		t.Errorf("Expected KeySize 0, got: %v", binary.LittleEndian.Uint64(serialized[KEY_SIZE_START:]))
	}

	if binary.LittleEndian.Uint64(serialized[VALUE_SIZE_START:]) != 0 {
		t.Errorf("Expected ValueSize 0, got: %v", binary.LittleEndian.Uint64(serialized[VALUE_SIZE_START:]))
	}
}
