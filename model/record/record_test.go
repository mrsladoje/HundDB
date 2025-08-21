package model

import (
	"bytes"
	"testing"
	"time"
)

// Mock global key dictionary for testing
type MockGlobalKeyDict struct {
	keyToIndex map[string]uint64
	indexToKey map[uint64]string
	nextIndex  uint64
}

func NewMockGlobalKeyDict() *MockGlobalKeyDict {
	return &MockGlobalKeyDict{
		keyToIndex: make(map[string]uint64),
		indexToKey: make(map[uint64]string),
		nextIndex:  1,
	}
}

func (m *MockGlobalKeyDict) GetEntryID(key string) (uint64, bool) {
	index, exists := m.keyToIndex[key]
	return index, exists
}

func (m *MockGlobalKeyDict) AddEntry(key string) (uint64, error) {
	if index, exists := m.keyToIndex[key]; exists {
		return index, nil
	}

	index := m.nextIndex
	m.nextIndex++
	m.keyToIndex[key] = index
	m.indexToKey[index] = key
	return index, nil
}

func (m *MockGlobalKeyDict) GetKey(index uint64) (string, bool) {
	key, exists := m.indexToKey[index]
	return key, exists
}

// Note: In actual implementation, you'll need to mock the global_key_dict.GetGlobalKeyDict function
// This is a simplified version for testing purposes

func TestNewRecord(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		value      []byte
		timestamp  uint64
		tombstone  bool
		compressed bool
	}{
		{
			name:       "Normal record",
			key:        "test_key",
			value:      []byte("test_value"),
			timestamp:  uint64(time.Now().Unix()),
			tombstone:  false,
			compressed: false,
		},
		{
			name:       "Tombstone record",
			key:        "deleted_key",
			value:      nil,
			timestamp:  uint64(time.Now().Unix()),
			tombstone:  true,
			compressed: false,
		},
		{
			name:       "Compressed record",
			key:        "compressed_key",
			value:      []byte("compressed_value"),
			timestamp:  uint64(time.Now().Unix()),
			tombstone:  false,
			compressed: true,
		},
		{
			name:       "Empty key",
			key:        "",
			value:      []byte("value"),
			timestamp:  0,
			tombstone:  false,
			compressed: false,
		},
		{
			name:       "Empty value",
			key:        "key",
			value:      []byte{},
			timestamp:  1234567890,
			tombstone:  false,
			compressed: false,
		},
		{
			name:       "Large value",
			key:        "large_key",
			value:      make([]byte, 10000), // 10KB value
			timestamp:  uint64(time.Now().Unix()),
			tombstone:  false,
			compressed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := NewRecord(tt.key, tt.value, tt.timestamp, tt.tombstone, tt.compressed)

			if record.Key != tt.key {
				t.Errorf("Expected key %s, got %s", tt.key, record.Key)
			}
			if !bytes.Equal(record.Value, tt.value) {
				t.Errorf("Expected value %v, got %v", tt.value, record.Value)
			}
			if record.Timestamp != tt.timestamp {
				t.Errorf("Expected timestamp %d, got %d", tt.timestamp, record.Timestamp)
			}
			if record.Tombstone != tt.tombstone {
				t.Errorf("Expected tombstone %v, got %v", tt.tombstone, record.Tombstone)
			}
			if record.Compressed != tt.compressed {
				t.Errorf("Expected compressed %v, got %v", tt.compressed, record.Compressed)
			}
		})
	}
}

func TestIsDeleted(t *testing.T) {
	tests := []struct {
		name      string
		tombstone bool
		expected  bool
	}{
		{"Not deleted", false, false},
		{"Deleted", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &Record{Tombstone: tt.tombstone}
			if record.IsDeleted() != tt.expected {
				t.Errorf("Expected IsDeleted() to return %v, got %v", tt.expected, record.IsDeleted())
			}
		})
	}
}

func TestMarkDeleted(t *testing.T) {
	record := &Record{
		Key:       "test_key",
		Value:     []byte("test_value"),
		Tombstone: false,
	}

	record.MarkDeleted()

	if !record.Tombstone {
		t.Error("Expected tombstone to be true after MarkDeleted()")
	}
	if record.Value != nil {
		t.Error("Expected value to be nil after MarkDeleted()")
	}
}

func TestSize(t *testing.T) {
	tests := []struct {
		name     string
		record   *Record
		expected int
	}{
		{
			name: "Basic record",
			record: &Record{
				Key:   "test",
				Value: []byte("value"),
			},
			expected: TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + 4 + 5, // "test" = 4, "value" = 5
		},
		{
			name: "Empty key and value",
			record: &Record{
				Key:   "",
				Value: []byte{},
			},
			expected: TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + 0 + 0,
		},
		{
			name: "Nil value",
			record: &Record{
				Key:   "key",
				Value: nil,
			},
			expected: TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + 3 + 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.record.Size()
			if size != tt.expected {
				t.Errorf("Expected size %d, got %d", tt.expected, size)
			}
		})
	}
}

func TestSizeSSTable(t *testing.T) {
	tests := []struct {
		name     string
		record   *Record
		expected int
	}{
		{
			name: "Uncompressed record",
			record: &Record{
				Key:        "test",
				Value:      []byte("value"),
				Compressed: false,
			},
			expected: COMPRESSED_FLAG_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + 4 + 5,
		},
		{
			name: "Compressed record with value",
			record: &Record{
				Key:        "test",
				Value:      []byte("value"),
				Tombstone:  false,
				Compressed: true,
			},
			expected: COMPRESSED_FLAG_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + 5, // No key in compressed format
		},
		{
			name: "Compressed tombstone record",
			record: &Record{
				Key:        "test",
				Value:      nil,
				Tombstone:  true,
				Compressed: true,
			},
			expected: COMPRESSED_FLAG_SIZE + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE, // No value size or value for tombstone
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := tt.record.SizeSSTable()
			if size != tt.expected {
				t.Errorf("Expected SSTable size %d, got %d", tt.expected, size)
			}
		})
	}
}

func TestSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "Basic record",
			record: &Record{
				Key:       "test_key",
				Value:     []byte("test_value"),
				Timestamp: 1234567890,
				Tombstone: false,
			},
		},
		{
			name: "Tombstone record",
			record: &Record{
				Key:       "deleted_key",
				Value:     nil,
				Timestamp: 1234567890,
				Tombstone: true,
			},
		},
		{
			name: "Empty key and value",
			record: &Record{
				Key:       "",
				Value:     []byte{},
				Timestamp: 0,
				Tombstone: false,
			},
		},
		{
			name: "Unicode key",
			record: &Record{
				Key:       "测试键",
				Value:     []byte("test_value"),
				Timestamp: 1234567890,
				Tombstone: false,
			},
		},
		{
			name: "Binary value",
			record: &Record{
				Key:       "binary_key",
				Value:     []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
				Timestamp: 1234567890,
				Tombstone: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test WAL serialization
			serialized := tt.record.Serialize()
			deserialized := Deserialize(serialized)

			compareRecords(t, tt.record, deserialized)
		})
	}
}

func TestSerializeDeserializeSSTableUncompressed(t *testing.T) {
	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "Basic uncompressed record",
			record: &Record{
				Key:        "test_key",
				Value:      []byte("test_value"),
				Timestamp:  1234567890,
				Tombstone:  false,
				Compressed: false,
			},
		},
		{
			name: "Uncompressed tombstone record",
			record: &Record{
				Key:        "deleted_key",
				Value:      nil,
				Timestamp:  1234567890,
				Tombstone:  true,
				Compressed: false,
			},
		},
		{
			name: "Large uncompressed record",
			record: &Record{
				Key:        "large_key",
				Value:      make([]byte, 1000),
				Timestamp:  1234567890,
				Tombstone:  false,
				Compressed: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized := tt.record.SerializeForSSTableUncompressed()
			deserialized := DeserializeForSSTable(serialized)

			compareRecords(t, tt.record, deserialized)
		})
	}
}

// Note: This test would need the actual global key dictionary implementation
// For now, it's a placeholder showing the test structure
func TestSerializeDeserializeSSTableCompressed(t *testing.T) {
	t.Skip("Skipping compressed tests - requires global key dictionary mock implementation")

	// This is how the test would look with proper mocking:
	/*
		tests := []struct {
			name   string
			record *Record
		}{
			{
				name: "Basic compressed record",
				record: &Record{
					Key:        "test_key",
					Value:      []byte("test_value"),
					Timestamp:  1234567890,
					Tombstone:  false,
					Compressed: true,
				},
			},
			{
				name: "Compressed tombstone record",
				record: &Record{
					Key:        "deleted_key",
					Value:      nil,
					Timestamp:  1234567890,
					Tombstone:  true,
					Compressed: true,
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Setup mock global key dictionary
				// ...

				serialized := tt.record.SerializeForSSTableCompressed()
				deserialized := DeserializeForSSTable(serialized)

				compareRecords(t, tt.record, deserialized)
			})
		}
	*/
}

func TestDeserializeForSSTable(t *testing.T) {
	t.Run("Uncompressed flag detection", func(t *testing.T) {
		record := &Record{
			Key:        "test",
			Value:      []byte("value"),
			Timestamp:  1234567890,
			Tombstone:  false,
			Compressed: false,
		}

		serialized := record.SerializeForSSTableUncompressed()
		deserialized := DeserializeForSSTable(serialized)

		compareRecords(t, record, deserialized)
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("Very long key", func(t *testing.T) {
		longKey := string(make([]byte, 10000)) // 10KB key
		record := &Record{
			Key:       longKey,
			Value:     []byte("short_value"),
			Timestamp: 1234567890,
			Tombstone: false,
		}

		serialized := record.Serialize()
		deserialized := Deserialize(serialized)

		compareRecords(t, record, deserialized)
	})

	t.Run("Maximum timestamp", func(t *testing.T) {
		record := &Record{
			Key:       "test",
			Value:     []byte("value"),
			Timestamp: ^uint64(0), // Maximum uint64 value
			Tombstone: false,
		}

		serialized := record.Serialize()
		deserialized := Deserialize(serialized)

		compareRecords(t, record, deserialized)
	})

	t.Run("Tombstone with non-nil value", func(t *testing.T) {
		record := &Record{
			Key:       "test",
			Value:     []byte("should_be_ignored"),
			Timestamp: 1234567890,
			Tombstone: true,
		}

		record.MarkDeleted() // This should set Value to nil

		serialized := record.Serialize()
		deserialized := Deserialize(serialized)

		compareRecords(t, record, deserialized)
	})
}

func TestConsistencyBetweenSizeAndSerialization(t *testing.T) {
	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "Basic record",
			record: &Record{
				Key:        "test",
				Value:      []byte("value"),
				Timestamp:  1234567890,
				Tombstone:  false,
				Compressed: false,
			},
		},
		{
			name: "Large record",
			record: &Record{
				Key:        string(make([]byte, 1000)),
				Value:      make([]byte, 5000),
				Timestamp:  1234567890,
				Tombstone:  false,
				Compressed: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test WAL consistency
			expectedSize := tt.record.Size()
			actualSize := len(tt.record.Serialize())
			if expectedSize != actualSize {
				t.Errorf("WAL size mismatch: expected %d, got %d", expectedSize, actualSize)
			}

			// Test SSTable consistency
			expectedSSTableSize := tt.record.SizeSSTable()
			actualSSTableSize := len(tt.record.SerializeForSSTableUncompressed())
			if expectedSSTableSize != actualSSTableSize {
				t.Errorf("SSTable size mismatch: expected %d, got %d", expectedSSTableSize, actualSSTableSize)
			}
		})
	}
}

func BenchmarkSerialization(b *testing.B) {
	record := &Record{
		Key:       "benchmark_key_12345",
		Value:     make([]byte, 1024), // 1KB value
		Timestamp: 1234567890,
		Tombstone: false,
	}

	b.Run("WAL Serialize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = record.Serialize()
		}
	})

	b.Run("WAL Deserialize", func(b *testing.B) {
		serialized := record.Serialize()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = Deserialize(serialized)
		}
	})

	b.Run("SSTable Uncompressed Serialize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = record.SerializeForSSTableUncompressed()
		}
	})

	b.Run("SSTable Uncompressed Deserialize", func(b *testing.B) {
		serialized := record.SerializeForSSTableUncompressed()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = DeserializeForSSTable(serialized)
		}
	})
}

// Helper function to compare two records
func compareRecords(t *testing.T, expected, actual *Record) {
	t.Helper()

	if expected.Key != actual.Key {
		t.Errorf("Key mismatch: expected %s, got %s", expected.Key, actual.Key)
	}
	if !bytes.Equal(expected.Value, actual.Value) {
		t.Errorf("Value mismatch: expected %v, got %v", expected.Value, actual.Value)
	}
	if expected.Timestamp != actual.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", expected.Timestamp, actual.Timestamp)
	}
	if expected.Tombstone != actual.Tombstone {
		t.Errorf("Tombstone mismatch: expected %v, got %v", expected.Tombstone, actual.Tombstone)
	}
}

// Property-based test helper (you could extend this with a proper property testing library)
func TestSerializationRoundTrip(t *testing.T) {
	// Test with random data
	for i := 0; i < 100; i++ {
		keyLen := i % 100
		valueLen := i % 1000

		record := &Record{
			Key:       string(make([]byte, keyLen)),
			Value:     make([]byte, valueLen),
			Timestamp: uint64(i),
			Tombstone: i%2 == 0,
		}

		// Fill with pseudo-random data
		for j := range record.Key {
			record.Key = string(append([]byte(record.Key[:j]), byte(i+j)))
		}
		for j := range record.Value {
			record.Value[j] = byte(i + j)
		}

		// Test WAL round trip
		serialized := record.Serialize()
		deserialized := Deserialize(serialized)
		compareRecords(t, record, deserialized)

		// Test SSTable round trip
		record.Compressed = false
		ssTableSerialized := record.SerializeForSSTableUncompressed()
		ssTableDeserialized := DeserializeForSSTable(ssTableSerialized)
		compareRecords(t, record, ssTableDeserialized)
	}
}
