package global_key_dict

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	mdl "hunddb/model"
	block_manager "hunddb/structures/block_manager"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestDebugLoadFromDisk(t *testing.T) {
	testFile := "debug_test_dict.db"
	defer os.Remove(testFile) // Clean up after test

	bm := block_manager.GetBlockManager()
	blockSize := int(bm.GetBlockSize())

	// Test data - simple single character keys
	testEntries := []struct {
		key string
		id  uint64
	}{
		{"a", 1},
		{"b", 2},
	}
	nextID := uint64(3) // Next available ID

	// Write header block
	headerBlock := make([]byte, blockSize)
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE:CRC_SIZE+DICT_NEXT_SIZE], nextID)
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE:CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE], 0)                             // lastBlockIndex
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE:CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE+DICT_LBO_SIZE], 0) // lastBlockOffset
	headerCRC := crc32.ChecksumIEEE(headerBlock[CRC_SIZE:])
	binary.LittleEndian.PutUint32(headerBlock[0:CRC_SIZE], headerCRC)

	headerLocation := mdl.BlockLocation{FilePath: testFile, BlockIndex: 0}
	err := bm.WriteBlock(headerLocation, headerBlock)
	if err != nil {
		t.Fatalf("Failed to write header block: %v", err)
	}

	// Write entries block
	entryBlock := make([]byte, blockSize)
	offset := CRC_SIZE

	for _, entry := range testEntries {
		// Check if we have enough space
		entrySize := ENTRY_ID_SIZE + ENTRY_KEY_LENGTH_SIZE + len(entry.key)
		if offset+entrySize > blockSize {
			t.Fatalf("Not enough space in block for entry. Need %d bytes, have %d",
				entrySize, blockSize-offset)
		}

		// Write ID
		binary.LittleEndian.PutUint64(entryBlock[offset:offset+ENTRY_ID_SIZE], entry.id)
		offset += ENTRY_ID_SIZE

		// Write key length
		keyLen := uint64(len(entry.key))
		binary.LittleEndian.PutUint64(entryBlock[offset:offset+ENTRY_KEY_LENGTH_SIZE], keyLen)
		offset += ENTRY_KEY_LENGTH_SIZE

		// Write key
		copy(entryBlock[offset:offset+len(entry.key)], []byte(entry.key))
		offset += len(entry.key)
	}

	// Calculate and write CRC for entry block
	entryCRC := crc32.ChecksumIEEE(entryBlock[CRC_SIZE:])
	binary.LittleEndian.PutUint32(entryBlock[0:CRC_SIZE], entryCRC)

	entryLocation := mdl.BlockLocation{FilePath: testFile, BlockIndex: 1}
	err = bm.WriteBlock(entryLocation, entryBlock)
	if err != nil {
		t.Fatalf("Failed to write entry block: %v", err)
	}

	// Reset the singleton for testing
	dictInstance = nil
	once = sync.Once{}

	// Load the dictionary
	dict := GetGlobalKeyDict(testFile)

	// Verify the results
	if dict.nextID != nextID {
		t.Errorf("Expected nextID %d, got %d", nextID, dict.nextID)
	}

	for _, expectedEntry := range testEntries {
		if actualID, exists := dict.keyToID[expectedEntry.key]; !exists {
			t.Errorf("Key '%s' not found in keyToID map", expectedEntry.key)
		} else if actualID != expectedEntry.id {
			t.Errorf("Expected ID %d for key '%s', got %d", expectedEntry.id, expectedEntry.key, actualID)
		}

		if actualKey, exists := dict.idToKey[expectedEntry.id]; !exists {
			t.Errorf("ID %d not found in idToKey map", expectedEntry.id)
		} else if actualKey != expectedEntry.key {
			t.Errorf("Expected key '%s' for ID %d, got '%s'", expectedEntry.key, expectedEntry.id, actualKey)
		}
	}
}

func TestLoadFromDiskWithOverflow(t *testing.T) {
	testFile := "test_overflow_dict.db"
	defer os.Remove(testFile)

	bm := block_manager.GetBlockManager()
	blockSize := int(bm.GetBlockSize())

	// Create a large key that will span multiple blocks
	largeKey := strings.Repeat("x", blockSize) // Key larger than one block

	testEntries := []struct {
		key string
		id  uint64
	}{
		{"small1", 1},
		{largeKey, 2},
		{"small2", 3},
	}
	nextID := uint64(4)

	// Write header block
	headerBlock := make([]byte, blockSize)
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE:CRC_SIZE+DICT_NEXT_SIZE], nextID)
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE:CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE], 0)                             // lastBlockIndex
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE:CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE+DICT_LBO_SIZE], 0) // lastBlockOffset
	headerCRC := crc32.ChecksumIEEE(headerBlock[CRC_SIZE:])
	binary.LittleEndian.PutUint32(headerBlock[0:CRC_SIZE], headerCRC)

	headerLocation := mdl.BlockLocation{FilePath: testFile, BlockIndex: 0}
	err := bm.WriteBlock(headerLocation, headerBlock)
	if err != nil {
		t.Fatalf("Failed to write header block: %v", err)
	}

	// Write entries across multiple blocks
	currentBlockIndex := 1
	currentBlock := make([]byte, blockSize)
	offset := CRC_SIZE

	for _, entry := range testEntries {
		entryHeaderSize := ENTRY_ID_SIZE + ENTRY_KEY_LENGTH_SIZE
		keyLen := len(entry.key)

		// Check if entry header fits in current block
		if offset+entryHeaderSize > blockSize {
			// Finish current block and start new one
			entryCRC := crc32.ChecksumIEEE(currentBlock[CRC_SIZE:])
			binary.LittleEndian.PutUint32(currentBlock[0:CRC_SIZE], entryCRC)

			location := mdl.BlockLocation{FilePath: testFile, BlockIndex: uint64(currentBlockIndex)}
			err := bm.WriteBlock(location, currentBlock)
			if err != nil {
				t.Fatalf("Failed to write block %d: %v", currentBlockIndex, err)
			}

			currentBlockIndex++
			currentBlock = make([]byte, blockSize)
			offset = CRC_SIZE
		}

		// Write entry header
		binary.LittleEndian.PutUint64(currentBlock[offset:offset+ENTRY_ID_SIZE], entry.id)
		offset += ENTRY_ID_SIZE

		binary.LittleEndian.PutUint64(currentBlock[offset:offset+ENTRY_KEY_LENGTH_SIZE], uint64(keyLen))
		offset += ENTRY_KEY_LENGTH_SIZE

		// Write key (might span multiple blocks)
		keyBytesRemaining := keyLen
		keyOffset := 0

		for keyBytesRemaining > 0 {
			spaceInBlock := blockSize - offset
			bytesToWrite := keyBytesRemaining
			if bytesToWrite > spaceInBlock {
				bytesToWrite = spaceInBlock
			}

			copy(currentBlock[offset:offset+bytesToWrite], []byte(entry.key)[keyOffset:keyOffset+bytesToWrite])
			offset += bytesToWrite
			keyOffset += bytesToWrite
			keyBytesRemaining -= bytesToWrite

			// If we filled the block and still have key bytes to write
			if keyBytesRemaining > 0 {
				// Finish current block
				entryCRC := crc32.ChecksumIEEE(currentBlock[CRC_SIZE:])
				binary.LittleEndian.PutUint32(currentBlock[0:CRC_SIZE], entryCRC)

				location := mdl.BlockLocation{FilePath: testFile, BlockIndex: uint64(currentBlockIndex)}
				err := bm.WriteBlock(location, currentBlock)
				if err != nil {
					t.Fatalf("Failed to write block %d: %v", currentBlockIndex, err)
				}

				currentBlockIndex++
				currentBlock = make([]byte, blockSize)
				offset = CRC_SIZE
			}
		}
	}

	// Write the final block
	entryCRC := crc32.ChecksumIEEE(currentBlock[CRC_SIZE:])
	binary.LittleEndian.PutUint32(currentBlock[0:CRC_SIZE], entryCRC)

	location := mdl.BlockLocation{FilePath: testFile, BlockIndex: uint64(currentBlockIndex)}
	err = bm.WriteBlock(location, currentBlock)
	if err != nil {
		t.Fatalf("Failed to write final block: %v", err)
	}

	// Reset the singleton for testing
	dictInstance = nil
	once = sync.Once{}

	// Test loading
	dict := GetGlobalKeyDict(testFile)

	// Verify the results
	if dict.nextID != nextID {
		t.Errorf("Expected nextID %d, got %d", nextID, dict.nextID)
	}

	if len(dict.keyToID) != len(testEntries) {
		t.Errorf("Expected %d entries, got %d", len(testEntries), len(dict.keyToID))
	}

	for _, expectedEntry := range testEntries {
		if actualID, exists := dict.keyToID[expectedEntry.key]; !exists {
			t.Errorf("Key not found (length %d)", len(expectedEntry.key))
		} else if actualID != expectedEntry.id {
			t.Errorf("Expected ID %d for key (length %d), got %d", expectedEntry.id, len(expectedEntry.key), actualID)
		}

		if actualKey, exists := dict.idToKey[expectedEntry.id]; !exists {
			t.Errorf("ID %d not found in idToKey map", expectedEntry.id)
		} else if actualKey != expectedEntry.key {
			t.Errorf("Key mismatch for ID %d: expected length %d, got length %d", expectedEntry.id, len(expectedEntry.key), len(actualKey))
		}
	}
}

// TestBasicOperations tests basic add, get operations
func TestBasicOperations(t *testing.T) {
	testFile := "test_basic_dict.db"
	defer os.Remove(testFile)

	// Reset singleton
	dictInstance = nil
	once = sync.Once{}

	dict := GetGlobalKeyDict(testFile)

	// Test adding entries
	testKeys := []string{"key1", "key2", "key3"}
	expectedIDs := []uint64{1, 2, 3}

	for i, key := range testKeys {
		id, err := dict.AddEntry(key)
		if err != nil {
			t.Fatalf("Failed to add key '%s': %v", key, err)
		}
		if id != expectedIDs[i] {
			t.Errorf("Expected ID %d for key '%s', got %d", expectedIDs[i], key, id)
		}
	}

	// Test getting entries by key
	for i, key := range testKeys {
		id, exists := dict.GetEntryID(key)
		if !exists {
			t.Errorf("Key '%s' should exist", key)
		}
		if id != expectedIDs[i] {
			t.Errorf("Expected ID %d for key '%s', got %d", expectedIDs[i], key, id)
		}
	}

	// Test getting entries by ID
	for i, key := range testKeys {
		retrievedKey, exists := dict.GetKey(expectedIDs[i])
		if !exists {
			t.Errorf("ID %d should exist", expectedIDs[i])
		}
		if retrievedKey != key {
			t.Errorf("Expected key '%s' for ID %d, got '%s'", key, expectedIDs[i], retrievedKey)
		}
	}

	// Test duplicate key
	_, err := dict.AddEntry("key1")
	if err == nil {
		t.Error("Expected error when adding duplicate key")
	}
}

// TestPersistenceAcrossRestarts tests that data persists after restart
func TestPersistenceAcrossRestarts(t *testing.T) {
	testFile := "test_persistence_dict.db"
	defer os.Remove(testFile)

	testKeys := []string{"persist1", "persist2", "persist3"}
	var originalIDs []uint64

	// First session: add data
	{
		dictInstance = nil
		once = sync.Once{}
		dict := GetGlobalKeyDict(testFile)

		for _, key := range testKeys {
			id, err := dict.AddEntry(key)
			if err != nil {
				t.Fatalf("Failed to add key '%s': %v", key, err)
			}
			originalIDs = append(originalIDs, id)
		}
	}

	// Second session: load and verify
	{
		dictInstance = nil
		once = sync.Once{}
		dict := GetGlobalKeyDict(testFile)

		for i, key := range testKeys {
			id, exists := dict.GetEntryID(key)
			if !exists {
				t.Errorf("Key '%s' should exist after restart", key)
			}
			if id != originalIDs[i] {
				t.Errorf("Expected ID %d for key '%s', got %d", originalIDs[i], key, id)
			}
		}
	}
}

// TestEmptyAndInvalidKeys tests edge cases with keys
func TestEmptyAndInvalidKeys(t *testing.T) {
	testFile := "test_invalid_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	// Test empty key
	_, err := dict.AddEntry("")
	if err != nil {
		t.Logf("Empty key rejected (this might be intended): %v", err)
	}

	// Test very long key
	longKey := strings.Repeat("a", 1000)
	id, err := dict.AddEntry(longKey)
	if err != nil {
		t.Fatalf("Failed to add long key: %v", err)
	}

	retrievedKey, exists := dict.GetKey(id)
	if !exists {
		t.Error("Long key should exist")
	}
	if retrievedKey != longKey {
		t.Error("Long key not retrieved correctly")
	}
}

// TestConcurrentAccess tests thread safety
func TestConcurrentAccess(t *testing.T) {
	testFile := "test_concurrent_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	const numGoroutines = 10
	const keysPerGoroutine = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*keysPerGoroutine)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < keysPerGoroutine; j++ {
				key := fmt.Sprintf("routine_%d_key_%d", routineID, j)
				_, err := dict.AddEntry(key)
				if err != nil {
					errors <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Errorf("Concurrent access error: %v", err)
	}

	// Verify all keys exist
	expectedCount := numGoroutines * keysPerGoroutine
	if len(dict.keyToID) != expectedCount {
		t.Errorf("Expected %d keys, got %d", expectedCount, len(dict.keyToID))
	}
}

// TestLargeDataSet tests with a large number of entries
func TestLargeDataSet(t *testing.T) {
	testFile := "test_large_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	const numEntries = 1000
	keys := make([]string, numEntries)

	// Generate test data
	for i := 0; i < numEntries; i++ {
		keys[i] = fmt.Sprintf("large_test_key_%05d", i)
	}

	// Add entries
	for _, key := range keys {
		_, err := dict.AddEntry(key)
		if err != nil {
			t.Fatalf("Failed to add key '%s': %v", key, err)
		}
	}

	// Verify all entries exist
	for _, key := range keys {
		_, exists := dict.GetEntryID(key)
		if !exists {
			t.Errorf("Key '%s' should exist", key)
		}
	}

	// Test persistence with large dataset
	dictInstance = nil
	once = sync.Once{}
	dict = GetGlobalKeyDict(testFile)

	for _, key := range keys {
		_, exists := dict.GetEntryID(key)
		if !exists {
			t.Errorf("Key '%s' should exist after restart", key)
		}
	}
}

// TestBlockBoundaryConditions tests entries that span block boundaries
func TestBlockBoundaryConditions(t *testing.T) {
	testFile := "test_boundary_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	bm := block_manager.GetBlockManager()
	blockSize := int(bm.GetBlockSize())

	// Create keys of various sizes around block boundaries
	testCases := []struct {
		name string
		key  string
	}{
		{"small", "small"},
		{"medium", strings.Repeat("m", blockSize/4)},
		{"large", strings.Repeat("l", blockSize-100)},
		{"exact_block", strings.Repeat("e", blockSize-CRC_SIZE-DICT_ENTRY_SIZE)},
		{"over_block", strings.Repeat("o", blockSize)},
		{"multi_block", strings.Repeat("x", blockSize*2+100)},
	}

	var ids []uint64
	for _, tc := range testCases {
		id, err := dict.AddEntry(tc.key)
		if err != nil {
			t.Fatalf("Failed to add %s key: %v", tc.name, err)
		}
		ids = append(ids, id)
	}

	// Verify all keys can be retrieved
	for i, tc := range testCases {
		retrievedKey, exists := dict.GetKey(ids[i])
		if !exists {
			t.Errorf("%s key should exist", tc.name)
		}
		if retrievedKey != tc.key {
			t.Errorf("%s key mismatch: expected length %d, got length %d",
				tc.name, len(tc.key), len(retrievedKey))
		}
	}

	// Test persistence
	dictInstance = nil
	once = sync.Once{}
	dict = GetGlobalKeyDict(testFile)

	for i, tc := range testCases {
		retrievedKey, exists := dict.GetKey(ids[i])
		if !exists {
			t.Errorf("%s key should exist after restart", tc.name)
		}
		if retrievedKey != tc.key {
			t.Errorf("%s key mismatch after restart: expected length %d, got length %d",
				tc.name, len(tc.key), len(retrievedKey))
		}
	}
}

// TestNonExistentKeys tests behavior with keys/IDs that don't exist
func TestNonExistentKeys(t *testing.T) {
	testFile := "test_nonexistent_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	// Test non-existent key
	_, exists := dict.GetEntryID("nonexistent")
	if exists {
		t.Error("Non-existent key should not exist")
	}

	// Test non-existent ID
	_, exists = dict.GetKey(999999)
	if exists {
		t.Error("Non-existent ID should not exist")
	}
}

// BenchmarkAddEntry benchmarks the AddEntry operation
func BenchmarkAddEntry(b *testing.B) {
	testFile := "bench_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		_, err := dict.AddEntry(key)
		if err != nil {
			b.Fatalf("Failed to add key: %v", err)
		}
	}
}

// BenchmarkGetEntryID benchmarks the GetEntryID operation
func BenchmarkGetEntryID(b *testing.B) {
	testFile := "bench_get_dict.db"
	defer os.Remove(testFile)

	dictInstance = nil
	once = sync.Once{}
	dict := GetGlobalKeyDict(testFile)

	// Pre-populate with some data
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("bench_get_key_%d", i)
		_, err := dict.AddEntry(keys[i])
		if err != nil {
			b.Fatalf("Failed to add key: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		_, _ = dict.GetEntryID(key)
	}
}
