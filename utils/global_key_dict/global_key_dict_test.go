package global_key_dict

import (
	"encoding/binary"
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
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE:CRC_SIZE+DICT_HEADER_SIZE], nextID)
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
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE:CRC_SIZE+DICT_HEADER_SIZE], nextID)
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
