package global_key_dict

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	mdl "hunddb/model"
	block_manager "hunddb/structures/block_manager"
	"sync"
)

// GlobalKeyDict is a singleton that manages a global dictionary for string keys
type GlobalKeyDict struct {

	// keyToID maps string keys to unique numerical IDs
	keyToID map[string]uint64

	// idToKey maps unique numerical IDs back to string keys
	idToKey map[uint64]string

	// nextID is the next available unique numerical ID
	nextID uint64

	// filePath is the path to the file where the dictionary is persisted
	filePath string

	// mutex protects concurrent access to the dictionary
	mutex sync.RWMutex

	// isDirty indicates if the dictionary has unsaved changes
	isDirty bool

	// blockManager is responsible for disk operations
	blockManager *block_manager.BlockManager
}

/*

Header block (first block):
	+----------+-------------+-----------+
	| CRC (4B) | NextID (8B) | Reserved  |
	+----------+-------------+-----------+

Entry blocks:
	+----------+----------+------------------+----------+----------+
	| CRC (4B) | ID (8B)  | KeyLength (8B)   | Key      | ID (8B)  | ...
	+----------+----------+------------------+----------+----------+

*/

var dictInstance *GlobalKeyDict
var once sync.Once

const (
	DICT_HEADER_SIZE      = 8  // Size of nextID field
	DICT_ENTRY_SIZE       = 16 // 8 bytes for ID + 8 bytes for key length
	ENTRY_ID_SIZE         = 8  // Size of ID field in entry
	ENTRY_KEY_LENGTH_SIZE = 8  // Size of key length field in entry
	CRC_SIZE              = 4  // Size of CRC field
)

// GetGlobalKeyDict returns the singleton instance of GlobalKeyDict, initializing it if necessary.
func GetGlobalKeyDict(filepath string) *GlobalKeyDict {
	once.Do(func() {
		dictInstance = &GlobalKeyDict{
			keyToID:      make(map[string]uint64),
			idToKey:      make(map[uint64]string),
			nextID:       1, // Start IDs from 1
			filePath:     filepath,
			isDirty:      false,
			blockManager: block_manager.GetBlockManager(),
		}
		dictInstance.loadFromDisk()
	})
	return dictInstance
}

// loadFromDisk loads the dictionary from the disk if it exists.
func (dict *GlobalKeyDict) loadFromDisk() error {
	headerLocation := mdl.BlockLocation{
		FilePath:   dict.filePath,
		BlockIndex: 0,
	}

	headerBlock, err := dict.blockManager.ReadBlock(headerLocation)
	if err != nil {
		return errors.New("failed to read header block: " + err.Error())
	}

	if err := dict.verifyBlockCRC(headerBlock); err != nil {
		return errors.New("header block CRC verification failed: " + err.Error())
	}

	dict.nextID = binary.LittleEndian.Uint64(headerBlock[CRC_SIZE : CRC_SIZE+DICT_HEADER_SIZE])

	if dict.nextID == 1 {
		return nil
	}

	err = dict.loadEntries(dict.nextID - 1)
	if err != nil {
		return errors.New("failed to load entries: " + err.Error())
	}

	return nil
}

/*
loadEntries loads entries from the disk into the dictionary.
We are careful about entries spanning multiple blocks.
*/
func (dict *GlobalKeyDict) loadEntries(expectedEntries uint64) error {
	entriesLeft := expectedEntries

	currentLocation := mdl.BlockLocation{
		FilePath:   dict.filePath,
		BlockIndex: 1,
	}

	keyBytes := make([]byte, 0)
	blockScanOffset := uint64(CRC_SIZE) // Start after CRC
	keyLength := uint64(0)
	entryID := uint64(0)
	key := ""
	block := make([]byte, 0)
	var err error

	for entriesLeft > 0 {

		// We read the block if we reach the end of the current block or if we read it for the first time
		if blockScanOffset >= uint64(len(block)) || len(block) == 0 {
			block, err = dict.blockManager.ReadBlock(currentLocation)
			if err != nil {
				return errors.New("failed to read entry block: " + err.Error())
			}
			if err := dict.verifyBlockCRC(block); err != nil {
				return errors.New("entry block CRC verification failed: " + err.Error())
			}
			currentLocation.BlockIndex++
			blockScanOffset = CRC_SIZE // Reset to start after CRC in new block
		}

		// If we're not in the middle of reading a key, read the entry header
		if keyLength == 0 {
			// Check if we have enough space for the entry header
			if blockScanOffset+DICT_ENTRY_SIZE > uint64(len(block)) {
				// Not enough space for header, move to next block
				blockScanOffset = uint64(len(block)) // Force reading next block
				continue
			}

			// Read entry ID and key length
			entryID = binary.LittleEndian.Uint64(block[blockScanOffset : blockScanOffset+ENTRY_ID_SIZE])
			blockScanOffset += ENTRY_ID_SIZE

			keyLength = binary.LittleEndian.Uint64(block[blockScanOffset : blockScanOffset+ENTRY_KEY_LENGTH_SIZE])
			blockScanOffset += ENTRY_KEY_LENGTH_SIZE
		}

		// Calculate how many key bytes we can read from this block
		remainingInBlock := uint64(len(block)) - blockScanOffset
		bytesToRead := keyLength
		if bytesToRead > remainingInBlock {
			bytesToRead = remainingInBlock
		}

		// Read the key bytes
		if bytesToRead > 0 {
			keyBytes = append(keyBytes, block[blockScanOffset:blockScanOffset+bytesToRead]...)
			keyLength -= bytesToRead
			blockScanOffset += bytesToRead
		}

		// If we've read the complete key, process it
		if keyLength == 0 {
			key = string(keyBytes)
			keyBytes = make([]byte, 0) // Reset for next entry
			dict.keyToID[key] = entryID
			dict.idToKey[entryID] = key
			entriesLeft--
		}
	}

	return nil
}

func (dict *GlobalKeyDict) verifyBlockCRC(data []byte) error {
	if len(data) < CRC_SIZE {
		return errors.New("data block too small to contain CRC")
	}

	crc := binary.LittleEndian.Uint32(data[0:CRC_SIZE])
	nonCRCData := data[CRC_SIZE:]

	calculatedCRC := crc32.ChecksumIEEE(nonCRCData)

	if crc != calculatedCRC {
		return errors.New("CRC mismatch in data block")
	}

	return nil
}
