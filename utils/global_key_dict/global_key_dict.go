package global_key_dict

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	mdl "hunddb/model"
	block_manager "hunddb/structures/block_manager"
	"sync"
)

/*
TODO: We should make some kind of a compaction mechanism for
the dictionary - it would rebuild the whole dict after compactions
of SSTables and remove any keys that are no longer present in the DB.
*/

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

	// lastBlockIndex is the index of the last block written to disk
	lastBlockIndex uint64

	// lastBlockOffset is the offset in the last block where the next entry will be written
	lastBlockOffset uint64

	// mutex ensures thread-safe access to the dictionary
	mutex sync.RWMutex

	// blockManager is responsible for disk operations
	blockManager *block_manager.BlockManager
}

/*

Header block (first block):
	+----------+-------------+---------------------+----------------------+-------------+
	| CRC (4B) | nextID (8B) | lastBlockIndex (8B) | lastBlockOffset (8B) | The Rest... |
	+----------+-------------+---------------------+----------------------+-------------+

Entry blocks:
	+----------+----------+------------------+----------+----------+
	| CRC (4B) | ID (8B)  | KeyLength (8B)   | Key      | ID (8B)  | ...
	+----------+----------+------------------+----------+----------+

*/

var dictInstance *GlobalKeyDict
var once sync.Once

const (
	DICT_NEXT_SIZE        = 8  // Size of nextID field
	DICT_LBI_SIZE         = 8  // Size of the lastBlockIndex
	DICT_LBO_SIZE         = 8  // Size of the lastBlockOffset
	DICT_ENTRY_SIZE       = 16 // 8 bytes for ID + 8 bytes for key length
	ENTRY_ID_SIZE         = 8  // Size of ID field in entry
	ENTRY_KEY_LENGTH_SIZE = 8  // Size of key length field in entry
	CRC_SIZE              = 4  // Size of CRC field
)

// GetGlobalKeyDict returns the singleton instance of GlobalKeyDict, initializing it if necessary.
func GetGlobalKeyDict(filepath string) *GlobalKeyDict {
	once.Do(func() {
		dictInstance = &GlobalKeyDict{
			keyToID:         make(map[string]uint64),
			idToKey:         make(map[uint64]string),
			nextID:          1,
			lastBlockIndex:  1,        // Start from 1 to reserve 0 for header block
			lastBlockOffset: CRC_SIZE, // Start after CRC in the first block
			filePath:        filepath,
			blockManager:    block_manager.GetBlockManager(),
		}
		dictInstance.loadFromDisk()
	})
	return dictInstance
}

// GetEntryID retrieves the ID for a given key.
func (dict *GlobalKeyDict) GetEntryID(key string) (uint64, bool) {
	dict.mutex.RLock()
	defer dict.mutex.RUnlock()

	id, exists := dict.keyToID[key]
	return id, exists
}

// GetKey retrieves the key for a given ID.
func (dict *GlobalKeyDict) GetKey(id uint64) (string, bool) {
	dict.mutex.RLock()
	defer dict.mutex.RUnlock()

	key, exists := dict.idToKey[id]
	return key, exists
}

// AddEntry adds a new key to the dictionary and returns its unique ID.
// It also persists the entry to disk.
func (dict *GlobalKeyDict) AddEntry(key string) (uint64, error) {
	dict.mutex.Lock()
	defer dict.mutex.Unlock()

	if _, exists := dict.keyToID[key]; exists {
		return 0, errors.New("key already exists in the dictionary")
	}

	id := dict.nextID

	// Persist the new entry to disk
	err := dict.persistEntry(id, key)
	if err != nil {
		return 0, err
	}

	// Update the in-memory maps
	dict.keyToID[key] = id
	dict.idToKey[id] = key

	return id, nil
}

// persistEntry writes the new entry to disk.
func (dict *GlobalKeyDict) persistEntry(id uint64, key string) error {
	// Prepare the entry data
	keyBytes := []byte(key)
	keyLength := uint64(len(keyBytes))

	lastBlockLocation := mdl.BlockLocation{
		FilePath:   dict.filePath,
		BlockIndex: dict.lastBlockIndex,
	}

	blockData, err := dict.blockManager.ReadBlock(lastBlockLocation)
	if err != nil {
		blockData = make([]byte, dict.blockManager.GetBlockSize())
	}

	// Check if we need a new block for the entry header
	if dict.lastBlockOffset+DICT_ENTRY_SIZE > uint64(dict.blockManager.GetBlockSize()) {
		if dict.lastBlockOffset > CRC_SIZE {
			blockData = dict.addCRCToData(blockData)
			err := dict.blockManager.WriteBlock(lastBlockLocation, blockData)
			if err != nil {
				return errors.New("failed to write block to disk: " + err.Error())
			}
		}

		dict.lastBlockIndex++
		dict.lastBlockOffset = CRC_SIZE
		blockData = make([]byte, dict.blockManager.GetBlockSize())
	}

	writing_header := true
	for keyLength > 0 {
		if writing_header {
			binary.LittleEndian.PutUint64(blockData[dict.lastBlockOffset:dict.lastBlockOffset+ENTRY_ID_SIZE], id)
			dict.lastBlockOffset += ENTRY_ID_SIZE
			binary.LittleEndian.PutUint64(blockData[dict.lastBlockOffset:dict.lastBlockOffset+ENTRY_KEY_LENGTH_SIZE], keyLength)
			dict.lastBlockOffset += ENTRY_KEY_LENGTH_SIZE
			writing_header = false
		}
		remainingSpace := uint64(dict.blockManager.GetBlockSize()) - dict.lastBlockOffset
		if keyLength > uint64(remainingSpace) {
			copy(blockData[dict.lastBlockOffset:], keyBytes[:remainingSpace])
			keyBytes = keyBytes[remainingSpace:]
			keyLength -= remainingSpace
			dict.lastBlockOffset = CRC_SIZE
			blockData = dict.addCRCToData(blockData)
			err := dict.blockManager.WriteBlock(mdl.BlockLocation{
				FilePath:   dict.filePath,
				BlockIndex: dict.lastBlockIndex,
			}, blockData)
			if err != nil {
				return errors.New("failed to write block to disk: " + err.Error())
			}
			dict.lastBlockIndex++
			blockData = make([]byte, dict.blockManager.GetBlockSize())
			continue
		}
		copy(blockData[dict.lastBlockOffset:], keyBytes)
		blockData = dict.addCRCToData(blockData)
		err = dict.blockManager.WriteBlock(mdl.BlockLocation{
			FilePath:   dict.filePath,
			BlockIndex: dict.lastBlockIndex,
		}, blockData)
		if err != nil {
			return errors.New("failed to write block to disk: " + err.Error())
		}
		dict.lastBlockOffset += keyLength
		keyLength = 0
	}

	dict.nextID++
	err = dict.resetHeaderBlock()
	if err != nil {
		return errors.New("failed to reset header block: " + err.Error())
	}

	return nil
}

// resetHeaderBlock resets the header block with the current state of the dictionary.
func (dict *GlobalKeyDict) resetHeaderBlock() error {
	headerBlock, err := dict.blockManager.ReadBlock(mdl.BlockLocation{
		FilePath:   dict.filePath,
		BlockIndex: 0,
	})
	if err != nil {
		return errors.New("failed to read header block: " + err.Error())
	}

	start_offset := CRC_SIZE
	binary.LittleEndian.PutUint64(headerBlock[start_offset:start_offset+DICT_NEXT_SIZE], dict.nextID)
	binary.LittleEndian.PutUint64(headerBlock[start_offset+DICT_NEXT_SIZE:start_offset+DICT_NEXT_SIZE+DICT_LBI_SIZE], dict.lastBlockIndex)
	binary.LittleEndian.PutUint64(headerBlock[start_offset+DICT_NEXT_SIZE+DICT_LBI_SIZE:start_offset+DICT_NEXT_SIZE+DICT_LBI_SIZE+DICT_LBO_SIZE], dict.lastBlockOffset)

	dict.addCRCToData(headerBlock)

	err = dict.blockManager.WriteBlock(mdl.BlockLocation{
		FilePath:   dict.filePath,
		BlockIndex: 0,
	}, headerBlock)
	if err != nil {
		return errors.New("failed to write header block: " + err.Error())
	}

	return nil
}

// loadFromDisk loads the dictionary from the disk if it exists.
func (dict *GlobalKeyDict) loadFromDisk() error {
	headerLocation := mdl.BlockLocation{
		FilePath:   dict.filePath,
		BlockIndex: 0,
	}

	headerBlock, err := dict.blockManager.ReadBlock(headerLocation)
	if err != nil {
		return dict.initializeNewFile()
	}

	if err := dict.verifyBlockCRC(headerBlock); err != nil {
		return errors.New("header block CRC verification failed: " + err.Error())
	}

	dict.nextID = binary.LittleEndian.Uint64(headerBlock[CRC_SIZE : CRC_SIZE+DICT_NEXT_SIZE])
	dict.lastBlockIndex = binary.LittleEndian.Uint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE : CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE])
	dict.lastBlockOffset = binary.LittleEndian.Uint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE : CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE+DICT_LBO_SIZE])

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

// initializeNewFile initializes a new GlobalKeyDict file with a header block and the first data block.
func (dict *GlobalKeyDict) initializeNewFile() error {
	// Create and write initial header block
	blockSize := dict.blockManager.GetBlockSize()
	headerBlock := make([]byte, blockSize)

	// Set initial values
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE:CRC_SIZE+DICT_NEXT_SIZE], 1)                                                                 // nextID = 1
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE:CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE], 1)                                    // lastBlockIndex = 1 (first data block)
	binary.LittleEndian.PutUint64(headerBlock[CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE:CRC_SIZE+DICT_NEXT_SIZE+DICT_LBI_SIZE+DICT_LBO_SIZE], CRC_SIZE) // lastBlockOffset = 4 (after CRC)

	// Add CRC
	crc := crc32.ChecksumIEEE(headerBlock[CRC_SIZE:])
	binary.LittleEndian.PutUint32(headerBlock[:CRC_SIZE], crc)

	// Write header block
	headerLocation := mdl.BlockLocation{FilePath: dict.filePath, BlockIndex: 0}
	err := dict.blockManager.WriteBlock(headerLocation, headerBlock)
	if err != nil {
		return err
	}

	// Create first data block
	dataBlock := make([]byte, blockSize)
	crc = crc32.ChecksumIEEE(dataBlock[CRC_SIZE:])
	binary.LittleEndian.PutUint32(dataBlock[:CRC_SIZE], crc)

	dataLocation := mdl.BlockLocation{FilePath: dict.filePath, BlockIndex: 1}
	err = dict.blockManager.WriteBlock(dataLocation, dataBlock)
	if err != nil {
		return err
	}

	// Update instance variables
	dict.lastBlockIndex = 1
	dict.lastBlockOffset = CRC_SIZE

	return nil
}

// verifyBlockCRC checks the CRC of a block to ensure data integrity.
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

// addCRCToData adds a CRC32 checksum to the beginning of the data.
func (dict *GlobalKeyDict) addCRCToData(data []byte) []byte {
	crc := crc32.ChecksumIEEE(data[CRC_SIZE:])
	binary.LittleEndian.PutUint32(data[:CRC_SIZE], crc)
	return data
}
