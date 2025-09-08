package block_manager

import (
	"errors"
	lru_cache "hunddb/lsm/lru_cache"
	block_location "hunddb/model/block_location"
	crc_util "hunddb/utils/crc"
	"io"
	"os"
	"sync"
)

var (
	instance *BlockManager
	once     sync.Once
)

// TODO: Load these from config
const (
	BLOCK_SIZE = 4096 // 4KB blocks
	CRC_SIZE   = 4    // 4 bytes for CRC32
)

// BlockManager manages disk I/O operations at block level
// Implements singleton pattern
type BlockManager struct {
	blockSize   uint16 // in bytes
	blockCache  *lru_cache.LRUCache[block_location.BlockLocation, []byte]
	fileMutexes sync.Map
}

// GetBlockManager returns the singleton instance
func GetBlockManager() *BlockManager {
	once.Do(func() {
		// TODO: load from config the block size and cache size,
		// since config will be singleton no function params are needed
		// For now dummy values are present
		instance = &BlockManager{
			blockSize:  1024 * uint16(4),
			blockCache: lru_cache.NewLRUCache[block_location.BlockLocation, []byte](100),
		}
	})
	return instance
}

// getFileMutex retrieves or creates a RWMutex for a given file path.
func (bm *BlockManager) getFileMutex(filePath string) *sync.RWMutex {
	if mutex, exists := bm.fileMutexes.Load(filePath); exists {
		return mutex.(*sync.RWMutex)
	}

	newMutex := &sync.RWMutex{}
	actual, _ := bm.fileMutexes.LoadOrStore(filePath, newMutex)
	return actual.(*sync.RWMutex)
}

// TODO: Call this method when an old SSTable is deleted
// RemoveFileMutex removes the mutex associated with a file path.
// This should be called when a file (e.g., an old SSTable) is deleted to prevent memory leaks.
func (bm *BlockManager) RemoveFileMutex(filePath string) {
	bm.fileMutexes.Delete(filePath)
}

// ReadBlock reads a block from disk, using cache if available.
func (bm *BlockManager) ReadBlock(location block_location.BlockLocation) ([]byte, error) {
	// Check cache first to avoid locking if the block is already in memory.
	cachedBlock, err := bm.blockCache.Get(location)
	if err == nil {
		return cachedBlock, nil
	}

	// Acquire a read-lock, allowing multiple concurrent reads from the same file.
	mutex := bm.getFileMutex(location.FilePath)
	mutex.RLock()
	defer mutex.RUnlock()

	// Double-check cache after acquiring the lock, as another goroutine might have
	// populated it while this one was waiting.
	cachedBlock, err = bm.blockCache.Get(location)
	if err == nil {
		return cachedBlock, nil
	}

	block, err := bm.readBlockFromDisk(location)
	if err != nil {
		return nil, errors.New("block not read successfully")
	}

	bm.blockCache.Put(location, block)
	return block, nil
}

// WriteBlock writes a block to disk and updates cache.
func (bm *BlockManager) WriteBlock(location block_location.BlockLocation, data []byte) error {
	// Acquire an exclusive write-lock to prevent any other reads or writes to the file.
	mutex := bm.getFileMutex(location.FilePath)
	mutex.Lock()
	defer mutex.Unlock()

	err := bm.writeBlockToDisk(location, data)
	if err != nil {
		return errors.New("block not written successfully")
	}
	bm.blockCache.Put(location, data)

	return nil
}

// GetBlockSize returns the current block size
func (bm *BlockManager) GetBlockSize() uint16 {
	return bm.blockSize
}

// Private helper method for ReadBlock
func (bm *BlockManager) readBlockFromDisk(location block_location.BlockLocation) ([]byte, error) {
	file, err := os.Open(location.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := int64(location.BlockIndex) * int64(bm.blockSize)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}

	data := make([]byte, int(bm.blockSize))
	_, err = file.Read(data)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return data, nil
}

// Private helper method for WriteBlock
func (bm *BlockManager) writeBlockToDisk(location block_location.BlockLocation, data []byte) error {
	file, err := os.OpenFile(location.FilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	offset := int64(location.BlockIndex) * int64(bm.blockSize)
	_, err = file.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
}

/*
Helper function to write serializedData to the disk (size irrelevant, block handling is internally managed).

	!!! Assumes each block of data has a valid CRC at the beginning. !!!
*/
func (blockManager *BlockManager) WriteToDisk(serializedData []byte, filePath string, startOffset uint64) error {

	currentLocation := block_location.BlockLocation{
		FilePath:   filePath,
		BlockIndex: startOffset / uint64(blockManager.blockSize),
	}
	startBlockIndex := currentLocation.BlockIndex

	for i := uint64(0); i < uint64(len(serializedData))/uint64(blockManager.blockSize); i++ {
		currentLocation.BlockIndex = startBlockIndex + i
		err := blockManager.WriteBlock(currentLocation, serializedData[i*uint64(blockManager.blockSize):(i+1)*uint64(blockManager.blockSize)])
		if err != nil {
			return errors.New("failed to write block to disk")
		}
	}
	return nil
}

/*
Helper function to read serializedData from the disk, using block manager.
Checks each block's integrity by checking the CRCs, returns the data without the CRCs.

	!!! Assumes each block of data has a valid CRC at the beginning. !!!
*/
func (blockManager *BlockManager) ReadFromDisk(filePath string, startOffset uint64, size uint64) ([]byte, uint64, error) {

	startBlockIndex := startOffset / uint64(blockManager.blockSize)
	blockOffset := startOffset % uint64(blockManager.blockSize)

	// If the offset is within the CRC area, move to after the CRC
	if blockOffset < CRC_SIZE {
		blockOffset = CRC_SIZE
	}

	finalBytes := make([]byte, 0, size) // Pre-allocate capacity
	currentBlockIndex := startBlockIndex
	remainingBytes := size

	for remainingBytes > 0 {
		// Read the current block
		currentLocation := block_location.BlockLocation{
			FilePath:   filePath,
			BlockIndex: currentBlockIndex,
		}

		blockData, err := blockManager.ReadBlock(currentLocation)
		if err != nil {
			return nil, 0, err
		}

		err = crc_util.CheckBlockIntegrity(blockData)
		if err != nil {
			return nil, 0, err
		}

		// Calculate how many bytes to read from this block
		availableInBlock := uint64(blockManager.blockSize) - blockOffset
		bytesToRead := remainingBytes
		if bytesToRead > availableInBlock {
			bytesToRead = availableInBlock
		}

		// Append the bytes from this block
		finalBytes = append(finalBytes, blockData[blockOffset:blockOffset+bytesToRead]...)

		remainingBytes -= bytesToRead
		currentBlockIndex++

		// For subsequent blocks, we start reading after the CRC
		blockOffset = CRC_SIZE
	}

	// Calculate the final offset (where reading ended)
	totalBytesRead := size
	finalPhysicalOffset := crc_util.SizeAfterAddingCRCs(crc_util.SizeWithoutCRCs(startOffset) + totalBytesRead)

	return finalBytes, finalPhysicalOffset, nil
}
