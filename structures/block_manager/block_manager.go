package block_manager

import (
	"errors"
	block_location "hunddb/model/block_location"
	lru_cache "hunddb/structures/block_manager/lru_cache"
	"io"
	"os"
	"sync"
)

var (
	instance *BlockManager
	once     sync.Once
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
