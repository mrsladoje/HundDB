package block_manager

import (
	"errors"
	mdl "hunddb/model"
	"hunddb/structures/lru_cache"
	"io"
	"os"
)

var instance *BlockManager

// TODO: Figure out interaction with SSTable
// BlockManager manages disk I/O operations at block level
// Implements singleton pattern
type BlockManager struct {
	blockSize  uint16 // in bytes
	blockCache *lru_cache.LRUCache[mdl.BlockLocation, []byte]
}

// GetBlockManager returns the singleton instance
func GetBlockManager() *BlockManager {
	if instance == nil {
		// TODO: load from config the block size and cache size,
		// since config will be singleton no function params are needed
		// For now dummy values are present
		instance = &BlockManager{
			blockSize:  1024 * uint16(4),
			blockCache: lru_cache.NewLRUCache[mdl.BlockLocation, []byte](100),
		}
	}
	return instance
}

// ReadBlock reads a block from disk, using cache if available
func (bm *BlockManager) ReadBlock(location mdl.BlockLocation) ([]byte, error) {
	cachedBlock, err := bm.blockCache.Get(location)
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

// WriteBlock writes a block to disk and updates cache
func (bm *BlockManager) WriteBlock(location mdl.BlockLocation, data []byte) error {
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
func (bm *BlockManager) readBlockFromDisk(location mdl.BlockLocation) ([]byte, error) {
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
func (bm *BlockManager) writeBlockToDisk(location mdl.BlockLocation, data []byte) error {
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
