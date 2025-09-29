package independent_bloom_filter

import (
	"encoding/binary"
	"fmt"
	"sync"

	block_manager "hunddb/lsm/block_manager"
	"hunddb/lsm/sstable/bloom_filter"
	crc_util "hunddb/utils/crc"
)

// IndependentBloomFilter is a decorator around the existing BloomFilter
// that adds disk persistence capabilities while maintaining all original methods
type IndependentBloomFilter struct {
	*bloom_filter.BloomFilter // Embedded original BloomFilter
	mu                        sync.RWMutex // Mutex for thread-safe disk operations
}

// NewIndependentBloomFilter creates a new independent bloom filter
func NewIndependentBloomFilter(expectedElements int, falsePositiveRate float64) *IndependentBloomFilter {
	return &IndependentBloomFilter{
		BloomFilter: bloom_filter.NewBloomFilter(expectedElements, falsePositiveRate),
		mu:          sync.RWMutex{},
	}
}

// SaveToDisk saves the Independent Bloom Filter to disk with the given name
func (ibf *IndependentBloomFilter) SaveToDisk(name string) error {
	ibf.mu.Lock()
	defer ibf.mu.Unlock()
	
	filename := fmt.Sprintf("independent_bloom_filter_%s", name)
	
	// Serialize the BloomFilter data
	serializedData := ibf.BloomFilter.Serialize()
	
	// Create file data: [size (8B) + serialized data]
	totalSize := 8 + len(serializedData)
	fileData := make([]byte, totalSize)
	
	// Write size header
	binary.LittleEndian.PutUint64(fileData[0:8], uint64(len(serializedData)))
	
	// Copy serialized data
	copy(fileData[8:], serializedData)
	
	// Add CRC blocks and write to disk
	dataWithCRC := crc_util.AddCRCsToData(fileData)
	
	blockManager := block_manager.GetBlockManager()
	return blockManager.WriteToDisk(dataWithCRC, filename, 0)
}

// LoadIndependentBloomFilterFromDisk loads an Independent Bloom Filter from disk with the given name
func LoadIndependentBloomFilterFromDisk(name string) (*IndependentBloomFilter, error) {
	filename := fmt.Sprintf("independent_bloom_filter_%s", name)
	blockManager := block_manager.GetBlockManager()
	
	// Read size header (first 8 bytes)
	sizeBytes, _, err := blockManager.ReadFromDisk(filename, 0, 8)
	if err != nil {
		return nil, fmt.Errorf("file not found or corrupted: %v", err)
	}
	
	dataSize := binary.LittleEndian.Uint64(sizeBytes)
	
	// Read serialized data starting from offset 8 + CRC_SIZE
	// (BlockManager accounts for CRC internally)
	serializedData, _, err := blockManager.ReadFromDisk(filename, 8+4, dataSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %v", err)
	}
	
	// Deserialize the BloomFilter
	bloomFilter := bloom_filter.Deserialize(serializedData)
	
	// Return as IndependentBloomFilter
	return &IndependentBloomFilter{
		BloomFilter: bloomFilter,
		mu:          sync.RWMutex{},
	}, nil
}

// LoadFromDisk loads data into the current IndependentBloomFilter instance
func (ibf *IndependentBloomFilter) LoadFromDisk(name string) error {
	loaded, err := LoadIndependentBloomFilterFromDisk(name)
	if err != nil {
		return err
	}
	
	ibf.mu.Lock()
	defer ibf.mu.Unlock()
	
	// Replace the internal BloomFilter with loaded data
	ibf.BloomFilter = loaded.BloomFilter
	
	return nil
}

// Thread-safe Add method override
func (ibf *IndependentBloomFilter) Add(item []byte) {
	ibf.mu.Lock()
	defer ibf.mu.Unlock()
	ibf.BloomFilter.Add(item)
}

// Thread-safe Contains method override
func (ibf *IndependentBloomFilter) Contains(item []byte) bool {
	ibf.mu.RLock()
	defer ibf.mu.RUnlock()
	return ibf.BloomFilter.Contains(item)
}

// Thread-safe Serialize method override
func (ibf *IndependentBloomFilter) Serialize() []byte {
	ibf.mu.RLock()
	defer ibf.mu.RUnlock()
	return ibf.BloomFilter.Serialize()
}