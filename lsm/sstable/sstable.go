package sstable

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	block_manager "hunddb/lsm/block_manager"
	bloom_filter "hunddb/lsm/sstable/bloom_filter"
	merkle_tree "hunddb/lsm/sstable/merkle_tree"
	block_location "hunddb/model/block_location"
	record "hunddb/model/record"
	byte_util "hunddb/utils/byte_util"
	crc_util "hunddb/utils/crc"
	"io"
	"os"
)

/*
TODO:
1. Serialize and write to disk (DONE)
2. Get method (DONE)
3. Merkle validity check (DONE)
4. Thread safety
5. Compaction-merging

Guide: https://claude.ai/share/864c522e-b5fe-4e34-8ec9-04c7d6a4e9ee
*/

// TODO: Load from config
var (
	COMPRESSION_ENABLED = true
	BLOCK_SIZE          = 1024 * uint64(4) // 4KB
	USE_SEPARATE_FILES  = true
	SPARSE_STEP_INDEX   = 10 // Every 10th index goes into the summary
)

const (
	FILE_NAME_FORMAT          = "sstable_%d.db"
	DATA_FILE_NAME_FORMAT     = "sstable_%d_data.db"
	INDEX_FILE_NAME_FORMAT    = "sstable_%d_index.db"
	SUMMARY_FILE_NAME_FORMAT  = "sstable_%d_summary.db"
	FILTER_FILE_NAME_FORMAT   = "sstable_%d_filter.db"
	METADATA_FILE_NAME_FORMAT = "sstable_%d_metadata.db"

	CRC_SIZE = 4

	INDEX_ENTRY_METADATA_SIZE = 24
	INDEX_ENTRY_PART_SIZE     = 8

	STANDARD_FLAG_SIZE = 8

	BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01
)

// SSTable is an on-disk immutable key-value storage structure.
type SSTable struct {

	// Unique identifier of each SSTable.
	Index int

	// The level of the SSTable in the LSM Tree
	Level int

	// Configuration component.
	Config *SSTableConfig

	// Data component (actual key-value pairs storage)
	DataComp *DataComp

	// Index component (indexes for efficient data access)
	IndexComp *IndexComp

	// Summary component (speeds up index access)
	SummaryComp *SummaryComp

	// Filter component (Bloom filter to avoid unnecessary disk reads)
	FilterComp *FilterComp

	// Metadata component (Merkle tree for integrity verification)
	MetadataComp *MetadataComp
}

// Interface for all SSTable components.
type SSTableComponent interface {

	/*
	 Serialize should turn the component into a byte array, returns the size as well.

	 The size is the actual size of the component without the padding. (includes the CRCs though)
	*/
	serialize() ([]byte, uint64, error)
}

// SSTable configuration - mostly user defined settings.
// Implements SSTableComponent interface.
type SSTableConfig struct {

	/*
		For true, we store the SSTable in multiple files (file per component).

		For false, we store the SSTable in a single file.

		Chosen by user.
	*/
	UseSeparateFiles bool

	/*
		Base file will always persist SSTableConfig,
		and potentially the whole SSTable if UseSeparateFiles is false.

		The file would be named sstable_{index}.db, if we use seperate files for
		components, they would be sstable_{index}_filter.db and similar for others..
	*/

	/*
		What sstable_{index}.db looks like in memory (in case of UseSeparateFiles = true,
		only the config part is present).
		+---------+--------+-----------+------
		| Config  |  Data  | IndexComp | ...
		+---------+--------+-----------+------

		In case of a single file storage, we will add positional and size info to Config part,
		for each component, to allow for easier access.

		In case of seperate file storage, we will add a size flag at the beginning of the bytes
		of each serialized component, to help with deserialization (avoid the padding).
	*/

	/*
		True if we should use a global dictionary to compress string keys to numerical values.
		Chosen by user.
	*/
	CompressionEnabled bool

	/*
		Each sparseStepIndex-th index for the Index component goes into the Summary component.
		Chosen by user.
	*/
	SparseStepIndex int
}

// DataComp handles the actual key-value data storage.
type DataComp struct {

	/*
		FilePath will be sstable_{index}.db in case of false UseSeparateFiles in Config.

		Otherwise, it will be sstable_{index}_data.db
	*/
	FilePath string

	/*
		In case of false UseSeparateFiles in Config, we need to use this to access data,
		since everything is in a single file.

		Otherwise, it will be 0.
	*/
	StartOffset uint64

	/*
		Records are the actual key-value pairs stored in the SSTable.
		They are serialized and stored in the DataComp.
	*/
	Records []record.Record
}

// IndexEntry represents an entry in the IndexComp.
type IndexEntry struct {
	Key string
	/*
		Offset in DataComp where this record starts.
	*/
	Offset uint64
}

// Serialization format:
//
//	+--------------+------------------+       ...       --------------------------------+
//	| Offset (8B)  | Key Length (8B)  |  will be added    Offset (in Index itself) (8B) |
//	+--------------+------------------+       ...       --------------------------------+
//
// We will need to add the Offset of IndexEntry in IndexComp or SummaryComp itself to be able to
// access the key easily and perform binary search.
func (entry *IndexEntry) serialize() ([]byte, []byte, error) {
	// Calculate the total size needed
	keyBytes := []byte(entry.Key)
	keyLen := uint64(len(keyBytes))

	// Create buffer with the calculated size
	metadataBuf := make([]byte, 16)

	// Write offset as little-endian uint64
	binary.LittleEndian.PutUint64(metadataBuf[0:8], entry.Offset)
	// Write key length as little-endian uint64
	binary.LittleEndian.PutUint64(metadataBuf[8:16], keyLen)

	return metadataBuf, keyBytes, nil
}

// IndexComp provides indexes for efficient data access.
type IndexComp struct {

	/*
		FilePath will be sstable_{index}.db in case of false UseSeparateFiles in Config.

		Otherwise, it will be sstable_{index}_index.db
	*/
	FilePath string

	/*
		In case of false UseSeparateFiles in Config, we need to use this to access data,
		since everything is in a single file.

		Otherwise, it will be 0.
	*/
	StartOffset uint64

	/*
		IndexEntries are the actual indexes with offsets.
	*/
	IndexEntries []IndexEntry
}

// SummaryComp provides every SparseStepIndex-th index for easier access.
type SummaryComp struct {

	/*
		FilePath will be sstable_{index}.db in case of false UseSeparateFiles in Config.

		Otherwise, it will be sstable_{index}_summary.db
	*/
	FilePath string

	/*
		In case of false UseSeparateFiles in Config, we need to use this to access data,
		since everything is in a single file.

		Otherwise, it will be 0.
	*/
	StartOffset uint64

	/*
		MinKey and MaxKey are used to quickly determine if a key is in the SSTable.
		They are used for range queries and to avoid unnecessary disk reads.
	*/
	MinKey string

	/*
		MinKey and MaxKey are used to quickly determine if a key is in the SSTable.
		They are used for range queries and to avoid unnecessary disk reads.
	*/
	MaxKey string

	/*
		IndexEntries are the actual indexes with offsets.
	*/
	IndexEntries []IndexEntry
}

// FilterComp utilizes a BloomFilter for the SSTable.
type FilterComp struct {

	/*
		FilePath will be sstable_{index}.db in case of false UseSeparateFiles in Config.

		Otherwise, it will be sstable_{index}_filter.db
	*/
	FilePath string

	/*
		In case of false UseSeparateFiles in Config, we need to use this to access data,
		since everything is in a single file.

		Otherwise, it will be 0.
	*/
	StartOffset uint64

	/*
		BloomFilter for the SSTable to avoid searching for non-existent keys.
	*/
	BloomFilter *bloom_filter.BloomFilter
}

// MetadataComp utilizes a MerkleTree for the SSTable.
type MetadataComp struct {

	/*
		FilePath will be sstable_{index}.db in case of false UseSeparateFiles in Config.

		Otherwise, it will be sstable_{index}_metadata.db
	*/
	FilePath string

	/*
		In case of false UseSeparateFiles in Config, we need to use this to access data,
		since everything is in a single file.

		Otherwise, it will be 0.
	*/
	StartOffset uint64

	/*
		MerkleTree for the SSTable for integrity verification.
	*/
	MerkleTree *merkle_tree.MerkleTree
}

/*
PersistMemtable is used to save the memtable to disk.

We handle WHOLE SSTable parts in-memory, since it is not larger than
the Memtable we'd already kept in-memory. For compaction, bottlenecks may arise
due to increased size, so we work block by block there.
*/
func PersistMemtable(sortedRecords []record.Record, index int) error {

	blockManager := block_manager.GetBlockManager()

	// 1. Persist SSTableConfig
	SSTableConfig := &SSTableConfig{
		UseSeparateFiles:   USE_SEPARATE_FILES,
		CompressionEnabled: COMPRESSION_ENABLED,
		SparseStepIndex:    SPARSE_STEP_INDEX,
	}

	serializedConfig, configSize, err := SSTableConfig.serialize()
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedConfig, fmt.Sprintf(FILE_NAME_FORMAT, index), 0)
	if err != nil {
		return err
	}

	// 2. Persist DataComp (actual key-value pairs)
	dataStartOffset := configSize
	dataFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
	if USE_SEPARATE_FILES {
		dataStartOffset = 0
		dataFilePath = fmt.Sprintf(DATA_FILE_NAME_FORMAT, index)
	}
	dataComp := &DataComp{
		FilePath:    dataFilePath,
		StartOffset: dataStartOffset,
		Records:     sortedRecords,
	}

	serializedData, dataSize, err := dataComp.serialize()
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedData, dataComp.FilePath, dataComp.StartOffset)
	if err != nil {
		return err
	}

	// 3. Persist IndexComp
	indexStartOffset := dataStartOffset + uint64(len(serializedData))
	indexFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
	if USE_SEPARATE_FILES {
		indexStartOffset = 0
		indexFilePath = fmt.Sprintf(INDEX_FILE_NAME_FORMAT, index)
	}

	serializedRecords := make([][]byte, len(sortedRecords))
	for i, rec := range sortedRecords {
		serializedRecords[i] = rec.SerializeForSSTable(COMPRESSION_ENABLED)
	}

	indexComp := &IndexComp{
		FilePath:     indexFilePath,
		StartOffset:  indexStartOffset,
		IndexEntries: generateIndexEntries(sortedRecords, serializedRecords, dataStartOffset),
	}

	serializedIndex, indexSize, err := indexComp.serialize(indexStartOffset)
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedIndex, indexComp.FilePath, indexComp.StartOffset)
	if err != nil {
		return err
	}

	// 4. Persist SummaryComp
	summaryStartOffset := indexStartOffset + uint64(len(serializedIndex))
	summaryFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
	if USE_SEPARATE_FILES {
		summaryStartOffset = 0
		summaryFilePath = fmt.Sprintf(SUMMARY_FILE_NAME_FORMAT, index)
	}

	summaryComp := &SummaryComp{
		FilePath:     summaryFilePath,
		StartOffset:  summaryStartOffset,
		MinKey:       sortedRecords[0].Key,
		MaxKey:       sortedRecords[len(sortedRecords)-1].Key,
		IndexEntries: generateSummaryEntries(indexComp.IndexEntries),
	}

	serializedSummary, summarySize, err := summaryComp.serialize(summaryStartOffset)
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedSummary, summaryComp.FilePath, summaryComp.StartOffset)
	if err != nil {
		return err
	}

	// 5. Persist FilterComp (Bloom Filter)
	filterStartOffset := summaryStartOffset + uint64(len(serializedSummary))
	filterFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
	if USE_SEPARATE_FILES {
		filterFilePath = fmt.Sprintf(FILTER_FILE_NAME_FORMAT, index)
		filterStartOffset = 0
	}

	bloomFilter := bloom_filter.NewBloomFilter(len(sortedRecords), BLOOM_FILTER_FALSE_POSITIVE_RATE)
	for _, rec := range sortedRecords {
		bloomFilter.Add([]byte(rec.Key))
	}
	filterComp := &FilterComp{
		FilePath:    filterFilePath,
		StartOffset: filterStartOffset,
		BloomFilter: bloomFilter,
	}

	serializedFilter, filterSize, err := filterComp.serialize()
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedFilter, filterComp.FilePath, filterComp.StartOffset)
	if err != nil {
		return err
	}

	// 6. Persist MetadataComp (MerkleTree)
	metaDataStartOffset := filterStartOffset + uint64(len(serializedFilter))
	metaDataFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
	if USE_SEPARATE_FILES {
		metaDataStartOffset = 0
		metaDataFilePath = fmt.Sprintf(METADATA_FILE_NAME_FORMAT, index)
	}

	merkleTree, err := merkle_tree.NewMerkleTree(serializedRecords, false)
	if err != nil {
		return err
	}
	metadataComp := &MetadataComp{
		FilePath:    metaDataFilePath,
		StartOffset: metaDataStartOffset,
		MerkleTree:  merkleTree,
	}

	serializedMerkle, metadataSize, err := metadataComp.serialize()
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedMerkle, metadataComp.FilePath, metadataComp.StartOffset)
	if err != nil {
		return err
	}

	sizes := []uint64{dataSize, indexSize, summarySize, filterSize, metadataSize}
	offsets := []uint64{dataStartOffset, indexStartOffset, summaryStartOffset, filterStartOffset, metaDataStartOffset}
	err = SSTableConfig.addSizeDataToConfig(sizes, offsets, int(index))
	if err != nil {
		return err
	}

	return nil
}

/*
The serialized data of the SSTableConfig takes up only 1 block.

This is the pattern (without the CRC):

	+-----------------------+-------------------------+----------------------+
	| UseSeparateFiles (1B) | CompressionEnabled (1B) | SparseStepIndex (8B) |
	+-----------------------+-------------------------+----------------------+
*/
func (config *SSTableConfig) serialize() ([]byte, uint64, error) {

	data := make([]byte, BLOCK_SIZE)

	data[CRC_SIZE] = byte_util.BoolToByte(config.UseSeparateFiles)
	data[CRC_SIZE+1] = byte_util.BoolToByte(config.CompressionEnabled)
	binary.LittleEndian.PutUint64(data[CRC_SIZE+2:CRC_SIZE+10], uint64(config.SparseStepIndex))

	data = crc_util.AddCRCToBlockData(data)

	return data, BLOCK_SIZE, nil
}

func (data *DataComp) serialize() ([]byte, uint64, error) {

	serializedData := []byte{}
	for _, rec := range data.Records {
		serializedRecord := rec.SerializeForSSTable(COMPRESSION_ENABLED)
		recordSize := make([]byte, STANDARD_FLAG_SIZE)
		binary.LittleEndian.PutUint64(recordSize, uint64(len(serializedRecord)))
		serializedData = append(serializedData, recordSize...)
		serializedData = append(serializedData, serializedRecord...)
	}

	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedData)
	}

	finalBytes := crc_util.AddCRCsToData(serializedData)
	finalSizeBytes := uint64(len(serializedData))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
	crc_util.FixLastBlockCRC(finalBytes)

	return finalBytes, finalSizeBytes, nil
}

// generateIndexEntries creates index entries for the sorted records, accounting for
// block structure with CRC headers and data start offset for single/separate file modes.
func generateIndexEntries(sortedRecords []record.Record, serializedRecords [][]byte, dataStartOffset uint64) []IndexEntry {
	indexEntries := make([]IndexEntry, 0, len(sortedRecords))

	currentOffset := dataStartOffset
	accumulatedOffset := uint64(0)

	if USE_SEPARATE_FILES {
		currentOffset += STANDARD_FLAG_SIZE
	}

	currentOffset += CRC_SIZE

	for i, rec := range sortedRecords {
		// Calculate the offset considering CRC and block boundaries
		noOfBlocks := uint64(accumulatedOffset / (BLOCK_SIZE - CRC_SIZE))
		actualOffset := currentOffset + noOfBlocks*CRC_SIZE
		indexEntry := IndexEntry{
			Key:    rec.Key,
			Offset: actualOffset,
		}
		indexEntries = append(indexEntries, indexEntry)

		// Update currentOffset based on the serialized record size
		currentOffset += uint64(len(serializedRecords[i]) + STANDARD_FLAG_SIZE)
		accumulatedOffset += uint64(len(serializedRecords[i]) + STANDARD_FLAG_SIZE)
	}

	return indexEntries
}

/*
The serialization format:

	+------------------+------------------+
	| Metadata Section | Key Data Section |
	+------------------+------------------+

The Metadata section contains the offset information for each key in the Key Data section.
The offset is of course relative to the IndexComp.
The Metadata begins with an 8 byte offset for the start of the last entry, allowing us to utilize a bounds check.

This is a metadata entry:

	+--------------+------------------+-------------------------------+
	| Offset (8B)  | Key Length (8B)  | Offset (in Index itself) (8B) |
	+--------------+------------------+-------------------------------+
*/
func (index *IndexComp) serialize(indexStartOffset uint64) ([]byte, uint64, error) {
	metadataBytes := []byte{}
	keyDataBytes := []byte{}

	keyStartOffset := uint64(0)

	metadataSize := uint64(len(index.IndexEntries))*INDEX_ENTRY_METADATA_SIZE + INDEX_ENTRY_PART_SIZE

	if USE_SEPARATE_FILES {
		metadataSize += STANDARD_FLAG_SIZE
	}

	crcsTillLastEntry := ((metadataSize - INDEX_ENTRY_METADATA_SIZE) / (BLOCK_SIZE - CRC_SIZE)) + 1
	lastEntryOffset := indexStartOffset + metadataSize - INDEX_ENTRY_METADATA_SIZE + crcsTillLastEntry*CRC_SIZE

	metadataBytes = append(metadataBytes, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(metadataBytes[0:8], lastEntryOffset)

	for _, entry := range index.IndexEntries {
		metadataEntry, keyBytes, err := entry.serialize()
		if err != nil {
			return nil, 0, err
		}
		crcs := ((metadataSize + keyStartOffset) / (BLOCK_SIZE - CRC_SIZE)) + 1
		indexIndexOffset := indexStartOffset + metadataSize + keyStartOffset + crcs*CRC_SIZE
		metadataEntryWithIndexOffset := append(metadataEntry, make([]byte, 8)...)
		binary.LittleEndian.PutUint64(metadataEntryWithIndexOffset[16:24], indexIndexOffset)
		metadataBytes = append(metadataBytes, metadataEntryWithIndexOffset...)
		keyDataBytes = append(keyDataBytes, keyBytes...)

		keyStartOffset += uint64(len(keyBytes))
	}

	serializedData := append(metadataBytes, keyDataBytes...)
	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedData)
	}

	finalBytes := crc_util.AddCRCsToData(serializedData)
	finalSizeBytes := uint64(len(finalBytes))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
	crc_util.FixLastBlockCRC(finalBytes)

	return finalBytes, finalSizeBytes, nil
}

// generateSummaryEntries creates summary entries from the index entries.
func generateSummaryEntries(indexEntries []IndexEntry) []IndexEntry {
	summaryEntries := make([]IndexEntry, 0, len(indexEntries))

	for i, entry := range indexEntries {
		if i%SPARSE_STEP_INDEX == 0 {
			summaryEntries = append(summaryEntries, entry)
		}
	}

	return summaryEntries
}

/*
The serialization format:

	+------------------+------------------+
	| Metadata Section | Key Data Section |
	+------------------+------------------+

The Metadata section contains the offset information for each key in the Key Data section.
The offset is of course relative to the SummaryComp.
The Metadata begins with an 8 byte offset for the start of the last entry, allowing us to utilize binary search .

This is a metadata entry:

	+--------------+------------------+-------------------------------+
	| Offset (8B)  | Key Length (8B)  | Offset (in Index itself) (8B) |
	+--------------+------------------+-------------------------------+
*/
func (index *SummaryComp) serialize(summaryStartOffset uint64) ([]byte, uint64, error) {
	metadataBytes := []byte{}
	keyDataBytes := []byte{}

	keyStartOffset := uint64(0)

	metadataSize := uint64(len(index.IndexEntries))*INDEX_ENTRY_METADATA_SIZE + INDEX_ENTRY_PART_SIZE

	if USE_SEPARATE_FILES {
		metadataSize += STANDARD_FLAG_SIZE
	}

	crcsTillLastEntry := ((metadataSize - INDEX_ENTRY_METADATA_SIZE) / (BLOCK_SIZE - CRC_SIZE)) + 1
	lastEntryOffset := summaryStartOffset + metadataSize - INDEX_ENTRY_METADATA_SIZE + crcsTillLastEntry*CRC_SIZE

	metadataBytes = append(metadataBytes, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(metadataBytes[0:8], lastEntryOffset)

	for _, entry := range index.IndexEntries {
		metadataEntry, keyBytes, err := entry.serialize()
		if err != nil {
			return nil, 0, err
		}
		crcs := ((metadataSize + keyStartOffset) / (BLOCK_SIZE - CRC_SIZE)) + 1
		indexIndexOffset := summaryStartOffset + metadataSize + keyStartOffset + crcs*CRC_SIZE
		metadataEntryWithIndexOffset := append(metadataEntry, make([]byte, 8)...)
		binary.LittleEndian.PutUint64(metadataEntryWithIndexOffset[16:24], indexIndexOffset)
		metadataBytes = append(metadataBytes, metadataEntryWithIndexOffset...)
		keyDataBytes = append(keyDataBytes, keyBytes...)

		keyStartOffset += uint64(len(keyBytes))
	}

	serializedData := append(metadataBytes, keyDataBytes...)
	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedData)
	}

	finalBytes := crc_util.AddCRCsToData(serializedData)
	finalSizeBytes := uint64(len(finalBytes))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
	crc_util.FixLastBlockCRC(finalBytes)

	return finalBytes, finalSizeBytes, nil
}

func (filterComp *FilterComp) serialize() ([]byte, uint64, error) {

	serializedFilter := filterComp.BloomFilter.Serialize()
	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedFilter)
	}

	finalBytes := crc_util.AddCRCsToData(serializedFilter)
	finalSizeBytes := uint64(len(finalBytes))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
	crc_util.FixLastBlockCRC(finalBytes)

	return finalBytes, finalSizeBytes, nil
}

func (metaComp *MetadataComp) serialize() ([]byte, uint64, error) {

	serializedMerkle := metaComp.MerkleTree.Serialize()
	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedMerkle)
	}

	finalBytes := crc_util.AddCRCsToData(serializedMerkle)
	finalSizeBytes := uint64(len(serializedMerkle))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
	crc_util.FixLastBlockCRC(finalBytes)

	return finalBytes, finalSizeBytes, nil
}

/*
Used to read just the component size, placed at the beginning of the serialized bytes.
Component size is prepended in case of USE_SEPERATE_FILES = true, otherwise, we read the size
from the config.
*/
func getComponentSize(filepath string) (uint64, error) {
	blockManager := block_manager.GetBlockManager()

	blockData, err := blockManager.ReadBlock(block_location.BlockLocation{FilePath: filepath, BlockIndex: 0})
	if err != nil {
		return 0, err
	}

	err = crc_util.CheckBlockIntegrity(blockData)
	if err != nil {
		return 0, fmt.Errorf("failed to verify block integrity (in get component size): %v", err)
	}

	return binary.LittleEndian.Uint64(blockData[CRC_SIZE : CRC_SIZE+STANDARD_FLAG_SIZE]), nil
}

/*
Used to prepend just the component size flag at the beginning of the serialized bytes.
Component size is prepended in case of USE_SEPERATE_FILES = true, otherwise, we read the size
from the config.
*/
func prependSizePrefix(serializedData *[]byte) {
	if USE_SEPARATE_FILES {
		size_prefix := make([]byte, STANDARD_FLAG_SIZE)
		binary.LittleEndian.PutUint64(size_prefix[0:STANDARD_FLAG_SIZE], uint64(len(*serializedData)))
		*serializedData = append(size_prefix, *serializedData...)
	}
}

/*
Used to add the size and offsets data of components to the config, to allow for easier reading.
*/
func (config *SSTableConfig) addSizeDataToConfig(sizes []uint64, offsets []uint64, index int) error {

	if config.UseSeparateFiles {
		return nil
	}

	configBlock := make([]byte, BLOCK_SIZE)

	configBlock[CRC_SIZE] = byte_util.BoolToByte(config.UseSeparateFiles)
	configBlock[CRC_SIZE+1] = byte_util.BoolToByte(config.CompressionEnabled)
	binary.LittleEndian.PutUint64(configBlock[CRC_SIZE+2:CRC_SIZE+10], uint64(config.SparseStepIndex))

	currentOffset := uint64(CRC_SIZE + 1 + 1 + 8)
	for i := 0; i < len(sizes); i++ {
		binary.LittleEndian.PutUint64(configBlock[currentOffset:currentOffset+STANDARD_FLAG_SIZE], sizes[i])
		currentOffset += STANDARD_FLAG_SIZE
		binary.LittleEndian.PutUint64(configBlock[currentOffset:currentOffset+STANDARD_FLAG_SIZE], offsets[i])
		currentOffset += STANDARD_FLAG_SIZE
	}

	configBlock = crc_util.AddCRCToBlockData(configBlock)

	blockManager := block_manager.GetBlockManager()
	err := blockManager.WriteToDisk(configBlock, fmt.Sprintf(FILE_NAME_FORMAT, index), 0)
	if err != nil {
		return err
	}

	return nil
}

/*
Get retrieves a record by its key from the SSTable, if it exists in the SSTable,
while minimizing the number of disk accesses.
*/
func Get(key string, index int) (record *record.Record, err error) {

	// 0. Deserialize SSTable Config
	config, sizes, offsets, err := deserializeSSTableConfig(index)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize SSTable config: %v", err)
	}

	// 1. Bloom Filter Check
	if config.UseSeparateFiles {
		filterPath := fmt.Sprintf(FILTER_FILE_NAME_FORMAT, index)
		filterSize, err := getComponentSize(filterPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get filter component size: %v", err)
		}
		filter, err := deserializeFilter(filterPath, 0, filterSize, config.UseSeparateFiles)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize filter: %v", err)
		}
		if !filter.Contains([]byte(key)) {
			return nil, nil
		}
	} else {
		filterPath := fmt.Sprintf(FILE_NAME_FORMAT, index)
		filterOffset := offsets[3]
		filterSize := sizes[3]
		filter, err := deserializeFilter(filterPath, filterOffset, filterSize, config.UseSeparateFiles)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize filter (single file): %v", err)
		}
		if !filter.Contains([]byte(key)) {
			return nil, nil
		}
	}

	// 1.5. Data, Index and Summary preparation
	summaryPath := fmt.Sprintf(SUMMARY_FILE_NAME_FORMAT, index)
	summaryOffset := uint64(CRC_SIZE) + uint64(STANDARD_FLAG_SIZE)
	if !config.UseSeparateFiles {
		summaryPath = fmt.Sprintf(FILE_NAME_FORMAT, index)
		summaryOffset = offsets[2] + CRC_SIZE
	}
	indexFileOffset := uint64(CRC_SIZE) + uint64(STANDARD_FLAG_SIZE)
	indexPath := fmt.Sprintf(INDEX_FILE_NAME_FORMAT, index)
	if !config.UseSeparateFiles {
		indexFileOffset = offsets[1] + CRC_SIZE
		indexPath = fmt.Sprintf(FILE_NAME_FORMAT, index)
	}
	dataPath := fmt.Sprintf(DATA_FILE_NAME_FORMAT, index)
	if !config.UseSeparateFiles {
		dataPath = fmt.Sprintf(FILE_NAME_FORMAT, index)
	}

	// 2. Index Bounds Check
	inIndexBounds, oneOfBounds, offsetOfMatchingBound, lastSummaryEntryIndex, lastIndexEntryIndex, err := checkIndexBounds(indexPath, indexFileOffset, key, config.SparseStepIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check index bounds: %v", err)
	}
	if !inIndexBounds {
		return nil, nil
	}
	if oneOfBounds {
		record, err := retrieveFromDataComponent(dataPath, offsetOfMatchingBound, config.CompressionEnabled)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve record from data component (one of bounds): %v", err)
		}
		return record, nil
	}
	if !config.UseSeparateFiles {
		indexFileOffset += STANDARD_FLAG_SIZE
	}

	// 3. Summary Binary Search -> Index Binary Search
	offset, found, err := binarySearchSummary(summaryPath, key, summaryOffset+STANDARD_FLAG_SIZE, 0, lastSummaryEntryIndex, config.SparseStepIndex, indexFileOffset, config.UseSeparateFiles, index, lastIndexEntryIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to perform binary search on summary: %v", err)
	}
	if !found {
		return nil, nil
	} else {
		record, err := retrieveFromDataComponent(dataPath, offset, config.CompressionEnabled)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve record from data component (final): %v", err)
		}
		return record, nil
	}
}

/*
DeserializeSSTableConfig deserializes the SSTable configuration from the specified index.

Also returns the sizes and offsets of the various components, in case of single file mode.
*/
func deserializeSSTableConfig(index int) (*SSTableConfig, []uint64, []uint64, error) {
	blockManager := block_manager.GetBlockManager()
	location := block_location.BlockLocation{
		FilePath:   fmt.Sprintf(FILE_NAME_FORMAT, index),
		BlockIndex: 0,
	}
	blockData, err := blockManager.ReadBlock(location)
	if err != nil {
		return nil, nil, nil, err
	}

	err = crc_util.CheckBlockIntegrity(blockData)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to verify block integrity: %v", err)
	}

	useSeparateFiles := byte_util.ByteToBool(blockData[CRC_SIZE])
	compressionEnabled := byte_util.ByteToBool(blockData[CRC_SIZE+1])
	sparseStepIndex := int(binary.LittleEndian.Uint64(blockData[CRC_SIZE+2 : CRC_SIZE+10]))

	config := &SSTableConfig{
		UseSeparateFiles:   useSeparateFiles,
		CompressionEnabled: compressionEnabled,
		SparseStepIndex:    sparseStepIndex,
	}

	if !useSeparateFiles {

		const expectedPairs = 5
		sizes := make([]uint64, 0)
		offsets := make([]uint64, 0)

		blockData = blockData[CRC_SIZE:]
		offset := uint64(1 + 1 + 8)
		for i := 0; i < expectedPairs; i++ {
			size := binary.LittleEndian.Uint64(blockData[offset : offset+STANDARD_FLAG_SIZE])
			offset += STANDARD_FLAG_SIZE
			offsetValue := binary.LittleEndian.Uint64(blockData[offset : offset+STANDARD_FLAG_SIZE])
			offsets = append(offsets, offsetValue)
			sizes = append(sizes, size)
			offset += STANDARD_FLAG_SIZE
		}

		return config, sizes, offsets, nil
	}

	return config, nil, nil, nil
}

func deserializeFilter(filepath string, offset uint64, filterSize uint64, useSeparateFiles bool) (*bloom_filter.BloomFilter, error) {
	actualOffset := offset
	actualSize := filterSize

	if useSeparateFiles {
		actualOffset += STANDARD_FLAG_SIZE + CRC_SIZE
	}

	blockManager := block_manager.GetBlockManager()
	filterBytes, _, err := blockManager.ReadFromDisk(filepath, actualOffset, actualSize)
	if err != nil {
		return nil, err
	}

	filter := bloom_filter.Deserialize(filterBytes)
	return filter, nil
}

/*
Returns if the key is within the bounds of the Index Component.

If the key is in the bounds, the function returns true along with the index (ex. 645. or 7328.) of the last entry.

If the key happens to be one of the bounds, we return the offset in Data component of that bound's key.

If the key is not in the bounds, there is no point in searching further.
*/
func checkIndexBounds(filepath string, offset uint64, key string, sparseStep int) (bool, bool, uint64, uint64, uint64, error) {

	firstEntryKey, firstEntryDataOffset, err := readIndexMetadataEntry(filepath, offset+STANDARD_FLAG_SIZE)

	if err != nil {
		return false, false, 0, 0, 0, err
	}
	if key < firstEntryKey {
		return false, false, 0, 0, 0, nil
	}
	if key == firstEntryKey {
		return true, true, firstEntryDataOffset, 0, 0, nil
	}

	blockManager := block_manager.GetBlockManager()
	lastEntryOffsetBytes, _, err := blockManager.ReadFromDisk(filepath, offset, STANDARD_FLAG_SIZE)
	if err != nil {
		return false, false, 0, 0, 0, err
	}
	lastEntryOffset := binary.LittleEndian.Uint64(lastEntryOffsetBytes)

	lastEntryKey, lastEntryDataOffset, err := readIndexMetadataEntry(filepath, lastEntryOffset)
	if err != nil {
		return false, false, 0, 0, 0, err
	}
	if lastEntryKey == key {
		return true, true, lastEntryDataOffset, 0, 0, nil
	}

	physicalOffsetFirst := offset + STANDARD_FLAG_SIZE
	crcsFirst := (physicalOffsetFirst / BLOCK_SIZE) + 1
	logicalOffsetFirst := physicalOffsetFirst - crcsFirst*CRC_SIZE
	physicalOffsetLast := lastEntryOffset
	crcsLast := (physicalOffsetLast / BLOCK_SIZE) + 1
	logicalOffsetLast := physicalOffsetLast - crcsLast*CRC_SIZE

	indexOfLastIndexEntry := (logicalOffsetLast - logicalOffsetFirst) / INDEX_ENTRY_METADATA_SIZE
	indexOfLastSummaryEntry := indexOfLastIndexEntry / uint64(sparseStep)

	return lastEntryKey > key, false, 0, indexOfLastSummaryEntry, indexOfLastIndexEntry, nil
}

/*
Read an index metadata entry from the SSTable Summary Component.

Takes in the filepath, and offset to the entry start, returns the key, its offset in the data, and any error encountered.
*/
func readIndexMetadataEntry(filepath string, offset uint64) (string, uint64, error) {
	blockManager := block_manager.GetBlockManager()
	entryBytes, _, err := blockManager.ReadFromDisk(filepath, offset, INDEX_ENTRY_METADATA_SIZE)
	if err != nil {
		return "", 0, err
	}

	offsetInData := binary.LittleEndian.Uint64(entryBytes[0:STANDARD_FLAG_SIZE])
	keySize := binary.LittleEndian.Uint64(entryBytes[STANDARD_FLAG_SIZE : 2*STANDARD_FLAG_SIZE])
	offsetInIndex := binary.LittleEndian.Uint64(entryBytes[2*STANDARD_FLAG_SIZE : 3*STANDARD_FLAG_SIZE])

	keyBytes, _, err := blockManager.ReadFromDisk(filepath, offsetInIndex, keySize)
	if err != nil {
		return "", 0, err
	}
	key := string(keyBytes)

	return key, offsetInData, nil
}

/*
Binary search the summary index for the given key.
When we reach recursion base case, we do binary search of the index component.
*/
func binarySearchSummary(filepath string, key string, offsetFirst uint64, indexFirst uint64, indexLast uint64,
	sparseIndex int, indexFileOffset uint64, useSeperateFiles bool, index int, originalIndexLast uint64) (uint64, bool, error) {

	if indexFirst+1 >= indexLast {
		indexFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
		if useSeperateFiles {
			indexFilePath = fmt.Sprintf(INDEX_FILE_NAME_FORMAT, index)
			indexFileOffset += STANDARD_FLAG_SIZE
		}

		lastIndex := (indexLast + 1) * uint64(sparseIndex)
		if lastIndex > originalIndexLast {
			lastIndex = originalIndexLast - 1
		}
		startIndex := uint64(1)
		if indexFirst > 0 {
			startIndex = (indexFirst - 1) * uint64(sparseIndex)
		}

		offset, found, err := binarySearchIndexes(indexFilePath, key, indexFileOffset, startIndex, lastIndex)
		if err != nil {
			return 0, false, err
		}
		if found {
			return offset, true, nil
		}
	}

	physicalOffsetFirst := offsetFirst
	crcsTillFirst := (physicalOffsetFirst / BLOCK_SIZE) + 1
	logicalOffsetFirst := physicalOffsetFirst - crcsTillFirst*CRC_SIZE
	mid := indexFirst + (indexLast-indexFirst)/2
	logicalOffsetMid := logicalOffsetFirst + uint64(mid)*INDEX_ENTRY_METADATA_SIZE
	crcsTillMid := ((logicalOffsetMid / (BLOCK_SIZE - CRC_SIZE)) + 1)
	physicalOffsetMid := logicalOffsetMid + crcsTillMid*CRC_SIZE

	midKey, midOffset, err := readIndexMetadataEntry(filepath, physicalOffsetMid)
	if err != nil {
		return 0, false, err
	}

	if midKey == key {
		return midOffset, true, nil
	} else if midKey < key {
		return binarySearchSummary(filepath, key, offsetFirst, mid+1, indexLast, sparseIndex, indexFileOffset, useSeperateFiles, index, originalIndexLast)
	} else {
		return binarySearchSummary(filepath, key, offsetFirst, indexFirst, mid-1, sparseIndex, indexFileOffset, useSeperateFiles, index, originalIndexLast)
	}
}

/*
Binary search the index entries for the given key, between the specified indexes.
*/
func binarySearchIndexes(filepath string, key string, offsetFirst uint64, indexFirst uint64, indexLast uint64) (uint64, bool, error) {

	if indexFirst == indexLast {
		finalKey, finalOffset, err := readIndexMetadataEntry(filepath, offsetFirst+uint64(indexFirst)*INDEX_ENTRY_METADATA_SIZE)
		if err != nil {
			return 0, false, err
		}
		if finalKey == key {
			return finalOffset, true, nil
		}
		return 0, false, nil
	}

	physicalOffsetFirst := offsetFirst
	crcsTillFirst := (physicalOffsetFirst / BLOCK_SIZE) + 1
	logicalOffsetFirst := physicalOffsetFirst - crcsTillFirst*CRC_SIZE
	mid := indexFirst + (indexLast-indexFirst)/2
	logicalOffsetMid := logicalOffsetFirst + uint64(mid)*INDEX_ENTRY_METADATA_SIZE
	crcsTillMid := ((logicalOffsetMid / (BLOCK_SIZE - CRC_SIZE)) + 1)
	physicalOffsetMid := logicalOffsetMid + crcsTillMid*CRC_SIZE

	midKey, midOffset, err := readIndexMetadataEntry(filepath, physicalOffsetMid)
	if err != nil {
		return 0, false, err
	}

	if midKey == key {
		return midOffset, true, nil
	} else if midKey < key {
		return binarySearchIndexes(filepath, key, offsetFirst, mid+1, indexLast)
	} else {
		return binarySearchIndexes(filepath, key, offsetFirst, indexFirst, mid-1)
	}
}

/*
Retrieve a record from the data component of the SSTable.
*/
func retrieveFromDataComponent(filepath string, offset uint64, compressionEnabled bool) (*record.Record, error) {

	blockManager := block_manager.GetBlockManager()
	recordSize, _, err := blockManager.ReadFromDisk(filepath, offset, STANDARD_FLAG_SIZE)
	if err != nil {
		return nil, err
	}

	recordData, _, err := blockManager.ReadFromDisk(filepath, offset+STANDARD_FLAG_SIZE, binary.LittleEndian.Uint64(recordSize))
	if err != nil {
		return nil, err
	}

	record := record.DeserializeForSSTable(recordData, compressionEnabled)

	return record, nil
}

/*
CheckIntegrity checks the data integrity of the SSTable at the given index.

Returns a boolean indicating whether the integrity check passed, a list of corrupt data blocks,
if a fatal error occurred (fatal error doesn't allow us to continue with the check), and an error if one occurred.
*/
func CheckIntegrity(index int) (bool, []block_location.BlockLocation, bool, error) {

	corruptDataBlocks := make([]block_location.BlockLocation, 0)
	blockManager := block_manager.GetBlockManager()

	// 1. Deserialize SSTable Config
	config, sizes, offsets, err := deserializeSSTableConfig(index)
	if err != nil {
		corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
			FilePath:   fmt.Sprintf(FILE_NAME_FORMAT, index),
			BlockIndex: 0,
		})
		return false, corruptDataBlocks, true, fmt.Errorf("failed to deserialize SSTable config: %v", err)
	}

	// 2. Construct new Merkle tree
	dataPath := ""
	var dataOffset uint64
	var dataEndOffset uint64
	if config.UseSeparateFiles {
		dataPath = fmt.Sprintf(DATA_FILE_NAME_FORMAT, index)
		dataCompSizeBytes, dataStartOffset, err := blockManager.ReadFromDisk(dataPath, 0, uint64(STANDARD_FLAG_SIZE))
		corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
			FilePath:   dataPath,
			BlockIndex: 0,
		})
		if err != nil {
			return false, corruptDataBlocks, true, fmt.Errorf("failed to read data file size: %v", err)
		}
		dataOffset = dataStartOffset
		dataCompSize := binary.LittleEndian.Uint64(dataCompSizeBytes)
		dataEndOffset = crc_util.SizeAfterAddingCRCs(crc_util.SizeWithoutCRCs(dataOffset) + dataCompSize)
	}
	if !config.UseSeparateFiles {
		dataPath = fmt.Sprintf(FILE_NAME_FORMAT, index)
		dataOffset = offsets[0] + CRC_SIZE
		dataCompSize := sizes[0]
		dataEndOffset = crc_util.SizeAfterAddingCRCs(crc_util.SizeWithoutCRCs(dataOffset) + dataCompSize)
	}

	currentOffset := dataOffset
	recordHashes := make([][]byte, 0)
	hashToOffset := make(map[[md5.Size]byte]uint64)

	i := 1

	for currentOffset < dataEndOffset {
		var recordSizeBytes []byte

		recordSizeBytes, currentOffset, err = blockManager.ReadFromDisk(dataPath, currentOffset, STANDARD_FLAG_SIZE)
		if err != nil {
			corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
				FilePath:   dataPath,
				BlockIndex: currentOffset / BLOCK_SIZE,
			})
			if errors.Is(err, io.EOF) || os.IsNotExist(err) || os.IsPermission(err) {
				return false, corruptDataBlocks, true, fmt.Errorf("failed to read record size: %v", err)
			}
		}
		recordSize := binary.LittleEndian.Uint64(recordSizeBytes)

		var recordData []byte
		offsetBeforeRecord := currentOffset
		recordData, currentOffset, err = blockManager.ReadFromDisk(dataPath, currentOffset, recordSize)
		if err != nil {
			corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
				FilePath:   dataPath,
				BlockIndex: currentOffset / BLOCK_SIZE,
			})
			if errors.Is(err, io.EOF) || os.IsNotExist(err) || os.IsPermission(err) {
				return false, corruptDataBlocks, true, fmt.Errorf("failed to read record data: %v", err)
			}
		}
		recordHash := md5.Sum(recordData)
		recordHashes = append(recordHashes, recordHash[:])
		hashToOffset[recordHash] = offsetBeforeRecord
		i++
	}

	merkleTree, err := merkle_tree.NewMerkleTree(recordHashes, true)
	if err != nil {
		return false, corruptDataBlocks, true, fmt.Errorf("failed to create Merkle tree: %v", err)
	}

	// 3. Load Serialized Merkle Tree
	metadataPath := fmt.Sprintf(METADATA_FILE_NAME_FORMAT, index)
	metadataOffset := uint64(CRC_SIZE) + STANDARD_FLAG_SIZE
	var metaDatasize uint64
	if !config.UseSeparateFiles {
		metadataPath = fmt.Sprintf(FILE_NAME_FORMAT, index)
		metadataOffset = offsets[4]
		metaDatasize = sizes[4]
	} else {
		metaDatasize, err = getComponentSize(metadataPath)
		if err != nil {
			corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
				FilePath:   metadataPath,
				BlockIndex: 0,
			})
			return false, corruptDataBlocks, true, fmt.Errorf("failed to get metadata size: %v", err)
		}
	}

	merkleBytes, _, err := blockManager.ReadFromDisk(metadataPath, metadataOffset, metaDatasize)
	if err != nil {
		corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
			FilePath:   metadataPath,
			BlockIndex: metadataOffset / BLOCK_SIZE,
		})
		return false, corruptDataBlocks, true, fmt.Errorf("failed to read serialized Merkle tree: %v", err)
	}
	merkleTreeStored := merkle_tree.Deserialize(merkleBytes)

	// 4. Validate
	isValidData, mismatchedNodes, _ := merkleTree.Validate(merkleTreeStored)
	if !isValidData {
		for _, node := range mismatchedNodes {
			corruptDataBlocks = append(corruptDataBlocks, block_location.BlockLocation{
				FilePath:   dataPath,
				BlockIndex: hashToOffset[node.GetHash()] / BLOCK_SIZE,
			})
		}
		return false, corruptDataBlocks, false, nil
	}

	return true, []block_location.BlockLocation{}, false, nil
}
