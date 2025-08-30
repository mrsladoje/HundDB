package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	block_location "hunddb/model/block_location"
	record "hunddb/model/record"
	block_manager "hunddb/structures/block_manager"
	bloom_filter "hunddb/structures/bloom_filter"
	merkle_tree "hunddb/structures/merkle_tree"
	byte_util "hunddb/utils/byte_util"
	crc_util "hunddb/utils/crc"
)

/*
TODO:
1. Serialize and write to disk (DONE)
2. Get method
3. Compaction-merging
4. Merkle validity check

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

	// Serialize should turn the component into a byte array, returns the size as well
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
	err = writeToDisk(serializedConfig, fmt.Sprintf(FILE_NAME_FORMAT, index), 0)
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
	err = writeToDisk(serializedData, dataComp.FilePath, dataComp.StartOffset)
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

	serializedIndex, indexSize, err := indexComp.serialize()
	if err != nil {
		return err
	}
	err = writeToDisk(serializedIndex, indexComp.FilePath, indexComp.StartOffset)
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

	serializedSummary, summarySize, err := summaryComp.serialize()
	if err != nil {
		return err
	}
	err = writeToDisk(serializedSummary, summaryComp.FilePath, summaryComp.StartOffset)
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
	err = writeToDisk(serializedFilter, filterComp.FilePath, filterComp.StartOffset)
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

	merkleTree, err := merkle_tree.NewMerkleTree(serializedRecords)
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
	err = writeToDisk(serializedMerkle, metadataComp.FilePath, metadataComp.StartOffset)
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
		serializedData = append(serializedData, rec.SerializeForSSTable(COMPRESSION_ENABLED)...)
	}

	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedData)
	}
	finalBytes := crc_util.AddCRCsToData(serializedData)
	finalSizeBytes := uint64(len(finalBytes))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)

	return finalBytes, finalSizeBytes, nil
}

// generateIndexEntries creates index entries for the sorted records, accounting for
// block structure with CRC headers and data start offset for single/separate file modes.
func generateIndexEntries(sortedRecords []record.Record, serializedRecords [][]byte, dataStartOffset uint64) []IndexEntry {
	indexEntries := make([]IndexEntry, 0, len(sortedRecords))

	currentOffset := dataStartOffset
	accumulatedOffset := uint64(0)

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
		currentOffset += uint64(len(serializedRecords[i]))
		accumulatedOffset += uint64(len(serializedRecords[i]))
	}

	return indexEntries
}

/*
The serialization format:

	+------------------+------------------+
	| Metadata Section | Key Data Section |
	+------------------+------------------+

The Metadata section would contain the offset information for each key in the Key Data section.
The offset is of course relative to the IndexComp.

This is a metadata entry:

	+--------------+------------------+-------------------------------+
	| Offset (8B)  | Key Length (8B)  | Offset (in Index itself) (8B) |
	+--------------+------------------+-------------------------------+
*/
func (index *IndexComp) serialize() ([]byte, uint64, error) {
	metadataBytes := []byte{}
	keyDataBytes := []byte{}

	keyStartOffset := uint64(0)

	metadataSize := uint64(len(index.IndexEntries)) * INDEX_ENTRY_METADATA_SIZE
	metadataSize = (metadataSize / (BLOCK_SIZE - CRC_SIZE)) * BLOCK_SIZE

	for _, entry := range index.IndexEntries {
		metadataEntry, keyBytes, err := entry.serialize()
		if err != nil {
			return nil, 0, err
		}
		indexIndexOffset := metadataSize + (keyStartOffset/(BLOCK_SIZE-CRC_SIZE))*BLOCK_SIZE
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

The Metadata section would contain the offset information for each key in the Key Data section.
The offset is of course relative to the SummaryComp.
The Metadata begins with an 8 byte offset for the start of the last entry, allowing us to utilize binary search .

This is a metadata entry:

	+--------------+------------------+-------------------------------+
	| Offset (8B)  | Key Length (8B)  | Offset (in Index itself) (8B) |
	+--------------+------------------+-------------------------------+
*/
func (index *SummaryComp) serialize() ([]byte, uint64, error) {
	metadataBytes := []byte{}
	keyDataBytes := []byte{}

	keyStartOffset := uint64(0)

	metadataSize := uint64(len(index.IndexEntries))*INDEX_ENTRY_METADATA_SIZE + INDEX_ENTRY_PART_SIZE
	metadataSize = (metadataSize / (BLOCK_SIZE - CRC_SIZE)) * BLOCK_SIZE

	lastEntryOffset := metadataSize
	if lastEntryOffset/BLOCK_SIZE != (lastEntryOffset-1)/BLOCK_SIZE {
		lastEntryOffset -= INDEX_ENTRY_METADATA_SIZE + CRC_SIZE
	} else {
		lastEntryOffset -= INDEX_ENTRY_METADATA_SIZE
	}
	metadataBytes = append(metadataBytes, make([]byte, 8)...)
	binary.LittleEndian.PutUint64(metadataBytes[0:8], lastEntryOffset)

	for _, entry := range index.IndexEntries {
		metadataEntry, keyBytes, err := entry.serialize()
		if err != nil {
			return nil, 0, err
		}
		indexIndexOffset := metadataSize + (keyStartOffset/(BLOCK_SIZE-CRC_SIZE))*BLOCK_SIZE
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

	return finalBytes, finalSizeBytes, nil
}

func (metaComp *MetadataComp) serialize() ([]byte, uint64, error) {

	serializedMerkle := metaComp.MerkleTree.Serialize()
	if USE_SEPARATE_FILES {
		prependSizePrefix(&serializedMerkle)
	}

	finalBytes := crc_util.AddCRCsToData(serializedMerkle)
	finalSizeBytes := uint64(len(finalBytes))

	byte_util.AddPadding(&finalBytes, BLOCK_SIZE)

	return finalBytes, finalSizeBytes, nil
}

func writeToDisk(serializedData []byte, filePath string, startOffset uint64) error {
	blockManager := block_manager.GetBlockManager()

	currentLocation := block_location.BlockLocation{
		FilePath:   filePath,
		BlockIndex: startOffset / BLOCK_SIZE,
	}
	startBlockIndex := currentLocation.BlockIndex

	for i := uint64(0); i < uint64(len(serializedData))/BLOCK_SIZE; i++ {
		currentLocation.BlockIndex = startBlockIndex + i
		err := blockManager.WriteBlock(currentLocation, serializedData[i*BLOCK_SIZE:(i+1)*BLOCK_SIZE])
		if err != nil {
			return errors.New("failed to write block to disk")
		}
	}
	return nil
}

func readFromDisk(filePath string, startOffset uint64, size uint64) ([]byte, error) {
	blockManager := block_manager.GetBlockManager()

	currentLocation := block_location.BlockLocation{
		FilePath:   filePath,
		BlockIndex: startOffset / BLOCK_SIZE,
	}
	blockOffset := startOffset % BLOCK_SIZE
	if blockOffset < CRC_SIZE {
		blockOffset = CRC_SIZE
	}

	finalBytes := make([]byte, 0)

	for uint64(len(finalBytes)) < size {
		blockData, err := blockManager.ReadBlock(currentLocation)
		if err != nil {
			return nil, err
		}
		err = crc_util.CheckBlockIntegrity(blockData)
		if err != nil {
			return nil, err
		}
		remainingBytes := size - uint64(len(finalBytes))
		var bytesToRead uint64
		if remainingBytes < BLOCK_SIZE-CRC_SIZE-blockOffset {
			bytesToRead = remainingBytes
		} else {
			bytesToRead = BLOCK_SIZE - CRC_SIZE - blockOffset
		}

		finalBytes = append(finalBytes, blockData[blockOffset:bytesToRead+blockOffset]...)
		blockOffset = CRC_SIZE
		currentLocation.BlockIndex++
	}

	return finalBytes, nil
}

func getComponentSize(filepath string) (uint64, error) {
	blockManager := block_manager.GetBlockManager()

	blockData, err := blockManager.ReadBlock(block_location.BlockLocation{FilePath: filepath, BlockIndex: 0})
	if err != nil {
		return 0, err
	}

	err = crc_util.CheckBlockIntegrity(blockData)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(blockData[CRC_SIZE : CRC_SIZE+STANDARD_FLAG_SIZE]), nil
}

func prependSizePrefix(serializedData *[]byte) {
	if USE_SEPARATE_FILES {
		size_prefix := make([]byte, STANDARD_FLAG_SIZE)
		binary.LittleEndian.PutUint64(size_prefix[0:STANDARD_FLAG_SIZE], crc_util.SizeAfterAddingCRCs(uint64(len(*serializedData))))
		*serializedData = append(size_prefix, *serializedData...)
	}
}

func (config *SSTableConfig) addSizeDataToConfig(sizes []uint64, offsets []uint64, index int) error {
	configData, _, err := config.serialize()
	if err != nil {
		return err
	}
	configData = configData[CRC_SIZE:]
	if !config.UseSeparateFiles {
		offset := uint64(1 + 1 + 8)
		for i := 0; i < len(sizes); i++ {
			binary.LittleEndian.PutUint64(configData[offset:offset+STANDARD_FLAG_SIZE], sizes[i])
			offset += STANDARD_FLAG_SIZE
			binary.LittleEndian.PutUint64(configData[offset:offset+STANDARD_FLAG_SIZE], offsets[i])
			offset += STANDARD_FLAG_SIZE
		}
	}

	configData = crc_util.AddCRCToBlockData(configData)

	err = writeToDisk(configData, fmt.Sprintf(FILE_NAME_FORMAT, index), 0)
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
		return nil, err
	}

	// 1. Bloom Filter Check
	if config.UseSeparateFiles {
		filterPath := fmt.Sprintf(FILTER_FILE_NAME_FORMAT, index)
		filterSize, err := getComponentSize(filterPath)
		if err != nil {
			return nil, err
		}
		filter, err := deserializeFilter(filterPath, 0, filterSize, config.UseSeparateFiles)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		if !filter.Contains([]byte(key)) {
			return nil, nil
		}
	}

	// 1. Bloom Filter Check

	// 2. Summary Bounds Check

	// 3. Summary Binary Search

	// 4. Index Check

	// 5. Retrieval from Data

	return nil, nil
}

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
		return nil, nil, nil, err
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
		// Deserialize sizes and offsets
		sizes := make([]uint64, 0)
		offsets := make([]uint64, 0)

		blockData = blockData[CRC_SIZE:]
		offset := uint64(1 + 1 + 8)
		for offset < uint64(len(blockData)) {
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
	filterBytes, err := readFromDisk(filepath, offset, filterSize)
	if err != nil {
		return nil, err
	}

	if useSeparateFiles {
		filterBytes = filterBytes[STANDARD_FLAG_SIZE:]
	}

	filter := bloom_filter.Deserialize(filterBytes)

	return filter, nil
}
