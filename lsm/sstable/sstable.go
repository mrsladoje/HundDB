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
	"hunddb/utils/config"
	crc_util "hunddb/utils/crc"
	string_util "hunddb/utils/string_util"
	"io"
	"os"
	"strings"
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

// Configuration variables loaded from config file
var (
	COMPRESSION_ENABLED bool   // Default values as fallback
	BLOCK_SIZE          uint64        
	USE_SEPARATE_FILES  bool 
	SPARSE_STEP_INDEX   uint64 // Every 10th index goes into the summary
)

// init loads SSTable configuration from config file
func init() {
	cfg := config.GetConfig()
	if cfg != nil {
		COMPRESSION_ENABLED = cfg.SSTable.CompressionEnabled
		BLOCK_SIZE = uint64(cfg.SSTable.BlockSize)
		USE_SEPARATE_FILES = cfg.SSTable.UseSeparateFiles
		SPARSE_STEP_INDEX = cfg.SSTable.SparseStepIndex
	}
}

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

// SSTableIterator represents an iterator over an SSTable's data component
type SSTableIterator struct {
	index              int
	filePath           string
	startOffset        uint64
	currentOffset      uint64
	recordIndex        uint64
	maxRecordIndex     uint64
	compressionEnabled bool
	hasNextRecord      bool
	currentRecord      *record.Record
}

// CompactionState tracks the state during compaction (memory-efficient)
type CompactionState struct {
	iterators         []*SSTableIterator
	totalNewRecords   uint64
	newDataOffset     uint64
	recordHashes      [][]byte     // Only store hashes for Merkle tree
	indexEntries      []IndexEntry // Track index entries as we go
	dataFilePath      string
	currentDataOffset uint64
	// totalLogical tracks total logical bytes written (record size flags + record payloads) since data start
	totalLogical uint64
	// dataPhysicalBase is the physical offset (file position) where the first record would start (after CRC and size prefix if any)
	dataPhysicalBase uint64
	// wroteSizePrefix indicates whether we've already emitted the size prefix (separate-files mode only)
	wroteSizePrefix bool
}

// initializeIterator creates and initializes an SSTable iterator
func initializeIterator(tableIndex int, config *SSTableConfig, sizes []uint64, offsets []uint64) (*SSTableIterator, error) {
	var dataPath string
	var dataOffset uint64

	if config.UseSeparateFiles {
		dataPath = fmt.Sprintf(DATA_FILE_NAME_FORMAT, tableIndex)
		dataOffset = CRC_SIZE + STANDARD_FLAG_SIZE
	} else {
		dataPath = fmt.Sprintf(FILE_NAME_FORMAT, tableIndex)
		dataOffset = offsets[0] + CRC_SIZE
	}

	// Get max record index using checkIndexBounds
	var indexPath string
	var indexFileOffset uint64
	if config.UseSeparateFiles {
		indexPath = fmt.Sprintf(INDEX_FILE_NAME_FORMAT, tableIndex)
		indexFileOffset = CRC_SIZE + STANDARD_FLAG_SIZE
	} else {
		indexPath = fmt.Sprintf(FILE_NAME_FORMAT, tableIndex)
		indexFileOffset = offsets[1] + CRC_SIZE
	}

	// Use a very high sentinel key to avoid early-out and still compute last index entry
	// We only need indexOfLastIndexEntry from the return values here.
	highKey := string([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	_, _, _, _, maxRecordIndex, err := checkIndexBounds(indexPath, indexFileOffset, highKey, config.SparseStepIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check index bounds for table %d: %v", tableIndex, err)
	}

	iterator := &SSTableIterator{
		index:              tableIndex,
		filePath:           dataPath,
		startOffset:        dataOffset,
		currentOffset:      dataOffset,
		recordIndex:        0,
		maxRecordIndex:     maxRecordIndex,
		compressionEnabled: config.CompressionEnabled,
		hasNextRecord:      true,
	}

	// Load first record
	err = iterator.loadNextRecord()
	if err != nil {
		return nil, fmt.Errorf("failed to load first record for table %d: %v", tableIndex, err)
	}

	return iterator, nil
}

// loadNextRecord loads the next record from the iterator's SSTable
func (iter *SSTableIterator) loadNextRecord() error {
	if iter.recordIndex > iter.maxRecordIndex {
		iter.hasNextRecord = false
		iter.currentRecord = nil
		return nil
	}

	blockManager := block_manager.GetBlockManager()

	// Read record size
	recordSizeBytes, newOffset, err := blockManager.ReadFromDisk(iter.filePath, iter.currentOffset, STANDARD_FLAG_SIZE)
	if err != nil {
		iter.hasNextRecord = false
		iter.currentRecord = nil
		return nil
	}
	recordSize := binary.LittleEndian.Uint64(recordSizeBytes)
	iter.currentOffset = newOffset

	// Read record data
	recordData, newOffset, err := blockManager.ReadFromDisk(iter.filePath, iter.currentOffset, recordSize)
	if err != nil {
		iter.hasNextRecord = false
		iter.currentRecord = nil
		return nil
	}
	iter.currentOffset = newOffset

	// Deserialize record
	rec := record.DeserializeForSSTable(recordData, iter.compressionEnabled)
	iter.currentRecord = rec
	iter.recordIndex++

	return nil
}

// advance moves the iterator to the next record
func (iter *SSTableIterator) advance() error {
	return iter.loadNextRecord()
}

// hasNext checks if the iterator has more records
func (iter *SSTableIterator) hasNext() bool {
	return iter.hasNextRecord && iter.currentRecord != nil
}

// getCurrentRecord returns the current record
func (iter *SSTableIterator) getCurrentRecord() *record.Record {
	return iter.currentRecord
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
		SparseStepIndex:    int(SPARSE_STEP_INDEX),
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

		// Add all prefixes of length 1 to 10 (or maximum key length) with a special prefix marker
		keyLen := len(rec.Key)
		maxPrefixLen := 10
		if keyLen < maxPrefixLen {
			maxPrefixLen = keyLen
		}

		for prefixLen := 1; prefixLen <= maxPrefixLen; prefixLen++ {
			prefix := rec.Key[:prefixLen]
			prefixWithMarker := prependPrefixPrefix(prefix)
			bloomFilter.Add([]byte(prefixWithMarker))
		}
	}
	bloomFilter.Add([]byte(prependPrefixPrefix("")))

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
		if i%int(SPARSE_STEP_INDEX) == 0 {
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
Helper function to prepend the prefix marker to a prefix
*/
func prependPrefixPrefix(prefix string) string {
	// Use non-printable characters that users cannot type
	const PREFIX_MARKER = "\x00\x01\x02"
	return PREFIX_MARKER + prefix
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
		if record.IsDeleted() {
			return nil, nil
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
		if record.IsDeleted() {
			return nil, nil
		}
		return record, nil
	}
}

/*
GetNextForPrefix retrieves the next record for a given prefix from the SSTable
*/
func GetNextForPrefix(prefix string, key string, tombstonedKeys *[]string, index int) (record *record.Record, err error) {

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
		if !filter.Contains([]byte(prependPrefixPrefix(prefix))) {
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

		maxPrefixLen := len(prefix)
		if maxPrefixLen > 10 {
			maxPrefixLen = 10
		}
		for prefixLen := 1; prefixLen <= maxPrefixLen; prefixLen++ {
			checkPrefix := prefix[:prefixLen]
			if !filter.Contains([]byte(prependPrefixPrefix(checkPrefix))) {
				return nil, nil
			}
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
	inIndexBounds, _, _, lastSummaryEntryIndex, lastIndexEntryIndex, err := checkIndexBoundsForPrefix(indexPath, indexFileOffset, key, config.SparseStepIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check index bounds: %v", err)
	}
	if !inIndexBounds {
		return nil, nil
	}
	if !config.UseSeparateFiles {
		indexFileOffset += STANDARD_FLAG_SIZE
	}

	// 3. Find starting position - use binary search to get close, then iterate sequentially
	var searchKey string = key

	// Use existing binary search to find the starting position (approximate)
	offset, _, err := lowerBoundSearchSummary(summaryPath, summaryOffset+STANDARD_FLAG_SIZE, 0, lastSummaryEntryIndex, searchKey, config.SparseStepIndex, indexFileOffset, config.UseSeparateFiles, index, lastIndexEntryIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to perform binary search on summary: %v", err)
	}

	var startingDataOffset uint64 = offset

	// 4. Sequential iteration from the starting position
	within := func(k string) bool { return strings.HasPrefix(k, prefix) }
	stop := func(k string) bool { return !strings.HasPrefix(k, prefix) }
	return iterateSequentially(dataPath, startingDataOffset, tombstonedKeys, config.CompressionEnabled, within, stop)

}

/*
Checks basic bounds for a range and computes last summary and index entry indexes for searching.
Returns inBounds=false if [rangeStart, rangeEnd] doesn't overlap with [firstKey, lastKey] of the table.
*/
func checkIndexBoundsForRange(filepath string, offset uint64, rangeStart string, rangeEnd string, sparseStep int) (bool, uint64, uint64, error) {
	// Read first entry key
	firstEntryKey, _, err := readIndexMetadataEntry(filepath, offset+STANDARD_FLAG_SIZE)
	if err != nil {
		return false, 0, 0, err
	}

	// Read last entry info
	blockManager := block_manager.GetBlockManager()
	lastEntryOffsetBytes, _, err := blockManager.ReadFromDisk(filepath, offset, STANDARD_FLAG_SIZE)
	if err != nil {
		return false, 0, 0, err
	}
	lastEntryOffset := binary.LittleEndian.Uint64(lastEntryOffsetBytes)
	lastEntryKey, _, err := readIndexMetadataEntry(filepath, lastEntryOffset)
	if err != nil {
		return false, 0, 0, err
	}

	// Quick overlap check: [rangeStart, rangeEnd] vs [firstEntryKey, lastEntryKey]
	if rangeEnd < firstEntryKey || rangeStart > lastEntryKey {
		return false, 0, 0, nil
	}

	// Compute last entry indexes similarly to other helpers
	physicalOffsetFirst := offset + STANDARD_FLAG_SIZE
	crcsFirst := (physicalOffsetFirst / BLOCK_SIZE) + 1
	logicalOffsetFirst := physicalOffsetFirst - crcsFirst*CRC_SIZE
	physicalOffsetLast := lastEntryOffset
	crcsLast := (physicalOffsetLast / BLOCK_SIZE) + 1
	logicalOffsetLast := physicalOffsetLast - crcsLast*CRC_SIZE

	indexOfLastIndexEntry := (logicalOffsetLast - logicalOffsetFirst) / INDEX_ENTRY_METADATA_SIZE
	indexOfLastSummaryEntry := indexOfLastIndexEntry / uint64(sparseStep)

	return true, indexOfLastSummaryEntry, indexOfLastIndexEntry, nil
}

/*
GetNextForRange retrieves the next record whose key is within [rangeStart, rangeEnd] (inclusive).
No Bloom filter checks are performed (range cannot be easily represented in filter).
It returns the next record strictly greater than the provided key.
*/
func GetNextForRange(rangeStart string, rangeEnd string, key string, tombstonedKeys *[]string, index int) (*record.Record, error) {

	// Quick invalid range check
	if rangeStart > rangeEnd {
		return nil, nil
	}

	// 0. Deserialize SSTable Config
	config, _, offsets, err := deserializeSSTableConfig(index)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize SSTable config: %v", err)
	}

	// 1.5. Data, Index and Summary preparation (no Bloom filter for range)
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

	// 2. Index Bounds Check for range overlap
	inBounds, lastSummaryEntryIndex, lastIndexEntryIndex, err := checkIndexBoundsForRange(indexPath, indexFileOffset, rangeStart, rangeEnd, config.SparseStepIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to check index bounds for range: %v", err)
	}
	if !inBounds {
		return nil, nil
	}
	if !config.UseSeparateFiles {
		indexFileOffset += STANDARD_FLAG_SIZE
	}

	// 3. Find starting position: first key strictly greater than max(key, rangeStart)
	startingKey := key
	if startingKey < rangeStart {
		startingKey = rangeStart
	}

	offset, found, err := lowerBoundSearchSummary(summaryPath, summaryOffset+STANDARD_FLAG_SIZE, 0, lastSummaryEntryIndex, startingKey, config.SparseStepIndex, indexFileOffset, config.UseSeparateFiles, index, lastIndexEntryIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to perform binary search on summary: %v", err)
	}

	// If there is no key strictly greater than startingKey, there's no next record in range
	if !found {
		return nil, nil
	}

	// Exact lower-bound offset to begin reading data
	startingDataOffset := offset

	// 4. Sequential iteration within range [rangeStart, rangeEnd] and strictly greater than 'key'
	within := func(k string) bool { return k > key && k >= rangeStart && k <= rangeEnd }
	stop := func(k string) bool { return k > rangeEnd }
	return iterateSequentially(dataPath, startingDataOffset, tombstonedKeys, config.CompressionEnabled, within, stop)
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
Returns if the key is within the bounds of the Index Component, (doesn't have to exactly be there due to prefix constraints).

If the key is in the bounds, the function returns true along with the index (ex. 645. or 7328.) of the last entry.

If the key happens to be one of the bounds, we return the offset in Data component of that bound's key.

If the key is not in the bounds, there is no point in searching further.
*/
func checkIndexBoundsForPrefix(filepath string, offset uint64, key string, sparseStep int) (bool, bool, uint64, uint64, uint64, error) {

	firstEntryKey, _, err := readIndexMetadataEntry(filepath, offset+STANDARD_FLAG_SIZE)

	if err != nil {
		return false, false, 0, 0, 0, err
	}
	if key < firstEntryKey {
		if !strings.HasPrefix(firstEntryKey, key) {
			return false, false, 0, 0, 0, nil
		}
	}

	blockManager := block_manager.GetBlockManager()
	lastEntryOffsetBytes, _, err := blockManager.ReadFromDisk(filepath, offset, STANDARD_FLAG_SIZE)
	if err != nil {
		return false, false, 0, 0, 0, err
	}
	lastEntryOffset := binary.LittleEndian.Uint64(lastEntryOffsetBytes)

	lastEntryKey, _, err := readIndexMetadataEntry(filepath, lastEntryOffset)

	if err != nil {
		return false, false, 0, 0, 0, err
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

	if indexFirst > indexLast {
		return 0, false, nil // Key not found, terminate gracefully
	}

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

	if indexFirst > indexLast {
		return 0, false, nil // Key not found, terminate gracefully
	}

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
lowerBoundSearchSummaryForPrefix performs a lower-bound search in the summary component for the first key >= startingKey.
If found, it then performs a more precise lower-bound search in the index component to find the exact offset.
*/
func lowerBoundSearchSummary(summaryPath string, offsetFirst uint64, indexFirst uint64,
	indexLast uint64, startingKey string, sparseIndex int, indexFileOffset uint64,
	useSeparateFiles bool, index int, originalIndexLast uint64) (uint64, bool, error) {

	low := indexFirst
	high := indexLast

	for low < high {
		mid := low + (high-low)/2

		physicalOffsetFirst := offsetFirst
		crcsTillFirst := (physicalOffsetFirst / BLOCK_SIZE) + 1
		logicalOffsetFirst := physicalOffsetFirst - crcsTillFirst*CRC_SIZE
		logicalOffsetMid := logicalOffsetFirst + uint64(mid)*INDEX_ENTRY_METADATA_SIZE
		crcsTillMid := ((logicalOffsetMid / (BLOCK_SIZE - CRC_SIZE)) + 1)
		physicalOffsetMid := logicalOffsetMid + crcsTillMid*CRC_SIZE

		midKey, _, err := readIndexMetadataEntry(summaryPath, physicalOffsetMid)
		if err != nil {
			return 0, false, err
		}

		if midKey >= startingKey {
			if mid == 0 {
				high = 0

			} else {
				high = mid - 1
			}
		} else {
			low = mid + 1
		}
	}

	indexFilePath := fmt.Sprintf(FILE_NAME_FORMAT, index)
	if useSeparateFiles {
		indexFilePath = fmt.Sprintf(INDEX_FILE_NAME_FORMAT, index)
		indexFileOffset += STANDARD_FLAG_SIZE
	}

	// Find the index range to search in the IndexComp
	if low > 0 {
		low -= 1
	}
	startIndex := low * uint64(sparseIndex)
	endIndex := (high + 2) * uint64(sparseIndex)
	if endIndex > originalIndexLast {
		endIndex = originalIndexLast
	}
	if endIndex > originalIndexLast {
		endIndex = originalIndexLast
	}

	return lowerBoundSearchIndexes(indexFilePath, startingKey, indexFileOffset, startIndex, endIndex)
}

/*
lowerBoundSearchIndexes performs a lower-bound search in the index entries to find the first key >= startingKey.
*/
func lowerBoundSearchIndexes(filepath string, startingKey string, offsetFirst uint64, indexFirst uint64, indexLast uint64) (uint64, bool, error) {
	low := indexFirst
	high := indexLast
	var bestOffset uint64 = 0

	for low <= high {
		mid := low + (high-low)/2

		// Calculate the physical offset of the mid-entry in the index file
		physicalOffsetFirst := offsetFirst
		crcsTillFirst := (physicalOffsetFirst / BLOCK_SIZE) + 1
		logicalOffsetFirst := physicalOffsetFirst - crcsTillFirst*CRC_SIZE
		logicalOffsetMid := logicalOffsetFirst + uint64(mid)*INDEX_ENTRY_METADATA_SIZE
		crcsTillMid := ((logicalOffsetMid / (BLOCK_SIZE - CRC_SIZE)) + 1)
		physicalOffsetMid := logicalOffsetMid + crcsTillMid*CRC_SIZE

		midKey, midOffset, err := readIndexMetadataEntry(filepath, physicalOffsetMid)
		if err != nil {
			return 0, false, err
		}

		if midKey > startingKey {
			// This is a potential answer. Store it and try to find an even
			// better (smaller) key in the left half.
			bestOffset = midOffset
			if mid == 0 {
				high = 0
				break
			} else {
				high = mid - 1
			}
		} else { // midKey < startingKey
			// The answer must be in the right half.
			low = mid + 1
		}
	}

	return bestOffset, bestOffset != 0, nil
}

/*
Generic helper for sequential iteration with custom inclusion and stop conditions.
within returns true if the record is within the desired window (e.g., has prefix or in range).
stop returns true if we should stop scanning (e.g., key no longer has prefix or key > rangeEnd).
*/
func iterateSequentially(
	dataPath string,
	startOffset uint64,
	tombstonedKeys *[]string,
	compressionEnabled bool,
	within func(string) bool,
	stop func(string) bool,
) (*record.Record, error) {
	blockManager := block_manager.GetBlockManager()
	currentOffset := startOffset

	// Read the data file sequentially to find the next valid record
	for {
		// Read record size
		recordSizeBytes, newOffset, err := blockManager.ReadFromDisk(dataPath, currentOffset, STANDARD_FLAG_SIZE)
		if err != nil {
			if err == io.EOF {
				return nil, nil // No more records
			}
			return nil, err
		}
		recordSize := binary.LittleEndian.Uint64(recordSizeBytes)
		if recordSize == 0 {
			return nil, nil // Invalid record size indicates end
		}
		currentOffset = newOffset

		// Read record data
		// Guard against pathological sizes which would cause panics deeper in deserialization
		// If recordSize is excessively large, bail out gracefully.
		if recordSize > 64*1024*1024 { // 64MB safety cap, data files here are small
			return nil, nil
		}
		recordData, newOffset, err := blockManager.ReadFromDisk(dataPath, currentOffset, recordSize)
		if err != nil {
			if err == io.EOF {
				return nil, nil // No more records
			}
			return nil, err
		}
		currentOffset = newOffset

		// Deserialize record
		rec := record.DeserializeForSSTable(recordData, compressionEnabled)

		// If we are outside of the scan window, stop.
		if stop != nil && stop(rec.Key) {
			return nil, nil
		}

		// If not in our window yet, continue scanning
		if within != nil && !within(rec.Key) {
			continue
		}

		// Handle tombstones only for records within the window
		if rec.IsDeleted() {
			if tombstonedKeys != nil {
				isTomstonedAlready := false
				for _, tombstonedKey := range *tombstonedKeys {
					if tombstonedKey == rec.Key {
						isTomstonedAlready = true
						break
					}
				}
				if isTomstonedAlready {
					continue
				}
				// Add to tombstoned list and continue
				*tombstonedKeys = append(*tombstonedKeys, rec.Key)
			}
			continue
		}

		// Check if already tombstoned by higher levels
		if tombstonedKeys != nil {
			isTombstoned := false
			for _, tombstonedKey := range *tombstonedKeys {
				if tombstonedKey == rec.Key {
					isTombstoned = true
					break
				}
			}
			if isTombstoned {
				continue // Skip tombstoned record
			}
		}

		// Found a valid record!
		return rec, nil
	}
}

/*
ScanForPrefix scans records with the given prefix and adds keys to bestKeys.
Only keys are added for memory efficiency - use Get() to retrieve full records.
Parameters:
- prefix: the key prefix to search for
- tombstonedKeys: keys that have been tombstoned in more recent structures
- bestKeys: best keys found so far from previous memtables (will be modified)
- pageSize: maximum number of results per page (typically <= 50)
- pageNumber: which page to return (0-based)
- index: SSTable index to scan
*/
func ScanForPrefix(prefix string, tombstonedKeys *[]string, bestKeys *[]string, pageSize int, pageNumber int, index int) error {
	// 0. Deserialize SSTable Config
	config, sizes, offsets, err := deserializeSSTableConfig(index)
	if err != nil {
		return fmt.Errorf("failed to deserialize SSTable config: %v", err)
	}

	// 1. Bloom Filter Check
	if config.UseSeparateFiles {
		filterPath := fmt.Sprintf(FILTER_FILE_NAME_FORMAT, index)
		filterSize, err := getComponentSize(filterPath)
		if err != nil {
			return fmt.Errorf("failed to get filter component size: %v", err)
		}
		filter, err := deserializeFilter(filterPath, 0, filterSize, config.UseSeparateFiles)
		if err != nil {
			return fmt.Errorf("failed to deserialize filter: %v", err)
		}
		if !filter.Contains([]byte(prependPrefixPrefix(prefix))) {
			return nil // No records with this prefix
		}
	} else {
		filterPath := fmt.Sprintf(FILE_NAME_FORMAT, index)
		filterOffset := offsets[3]
		filterSize := sizes[3]
		filter, err := deserializeFilter(filterPath, filterOffset, filterSize, config.UseSeparateFiles)
		if err != nil {
			return fmt.Errorf("failed to deserialize filter (single file): %v", err)
		}

		// Check multiple prefix lengths up to 10 characters
		maxPrefixLen := len(prefix)
		if maxPrefixLen > 10 {
			maxPrefixLen = 10
		}
		for prefixLen := 1; prefixLen <= maxPrefixLen; prefixLen++ {
			checkPrefix := prefix[:prefixLen]
			if !filter.Contains([]byte(prependPrefixPrefix(checkPrefix))) {
				return nil
			}
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
	inIndexBounds, _, _, lastSummaryEntryIndex, lastIndexEntryIndex, err := checkIndexBoundsForPrefix(indexPath, indexFileOffset, prefix, config.SparseStepIndex)
	if err != nil {
		return fmt.Errorf("failed to check index bounds: %v", err)
	}
	if !inIndexBounds {
		return nil
	}
	if !config.UseSeparateFiles {
		indexFileOffset += STANDARD_FLAG_SIZE
	}

	// 3. Find starting position using lexicographically smaller string
	searchKey := string_util.FindLexicographicallySmaller(prefix)

	// Use existing binary search to find the starting position
	offset, found, err := lowerBoundSearchSummary(summaryPath, summaryOffset+STANDARD_FLAG_SIZE, 0, lastSummaryEntryIndex, searchKey, config.SparseStepIndex, indexFileOffset, config.UseSeparateFiles, index, lastIndexEntryIndex)
	if err != nil {
		return fmt.Errorf("failed to perform binary search on summary: %v", err)
	}

	var startingDataOffset uint64
	if found {
		startingDataOffset = offset
	} else {
		// If not found, start from the beginning of data
		startingDataOffset = uint64(CRC_SIZE)
		if config.UseSeparateFiles {
			startingDataOffset += STANDARD_FLAG_SIZE
		} else {
			startingDataOffset = offsets[0] + CRC_SIZE
		}
	}

	// 4. Sequential scan from the starting position
	return scanSequentiallyForPrefixRange(dataPath, startingDataOffset, prefix, tombstonedKeys, bestKeys, pageSize, pageNumber, config.CompressionEnabled)
}

/*
scanSequentiallyForPrefixRange scans records sequentially starting from the given offset,
looking for records with the specified prefix. It handles tombstones and maintains bestKeys with pagination.
*/
func scanSequentiallyForPrefixRange(dataPath string, startOffset uint64, prefix string, tombstonedKeys *[]string, bestKeys *[]string, pageSize int, pageNumber int, compressionEnabled bool) error {
	blockManager := block_manager.GetBlockManager()
	currentOffset := startOffset

	// Create sets for efficient lookup
	tombstonedSet := make(map[string]bool)
	if tombstonedKeys != nil {
		for _, key := range *tombstonedKeys {
			tombstonedSet[key] = true
		}
	}

	bestKeysSet := make(map[string]bool)
	if bestKeys != nil {
		for _, key := range *bestKeys {
			bestKeysSet[key] = true
		}
	}

	var candidateKeys []string
	foundPrefixRange := false

	// Read records sequentially
	for {
		// Read record size
		recordSizeBytes, newOffset, err := blockManager.ReadFromDisk(dataPath, currentOffset, STANDARD_FLAG_SIZE)
		if err != nil {
			if err == io.EOF {
				break // No more records
			}
			return err
		}
		recordSize := binary.LittleEndian.Uint64(recordSizeBytes)
		if recordSize == 0 {
			break // Invalid record size indicates end
		}
		currentOffset = newOffset

		// Read record data
		recordData, newOffset, err := blockManager.ReadFromDisk(dataPath, currentOffset, recordSize)
		if err != nil {
			if err == io.EOF {
				break // No more records
			}
			return err
		}
		currentOffset = newOffset

		// Deserialize record
		rec := record.DeserializeForSSTable(recordData, compressionEnabled)

		// Check if we've found the prefix range
		if strings.HasPrefix(rec.Key, prefix) {
			foundPrefixRange = true
		} else if foundPrefixRange {
			// We've moved past the prefix range, stop scanning
			break
		} else {
			// We haven't reached the prefix range yet, continue scanning
			continue
		}

		// Skip if already tombstoned by higher levels
		if tombstonedSet[rec.Key] {
			continue
		}

		// Skip if already found in newer memtables
		if bestKeysSet[rec.Key] {
			continue
		}

		// Handle tombstones
		if rec.IsDeleted() {
			if tombstonedKeys != nil {
				*tombstonedKeys = append(*tombstonedKeys, rec.Key)
				tombstonedSet[rec.Key] = true
			}
			continue
		}

		// Add to candidate keys
		candidateKeys = append(candidateKeys, rec.Key)
		bestKeysSet[rec.Key] = true
	}

	// Handle pagination
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize

	if startIndex >= len(candidateKeys) {
		return nil // No keys for this page
	}

	if endIndex > len(candidateKeys) {
		endIndex = len(candidateKeys)
	}

	// Add paginated keys to bestKeys in sorted order
	if bestKeys != nil {
		for i := startIndex; i < endIndex; i++ {
			*bestKeys = insertKeySortedIfNotExists(*bestKeys, candidateKeys[i])
		}
	}

	return nil
}

/*
ScanForRange scans records within the given range [rangeStart, rangeEnd] (inclusive) and adds keys to bestKeys.
Only keys are added for memory efficiency - use Get() to retrieve full records.
No Bloom filter checks are performed as ranges cannot be easily represented in filters.
Parameters:
- rangeStart: the starting key of the range (inclusive)
- rangeEnd: the ending key of the range (inclusive)
- tombstonedKeys: keys that have been tombstoned in more recent structures
- bestKeys: best keys found so far from previous memtables (will be modified)
- pageSize: maximum number of results per page (typically <= 50)
- pageNumber: which page to return (0-based)
- index: SSTable index to scan
*/
func ScanForRange(rangeStart string, rangeEnd string, tombstonedKeys *[]string, bestKeys *[]string, pageSize int, pageNumber int, index int) error {
	// Quick invalid range check
	if rangeStart > rangeEnd {
		return nil
	}

	// 0. Deserialize SSTable Config
	config, _, offsets, err := deserializeSSTableConfig(index)
	if err != nil {
		return fmt.Errorf("failed to deserialize SSTable config: %v", err)
	}

	// Skip Bloom filter check - ranges cannot be easily represented in filters

	// 1. Data, Index and Summary preparation
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

	// 2. Index Bounds Check for range overlap
	inBounds, lastSummaryEntryIndex, lastIndexEntryIndex, err := checkIndexBoundsForRange(indexPath, indexFileOffset, rangeStart, rangeEnd, config.SparseStepIndex)
	if err != nil {
		return fmt.Errorf("failed to check index bounds for range: %v", err)
	}
	if !inBounds {
		return nil
	}
	if !config.UseSeparateFiles {
		indexFileOffset += STANDARD_FLAG_SIZE
	}

	// 3. Find starting position using lexicographically smaller string than rangeStart
	searchKey := string_util.FindLexicographicallySmaller(rangeStart)

	// Use existing binary search to find the starting position
	offset, found, err := lowerBoundSearchSummary(summaryPath, summaryOffset+STANDARD_FLAG_SIZE, 0, lastSummaryEntryIndex, searchKey, config.SparseStepIndex, indexFileOffset, config.UseSeparateFiles, index, lastIndexEntryIndex)
	if err != nil {
		return fmt.Errorf("failed to perform binary search on summary: %v", err)
	}

	var startingDataOffset uint64
	if found {
		startingDataOffset = offset
	} else {
		// If not found, start from the beginning of data
		startingDataOffset = uint64(CRC_SIZE)
		if config.UseSeparateFiles {
			startingDataOffset += STANDARD_FLAG_SIZE
		} else {
			startingDataOffset = offsets[0] + CRC_SIZE
		}
	}

	// 4. Sequential scan from the starting position
	return scanSequentiallyForRange(dataPath, startingDataOffset, rangeStart, rangeEnd, tombstonedKeys, bestKeys, pageSize, pageNumber, config.CompressionEnabled)
}

/*
scanSequentiallyForRange scans records sequentially starting from the given offset,
looking for records within the specified range [rangeStart, rangeEnd] (inclusive).
It handles tombstones and maintains bestKeys with pagination.
*/
func scanSequentiallyForRange(dataPath string, startOffset uint64, rangeStart string, rangeEnd string, tombstonedKeys *[]string, bestKeys *[]string, pageSize int, pageNumber int, compressionEnabled bool) error {
	blockManager := block_manager.GetBlockManager()
	currentOffset := startOffset

	// Create sets for efficient lookup
	tombstonedSet := make(map[string]bool)
	if tombstonedKeys != nil {
		for _, key := range *tombstonedKeys {
			tombstonedSet[key] = true
		}
	}

	bestKeysSet := make(map[string]bool)
	if bestKeys != nil {
		for _, key := range *bestKeys {
			bestKeysSet[key] = true
		}
	}

	var candidateKeys []string
	foundRangeStart := false

	// Read records sequentially
	for {
		// Read record size
		recordSizeBytes, newOffset, err := blockManager.ReadFromDisk(dataPath, currentOffset, STANDARD_FLAG_SIZE)
		if err != nil {
			if err == io.EOF {
				break // No more records
			}
			return err
		}
		recordSize := binary.LittleEndian.Uint64(recordSizeBytes)
		if recordSize == 0 {
			break // Invalid record size indicates end
		}
		currentOffset = newOffset

		// Read record data
		recordData, newOffset, err := blockManager.ReadFromDisk(dataPath, currentOffset, recordSize)
		if err != nil {
			if err == io.EOF {
				break // No more records
			}
			return err
		}
		currentOffset = newOffset

		// Deserialize record
		rec := record.DeserializeForSSTable(recordData, compressionEnabled)

		// Check if we've reached the range
		if rec.Key >= rangeStart && rec.Key <= rangeEnd {
			foundRangeStart = true
		} else if foundRangeStart && rec.Key > rangeEnd {
			// We've moved past the range, stop scanning
			break
		} else if rec.Key < rangeStart {
			// We haven't reached the range yet, continue scanning
			continue
		}

		// If we're not in the range, skip
		if rec.Key < rangeStart || rec.Key > rangeEnd {
			continue
		}

		// Skip if already tombstoned by higher levels
		if tombstonedSet[rec.Key] {
			continue
		}

		// Skip if already found in newer memtables
		if bestKeysSet[rec.Key] {
			continue
		}

		// Handle tombstones
		if rec.IsDeleted() {
			if tombstonedKeys != nil {
				*tombstonedKeys = append(*tombstonedKeys, rec.Key)
				tombstonedSet[rec.Key] = true
			}
			continue
		}

		// Add to candidate keys
		candidateKeys = append(candidateKeys, rec.Key)
		bestKeysSet[rec.Key] = true
	}

	// Handle pagination
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize

	if startIndex >= len(candidateKeys) {
		return nil // No keys for this page
	}

	if endIndex > len(candidateKeys) {
		endIndex = len(candidateKeys)
	}

	// Add paginated keys to bestKeys in sorted order
	if bestKeys != nil {
		for i := startIndex; i < endIndex; i++ {
			*bestKeys = insertKeySortedIfNotExists(*bestKeys, candidateKeys[i])
		}
	}

	return nil
}

// insertKeySortedIfNotExists inserts a key in sorted order into the slice if it doesn't already exist
func insertKeySortedIfNotExists(keys []string, newKey string) []string {
	// Check if key already exists
	for _, key := range keys {
		if key == newKey {
			return keys // Key already exists, don't add
		}
	}

	// Binary search for insertion point
	left, right := 0, len(keys)
	for left < right {
		mid := (left + right) / 2
		if keys[mid] < newKey {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// Insert at the found position
	keys = append(keys, "")
	copy(keys[left+1:], keys[left:])
	keys[left] = newKey
	return keys
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

	if len(recordHashes) == 0 {
		emptyLeaf := md5.Sum([]byte{})
		recordHashes = append(recordHashes, emptyLeaf[:])
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

/*
Compact performs SSTable compaction by merging multiple SSTables into a single new SSTable.
The input SSTables are specified by their indexes, sorted by age (newest first).
The compacted SSTable will be stored at the specified newIndex.
*/
func Compact(sstableIndexes []int, newIndex int) error {
	if len(sstableIndexes) == 0 {
		return fmt.Errorf("no SSTables provided for compaction")
	}

	blockManager := block_manager.GetBlockManager()

	// 1. Load configs and initialize iterators
	iterators := make([]*SSTableIterator, 0, len(sstableIndexes))

	for _, tableIndex := range sstableIndexes {
		config, sizes, offsets, err := deserializeSSTableConfig(tableIndex)
		if err != nil {
			return fmt.Errorf("failed to deserialize config for table %d: %v", tableIndex, err)
		}

		iterator, err := initializeIterator(tableIndex, config, sizes, offsets)
		if err != nil {
			return fmt.Errorf("failed to initialize iterator for table %d: %v", tableIndex, err)
		}
		iterators = append(iterators, iterator)
	}

	// 2. Create new SSTable config using global variables
	newConfig := &SSTableConfig{
		UseSeparateFiles:   USE_SEPARATE_FILES,
		CompressionEnabled: COMPRESSION_ENABLED,
		SparseStepIndex:    SPARSE_STEP_INDEX,
	}

	// 3. Persist new config
	serializedConfig, configSize, err := newConfig.serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize new config: %v", err)
	}
	err = blockManager.WriteToDisk(serializedConfig, fmt.Sprintf(FILE_NAME_FORMAT, newIndex), 0)
	if err != nil {
		return fmt.Errorf("failed to write new config: %v", err)
	}

	// 4. Setup data component paths
	dataStartOffset := configSize
	dataFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
	if USE_SEPARATE_FILES {
		dataStartOffset = 0
		dataFilePath = fmt.Sprintf(DATA_FILE_NAME_FORMAT, newIndex)
	}

	// 5. Initialize compaction state (memory-efficient)
	state := &CompactionState{
		iterators:         iterators,
		totalNewRecords:   0,
		newDataOffset:     dataStartOffset,
		recordHashes:      make([][]byte, 0),
		indexEntries:      make([]IndexEntry, 0),
		dataFilePath:      dataFilePath,
		currentDataOffset: dataStartOffset,
		totalLogical:      0,
		wroteSizePrefix:   false,
	}

	if USE_SEPARATE_FILES {
		state.currentDataOffset += STANDARD_FLAG_SIZE
	}
	state.currentDataOffset += CRC_SIZE
	// Capture base physical offset for first record start (after CRC and size prefix if any)
	state.dataPhysicalBase = state.currentDataOffset

	// 6. Perform streaming compaction
	err = performStreamingDataCompaction(state)
	if err != nil {
		return fmt.Errorf("failed to compact data: %v", err)
	}

	// 7. Create other components
	err = createCompactedComponentsFromState(state, newIndex, newConfig, dataStartOffset)
	if err != nil {
		return fmt.Errorf("failed to create compacted components: %v", err)
	}

	return nil
}

// performStreamingDataCompaction performs merge-sort compaction with streaming writes
func performStreamingDataCompaction(state *CompactionState) error {
	blockManager := block_manager.GetBlockManager()

	// Track data to accumulate before writing to disk in blocks
	accumulatedData := []byte{}
	tombstonedKeys := make(map[string]bool)

	for {
		// Find the iterator with the smallest current key
		minIterator := findMinIterator(state.iterators)
		if minIterator == nil {
			break // All iterators exhausted
		}

		currentRecord := minIterator.getCurrentRecord()
		currentKey := currentRecord.Key

		// Check if this key is tombstoned
		if currentRecord.IsDeleted() {
			tombstonedKeys[currentKey] = true
			// Skip this record and all future occurrences of this key
			skipKeyInAllIterators(state.iterators, currentKey)
			continue
		}

		// Check if this key was already tombstoned by a newer SSTable
		if tombstonedKeys[currentKey] {
			// Skip this record and all future occurrences of this key
			skipKeyInAllIterators(state.iterators, currentKey)
			continue
		}

		// This is a valid record - serialize and stream it
		serializedRecord := currentRecord.SerializeForSSTable(COMPRESSION_ENABLED)

		// Store hash for Merkle tree (only 32 bytes per record)
		recordHash := md5.Sum(serializedRecord)
		state.recordHashes = append(state.recordHashes, recordHash[:])

		// Create index entry using total logical bytes so far (since data start)
		noOfBlocks := uint64(state.totalLogical / (BLOCK_SIZE - CRC_SIZE))
		actualOffset := state.currentDataOffset + noOfBlocks*CRC_SIZE

		state.indexEntries = append(state.indexEntries, IndexEntry{
			Key:    currentRecord.Key,
			Offset: actualOffset,
		})

		// Prepare record for writing
		recordSize := make([]byte, STANDARD_FLAG_SIZE)
		binary.LittleEndian.PutUint64(recordSize, uint64(len(serializedRecord)))

		recordData := append(recordSize, serializedRecord...)
		accumulatedData = append(accumulatedData, recordData...)

		recordTotalSize := uint64(len(recordData))
		state.currentDataOffset += recordTotalSize // logical cursor (base + totalLogical)
		state.totalLogical += recordTotalSize      // total logical bytes since data start
		state.totalNewRecords++

		// Advance the iterator we consumed from
		_ = minIterator.advance()
		// Skip this key in all other iterators
		skipKeyInAllIterators(state.iterators, currentKey)

		// If accumulated data is approaching a block boundary, flush to disk periodically
		if len(accumulatedData) > 0 && (uint64(len(accumulatedData))%(BLOCK_SIZE-CRC_SIZE) < STANDARD_FLAG_SIZE) {
			toWrite := accumulatedData
			// Only the very first chunk in separate-file mode should start with the size prefix placeholder
			if USE_SEPARATE_FILES && !state.wroteSizePrefix {
				// Prepend placeholder (actual size will be patched later)
				placeholder := make([]byte, STANDARD_FLAG_SIZE)
				toWrite = append(placeholder, toWrite...)
				state.wroteSizePrefix = true
			}
			finalBytes := crc_util.AddCRCsToData(toWrite)
			byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
			crc_util.FixLastBlockCRC(finalBytes)
			if err := blockManager.WriteToDisk(finalBytes, state.dataFilePath, state.newDataOffset); err != nil {
				return fmt.Errorf("failed to write data chunk to disk: %v", err)
			}
			state.newDataOffset += uint64(len(finalBytes))
			accumulatedData = accumulatedData[:0]
			// currentDataOffset already tracked physical pointer; keep as-is
		}
	}

	// Write accumulated data to disk
	if len(accumulatedData) > 0 {
		if USE_SEPARATE_FILES && !state.wroteSizePrefix {
			// Prepend placeholder on the first (and only) write
			placeholder := make([]byte, STANDARD_FLAG_SIZE)
			accumulatedData = append(placeholder, accumulatedData...)
			state.wroteSizePrefix = true
		}

		finalBytes := crc_util.AddCRCsToData(accumulatedData)
		byte_util.AddPadding(&finalBytes, BLOCK_SIZE)
		crc_util.FixLastBlockCRC(finalBytes)

		err := blockManager.WriteToDisk(finalBytes, state.dataFilePath, state.newDataOffset)
		if err != nil {
			return fmt.Errorf("failed to write data to disk: %v", err)
		}
		state.newDataOffset += uint64(len(finalBytes))
	}

	// If separate files, patch the size prefix with the actual logical size and fix CRC of first block
	if USE_SEPARATE_FILES && state.wroteSizePrefix {
		// Read the first block
		blk, err := blockManager.ReadBlock(block_location.BlockLocation{FilePath: state.dataFilePath, BlockIndex: 0})
		if err != nil {
			return fmt.Errorf("failed to read first data block for patching: %v", err)
		}
		// Extract data portion
		if uint64(len(blk)) < BLOCK_SIZE {
			return fmt.Errorf("invalid block size while patching data size prefix")
		}
		dataPart := make([]byte, BLOCK_SIZE-CRC_SIZE)
		copy(dataPart, blk[CRC_SIZE:CRC_SIZE+int(BLOCK_SIZE-CRC_SIZE)])
		// Set size prefix = total logical bytes. The prefix stores the length of the data *following* it.
		sizeVal := state.totalLogical
		binary.LittleEndian.PutUint64(dataPart[0:STANDARD_FLAG_SIZE], sizeVal)
		// Recompute CRC for this block and write it back
		newBlk := crc_util.AddCRCToBlockData(dataPart)
		if err := blockManager.WriteToDisk(newBlk, state.dataFilePath, 0); err != nil {
			return fmt.Errorf("failed to patch size prefix in data file: %v", err)
		}
	}

	return nil
}

// findMinIterator finds the iterator with the smallest current key
func findMinIterator(iterators []*SSTableIterator) *SSTableIterator {
	var minIterator *SSTableIterator
	var minKey string

	for _, iter := range iterators {
		if iter.hasNext() {
			currentKey := iter.getCurrentRecord().Key
			if minIterator == nil || currentKey < minKey {
				minKey = currentKey
				minIterator = iter
			}
		}
	}

	return minIterator
}

// skipKeyInAllIterators advances all iterators past the given key
func skipKeyInAllIterators(iterators []*SSTableIterator, key string) {
	for _, iter := range iterators {
		for iter.hasNext() && iter.getCurrentRecord().Key == key {
			iter.advance()
		}
	}
}

// createCompactedComponentsFromState creates all remaining components using the compaction state
func createCompactedComponentsFromState(state *CompactionState, newIndex int, config *SSTableConfig, dataStartOffset uint64) error {
	blockManager := block_manager.GetBlockManager()

	// Handle the edge case where no records survived compaction (all were tombstoned)
	if len(state.indexEntries) == 0 || state.totalNewRecords == 0 {
		// Data component: write empty payload (size prefix only in separate files)
		if USE_SEPARATE_FILES {
			empty := make([]byte, 0)
			prependSizePrefix(&empty)
			final := crc_util.AddCRCsToData(empty)
			byte_util.AddPadding(&final, BLOCK_SIZE)
			crc_util.FixLastBlockCRC(final)
			if err := blockManager.WriteToDisk(final, fmt.Sprintf(DATA_FILE_NAME_FORMAT, newIndex), 0); err != nil {
				return err
			}
		}

		// Calculate component start offsets similar to non-empty
		indexStartOffset := dataStartOffset
		if !USE_SEPARATE_FILES {
			// No data written; start right after dataStartOffset
			indexStartOffset = state.newDataOffset
		}
		indexFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
		if USE_SEPARATE_FILES {
			indexStartOffset = 0
			indexFilePath = fmt.Sprintf(INDEX_FILE_NAME_FORMAT, newIndex)
		}
		emptyIndex := &IndexComp{FilePath: indexFilePath, StartOffset: indexStartOffset, IndexEntries: []IndexEntry{}}
		idxBytes, idxSize, err := emptyIndex.serialize(indexStartOffset)
		if err != nil {
			return err
		}
		if err := blockManager.WriteToDisk(idxBytes, indexFilePath, indexStartOffset); err != nil {
			return err
		}

		summaryStartOffset := indexStartOffset + uint64(len(idxBytes))
		summaryFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
		if USE_SEPARATE_FILES {
			summaryStartOffset = 0
			summaryFilePath = fmt.Sprintf(SUMMARY_FILE_NAME_FORMAT, newIndex)
		}
		emptySummary := &SummaryComp{FilePath: summaryFilePath, StartOffset: summaryStartOffset, MinKey: "", MaxKey: "", IndexEntries: []IndexEntry{}}
		sumBytes, sumSize, err := emptySummary.serialize(summaryStartOffset)
		if err != nil {
			return err
		}
		if err := blockManager.WriteToDisk(sumBytes, summaryFilePath, summaryStartOffset); err != nil {
			return err
		}

		filterStartOffset := summaryStartOffset + uint64(len(sumBytes))
		filterFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
		if USE_SEPARATE_FILES {
			filterFilePath = fmt.Sprintf(FILTER_FILE_NAME_FORMAT, newIndex)
			filterStartOffset = 0
		}
		bf := bloom_filter.NewBloomFilter(1, BLOOM_FILTER_FALSE_POSITIVE_RATE)
		filterComp := &FilterComp{FilePath: filterFilePath, StartOffset: filterStartOffset, BloomFilter: bf}
		filterBytes, filterSize, err := filterComp.serialize()
		if err != nil {
			return err
		}
		if err := blockManager.WriteToDisk(filterBytes, filterFilePath, filterStartOffset); err != nil {
			return err
		}

		metaDataStartOffset := filterStartOffset + uint64(len(filterBytes))
		metaDataFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
		if USE_SEPARATE_FILES {
			metaDataStartOffset = 0
			metaDataFilePath = fmt.Sprintf(METADATA_FILE_NAME_FORMAT, newIndex)
		}
		emptyLeaf := md5.Sum([]byte{})
		mt, err := merkle_tree.NewMerkleTree([][]byte{emptyLeaf[:]}, true)
		if err != nil {
			return err
		}
		metaComp := &MetadataComp{FilePath: metaDataFilePath, StartOffset: metaDataStartOffset, MerkleTree: mt}
		metaBytes, metaSize, err := metaComp.serialize()
		if err != nil {
			return err
		}
		if err := blockManager.WriteToDisk(metaBytes, metaDataFilePath, metaDataStartOffset); err != nil {
			return err
		}

		// Update main config if single-file mode
		if !USE_SEPARATE_FILES {
			sizes := []uint64{0, idxSize, sumSize, filterSize, metaSize}
			offsets := []uint64{dataStartOffset, indexStartOffset, summaryStartOffset, filterStartOffset, metaDataStartOffset}
			if err := config.addSizeDataToConfig(sizes, offsets, newIndex); err != nil {
				return err
			}
		}
		return nil
	}

	// Calculate data size (logical, without CRCs). In single-file mode this is needed for index offset.
	dataSize := state.totalLogical

	// 1. Create Index Component — in single-file mode, place right after last written data chunk (aligned)
	indexStartOffset := state.newDataOffset
	indexFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
	if USE_SEPARATE_FILES {
		indexStartOffset = 0
		indexFilePath = fmt.Sprintf(INDEX_FILE_NAME_FORMAT, newIndex)
	}

	indexComp := &IndexComp{
		FilePath:     indexFilePath,
		StartOffset:  indexStartOffset,
		IndexEntries: state.indexEntries,
	}

	serializedIndex, indexSize, err := indexComp.serialize(indexStartOffset)
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedIndex, indexComp.FilePath, indexComp.StartOffset)
	if err != nil {
		return err
	}

	// 2. Create Summary Component
	summaryStartOffset := indexStartOffset + uint64(len(serializedIndex))
	summaryFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
	if USE_SEPARATE_FILES {
		summaryStartOffset = 0
		summaryFilePath = fmt.Sprintf(SUMMARY_FILE_NAME_FORMAT, newIndex)
	}

	// Get min and max keys
	var minKey, maxKey string
	if len(state.indexEntries) > 0 {
		minKey = state.indexEntries[0].Key
		maxKey = state.indexEntries[len(state.indexEntries)-1].Key
	}

	summaryComp := &SummaryComp{
		FilePath:     summaryFilePath,
		StartOffset:  summaryStartOffset,
		MinKey:       minKey,
		MaxKey:       maxKey,
		IndexEntries: generateSummaryEntries(state.indexEntries),
	}

	serializedSummary, summarySize, err := summaryComp.serialize(summaryStartOffset)
	if err != nil {
		return err
	}
	err = blockManager.WriteToDisk(serializedSummary, summaryComp.FilePath, summaryComp.StartOffset)
	if err != nil {
		return err
	}

	// 3. Create Filter Component
	filterStartOffset := summaryStartOffset + uint64(len(serializedSummary))
	filterFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
	if USE_SEPARATE_FILES {
		filterFilePath = fmt.Sprintf(FILTER_FILE_NAME_FORMAT, newIndex)
		filterStartOffset = 0
	}

	expected := len(state.indexEntries)
	if expected <= 0 {
		expected = 1
	}
	bloomFilter := bloom_filter.NewBloomFilter(expected, BLOOM_FILTER_FALSE_POSITIVE_RATE)
	for _, entry := range state.indexEntries {
		bloomFilter.Add([]byte(entry.Key))

		// Add prefixes
		keyLen := len(entry.Key)
		maxPrefixLen := 10
		if keyLen < maxPrefixLen {
			maxPrefixLen = keyLen
		}

		for prefixLen := 1; prefixLen <= maxPrefixLen; prefixLen++ {
			prefix := entry.Key[:prefixLen]
			prefixWithMarker := prependPrefixPrefix(prefix)
			bloomFilter.Add([]byte(prefixWithMarker))
		}
	}
	bloomFilter.Add([]byte(prependPrefixPrefix("")))

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

	// 4. Create Metadata Component (using only hashes, memory-efficient)
	metaDataStartOffset := filterStartOffset + uint64(len(serializedFilter))
	metaDataFilePath := fmt.Sprintf(FILE_NAME_FORMAT, newIndex)
	if USE_SEPARATE_FILES {
		metaDataStartOffset = 0
		metaDataFilePath = fmt.Sprintf(METADATA_FILE_NAME_FORMAT, newIndex)
	}

	merkleTree, err := merkle_tree.NewMerkleTree(state.recordHashes, true)
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

	// 5. Update config with component sizes and offsets (if single file mode)
	sizes := []uint64{dataSize, indexSize, summarySize, filterSize, metadataSize}
	offsets := []uint64{dataStartOffset, indexStartOffset, summaryStartOffset, filterStartOffset, metaDataStartOffset}
	err = config.addSizeDataToConfig(sizes, offsets, newIndex)
	if err != nil {
		return err
	}

	return nil
}
