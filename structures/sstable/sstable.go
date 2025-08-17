package sstable

// import (
// 	block_manager "hunddb/structures/block_manager"
// )

import (
	mdl "hunddb/model"
	bloom_filter "hunddb/structures/bloom_filter"
	merkle_tree "hunddb/structures/merkle_tree"
)

// TODO: We must implement a compressed record (without the data part)
// when the Tombstone is true (SerializeForDisk method will suffice)

// TODO: We must implement a global compression dictionary for string keys in utils

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

	// Serialize should turn the component into a byte array
	Serialize() ([]byte, error)

	// Write to disk should write the component to the disk at the given location.
	WriteToDisk(filePath string, startOffset uint64) error
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
		Base file path - this file will always persist SSTableConfig,
		and potentially the whole SSTable if UseSeparateFiles is false.

		The file would be named sstable_{index}.db, if we use seperate files for
		components, they would be sstable_{index}_filter.db and similar for others..
	*/
	FilePath string

	/*
		What sstable_{index}.db looks like in memory (in case of UseSeparateFiles = true,
		only the config part is present).
		Config includes non-component fields of the SSTable - level, index, ...
		+-------------------+---------------+----------------+-----------------+-...-+--...--+
		| Config Size (8B)  | Config        | Data Size (8B) | Data            | ... |  ...  |
		+-------------------+---------------+----------------+-----------------+-...-+--...--+
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

	/*
		Size of a block in bytes.
	*/
	BlockSize uint64
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
	Records []mdl.Record
}

// IndexEntry represents an entry in the IndexComp.
type IndexEntry struct {
	Key string
	/*
		Offset in DataComp where this record starts.
	*/
	Offset uint64
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
