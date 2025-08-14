package wal

import (
	"fmt"
	"hash/crc32"
	mdl "hunddb/model"
	bm "hunddb/structures/block_manager"
	"math"
	"os"
)

// TODO: Create tests for WAL when all dependencies are complete.
// TODO: These const values should be imported from user config
const (
	BLOCK_SIZE = 4096
	LOG_SIZE   = 16
)

// WAL represents a Write-Ahead Log implementation for database persistence.
// It manages record writing, fragmentation across blocks, and crash recovery.
type WAL struct {
	lastBlock          []byte // Current block being written to
	offsetInBlock      uint16 // Current write position within the block
	blocksInCurrentLog uint16 // Number of blocks written in current log
	lastLogIndex       uint32 // Current log segment index
	dirPath            string // Directory path for log files
}

// NewWAL creates a new WAL instance with the specified directory path and starting log index.
// TODO: consult with Milos about this, shouldn't system set the filePath and log number is known? What about reading the last block if system closes?
// dirPath: the directory where log files will be stored.
// logIndex: the starting log index for this WAL instance.
func NewWAL(dirPath string, logIndex uint32) *WAL {
	return &WAL{
		lastBlock:          make([]byte, BLOCK_SIZE),
		offsetInBlock:      0,
		blocksInCurrentLog: 0,
		lastLogIndex:       logIndex,
		dirPath:            dirPath,
	}
}

// WriteRecord writes a WAL record to the log, handling both complete and fragmented records.
func (wal *WAL) WriteRecord(record *mdl.Record) error {
	serializedRecord := record.Serialize()
	spaceNeeded := HEADER_TOTAL_SIZE + len(serializedRecord)

	// Checks if there is enough space left in the block.
	if int(BLOCK_SIZE)-int(wal.offsetInBlock) < spaceNeeded {
		err := wal.flushCurrentAndMakeNewBlock()
		if err != nil {
			return err
		}
		// If the record is larger than a whole block, fragment it
		if spaceNeeded > int(BLOCK_SIZE) {
			return wal.writeFragmentedRecord(serializedRecord)
		}
	}
	return wal.writeToBlock(serializedRecord, FRAGMENT_FULL)
}

// writeFragmentedRecord handles records larger than a single block by splitting them into fragments.
// All fragments for a record are kept within the same log file.
func (wal *WAL) writeFragmentedRecord(serializedRecord []byte) error {
	recordSize := int(BLOCK_SIZE) - HEADER_TOTAL_SIZE
	numberOfFragments := int(math.Ceil(float64(len(serializedRecord)) / float64(recordSize)))

	// Ensure all fragments fit in the current log
	blocksRemaining := int(LOG_SIZE) - int(wal.blocksInCurrentLog)
	if numberOfFragments > blocksRemaining {
		// Force flush current block if it has data and start a new log
		if wal.offsetInBlock > 0 {
			err := wal.flushCurrentAndMakeNewBlock()
			if err != nil {
				return err
			}
		}
		wal.lastLogIndex++
		wal.blocksInCurrentLog = 0
	}

	// Writes the fragments. For example if a record is 1.5 blocks large, the first fragment + it's header
	// will be the size of a single block, the second fragment + it's header will be half the size of a block.
	for i := range numberOfFragments {
		start := i * recordSize
		end := min(start+recordSize, len(serializedRecord))

		fragment := serializedRecord[start:end]
		var fragmentType byte = FRAGMENT_MIDDLE
		switch i {
		case 0:
			fragmentType = FRAGMENT_FIRST
		case numberOfFragments - 1:
			fragmentType = FRAGMENT_LAST
		}
		err := wal.writeToBlock(fragment, fragmentType)
		if err != nil {
			return err
		}
	}
	return nil
}

// writeToBlock writes a record or record fragment to the current block with proper header.
// serializedRecord: the record or record fragment to write.
// fragmentType: the fragment type (FULL, FIRST, MIDDLE, LAST).
func (wal *WAL) writeToBlock(serializedRecord []byte, fragmentType byte) error {
	headerBytes := NewWALHeader(
		CRC32(serializedRecord),
		uint16(len(serializedRecord)),
		fragmentType,
		wal.lastLogIndex,
	).Serialize()

	totalSize := HEADER_TOTAL_SIZE + len(serializedRecord)

	if int(wal.offsetInBlock)+totalSize > int(BLOCK_SIZE) {
		return fmt.Errorf("not enough space in block to write record")
	}

	copy(wal.lastBlock[wal.offsetInBlock:], headerBytes)
	copy(wal.lastBlock[wal.offsetInBlock+HEADER_TOTAL_SIZE:], serializedRecord)

	wal.offsetInBlock += uint16(totalSize)

	// If the block is exactly full, flush it
	if wal.offsetInBlock == BLOCK_SIZE {
		return wal.flushCurrentAndMakeNewBlock()
	}

	return nil
}

// TODO: Agree on the correct path to logs
// flushCurrentAndMakeNewBlock writes the current block to storage and prepares for the next block.
func (wal *WAL) flushCurrentAndMakeNewBlock() error {
	err := bm.GetBlockManager().WriteBlock(mdl.BlockLocation{
		FilePath:   fmt.Sprintf("%s/wal_%d.log", wal.dirPath, wal.lastLogIndex),
		BlockIndex: uint64(wal.blocksInCurrentLog),
	}, wal.lastBlock)
	if err != nil {
		return fmt.Errorf("failed to write block to disk: %w", err)
	}

	wal.lastBlock = make([]byte, BLOCK_SIZE)
	wal.offsetInBlock = 0
	wal.blocksInCurrentLog++

	// Start new log if current log is full
	if wal.blocksInCurrentLog >= LOG_SIZE {
		wal.lastLogIndex++
		wal.blocksInCurrentLog = 0
	}
	return nil
}

// CRC32 calculates CRC32 checksum over a byte array.
// Used for fragment-level integrity checking in WALHeader.
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// Close flushes any remaining data and closes the WAL.
// Should be called during graceful shutdown to avoid data loss.
func (wal *WAL) Close() error {
	if wal.offsetInBlock > 0 {
		return wal.flushCurrentAndMakeNewBlock()
	}
	return nil
}

// DeleteOldLogs deletes all log files with numbers below the given low watermark.
// lowWatermark: the log number below which all logs should be deleted.
func (wal *WAL) DeleteOldLogs(lowWatermark uint32) error {
	if lowWatermark <= 0 {
		return nil
	}

	for logNum := uint32(0); logNum < lowWatermark; logNum++ {
		logFilePath := fmt.Sprintf("%s/wal_%d.log", wal.dirPath, logNum)
		err := os.Remove(logFilePath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete log file %s: %w", logFilePath, err)
		}
	}

	return nil
}

// ReadRecords reads and reconstructs all records from the WAL logs.
// It handles both complete records and fragmented records across multiple blocks.
// TODO: range blocks should be channel based function from block manager
// (its an iterator, just like how in python you have yield keyword)
// that returns blocks from a range with params: (startLog, startBlock, endLog, endBlock)
// WAL should be able read all logs or logs from a specific range
// func (wal *WAL) ReadRecords() ([]mdl.Record, error) {
// 	blocks := make([][]byte, 0)
// 	records := make([]mdl.Record, 0)

// 	fragmentBuffer := make([]byte, 0)
// 	for _, block := range blocks {
// 		offset := 0

// 		for offset < len(block) {
// 			// Check if we hit padding (all zeros)
// 			if block[offset] == 0 {
// 				break // Rest of block is padding
// 			}

// 			// Read header
// 			header := DeserializeWALHeader(block[offset:])
// 			offset += HEADER_TOTAL_SIZE

// 			// Read record
// 			record := block[offset : offset+int(header.Size)]
// 			offset += int(header.Size)

// 			// Verify CRC32 checksum
// 			if header.CRC != CRC32(record) {
// 				return nil, fmt.Errorf("corrupted record: CRC mismatch")
// 			}

// 			switch header.Type {
// 			case FRAGMENT_FULL:
// 				record := mdl.Deserialize(record)
// 				record := mdl.NewRecord(string(record.Key), record.Value, record.Timestamp, record.Tombstone)
// 				records = append(records, *record)

// 			case FRAGMENT_FIRST, FRAGMENT_MIDDLE:
// 				// Start new fragmented record or continue building it
// 				fragmentBuffer = append(fragmentBuffer, record...)

// 			case FRAGMENT_LAST:
// 				// Complete fragmented record
// 				fragmentBuffer = append(fragmentBuffer, record...)
// 				record := Deserialize(fragmentBuffer)
// 				record := mdl.NewRecord(string(record.Key), record.Value, record.Timestamp, record.Tombstone)
// 				records = append(records, *record)

// 				fragmentBuffer = fragmentBuffer[:0]
// 			}
// 		}
// 	}
// 	return records, nil
// }
