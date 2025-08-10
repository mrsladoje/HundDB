package wal

import (
	"fmt"
	"hash/crc32"
	"math"
	"os"
)

// TODO: Create tests for WAL when all dependencies are complete.
// TODO: These const values should be imported from user config
const (
	BLOCKSIZE = 1024
	LOGSIZE   = 16
)

// WAL represents a Write-Ahead Log implementation for database persistence.
// It manages record writing, fragmentation, block management, and crash recovery.
type WAL struct {
	currentBlock             []byte // Current block being written to
	currentOffset            uint16 // Current position within the block
	currentBlocksInLogAmount uint16 // Number of blocks written in current log
	currentLogNumber         uint32 // Current log segment number
	filePath                 string // Base path for log files
}

// NewWAL creates a new WAL instance with the specified file path and starting log number.
// filePath: the directory where log files will be stored.
// logNumber: the starting log number for this WAL instance.
func NewWAL(filePath string, logNumber uint32) *WAL {
	return &WAL{
		currentBlock:             make([]byte, BLOCKSIZE),
		currentOffset:            0,
		currentBlocksInLogAmount: 1,
		currentLogNumber:         logNumber,
		filePath:                 filePath,
	}
}

// WritePut writes a PUT operation to the WAL.
// key: the key being inserted or updated.
// value: the value associated with the key.
func (wal *WAL) WritePut(key, value []byte) error {
	record := NewWALPayload(key, value, false)
	return wal.WriteRecord(record)
}

// WriteDelete writes a DELETE operation to the WAL.
// key: the key being deleted.
func (wal *WAL) WriteDelete(key []byte) error {
	record := NewWALPayload(key, nil, true)
	return wal.WriteRecord(record)
}

// WriteRecord writes a WAL record to the log, handling both complete and fragmented records.
// payload: the WAL payload to be written.
func (wal *WAL) WriteRecord(payload *WALPayload) error {
	serializedRecord := payload.Serialize()
	spaceNeeded := HEADER_TOTAL_SIZE + len(serializedRecord)

	// Checks if there is enough space left in the block.
	if int(BLOCKSIZE)-int(wal.currentOffset) < spaceNeeded {
		wal.AddPaddingAndMakeNewBlock()
		// If the record is bigger than a whole block, then it's fragmented.
		if spaceNeeded > int(BLOCKSIZE) {
			return wal.WriteFragmentedRecord(payload)
		}
	}

	return wal.WriteToBlock(payload.Serialize(), FRAGMENT_FULL)
}

// WriteFragmentedRecord handles writing records that are larger than a single block.
// It splits the record into fragments and ensures all fragments stay within the same log.
// payload: the WAL payload to be fragmented and written.
func (wal *WAL) WriteFragmentedRecord(payload *WALPayload) error {
	serializedPayload := payload.Serialize()
	payloadSize := int(BLOCKSIZE) - HEADER_TOTAL_SIZE
	// Calculates into how many fragments the payload needs to be split, taking the header size into consideration.
	numberOfFragments := int(math.Ceil(float64(len(serializedPayload)) / float64(payloadSize)))

	// Ensures the fragments are in a single log.
	blocksRemaining := int(LOGSIZE) - int(wal.currentBlocksInLogAmount)
	if numberOfFragments > blocksRemaining {
		err := wal.createNewLog()
		if err != nil {
			return err
		}
	}

	// Writes the fragments. For example if a record is 1.5 blocks large, the first fragment + it's header
	// will be the size of a single block, the second fragment + it's header will be half the size of a block.
	for i := range numberOfFragments {
		start := i * payloadSize
		end := min(start+payloadSize, len(serializedPayload))

		fragment := serializedPayload[start:end]
		var payloadType byte = FRAGMENT_MIDDLE

		if i == 0 {
			payloadType = FRAGMENT_FIRST
		}
		if i == numberOfFragments-1 {
			payloadType = FRAGMENT_LAST
		}
		err := wal.WriteToBlock(fragment, payloadType)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddPaddingAndMakeNewBlock fills the remaining space in the current block with zeros
// and flushes it to prepare for a new block.
func (wal *WAL) AddPaddingAndMakeNewBlock() error {
	for i := int(wal.currentOffset); i < int(BLOCKSIZE); i++ {
		wal.currentBlock[i] = 0
	}

	if wal.flushCurrentBlock() != nil {
		return fmt.Errorf("failed to flush block")
	}
	return nil
}

// flushCurrentBlock writes the current block to storage and prepares for the next block.
// TODO: Add flushing of block through BlockManager
func (wal *WAL) flushCurrentBlock() error {
	wal.currentBlock = make([]byte, BLOCKSIZE)
	wal.currentOffset = 0
	// call BlockManager to write block
	if wal.currentBlocksInLogAmount == LOGSIZE {
		wal.createNewLog()
	} else {
		wal.currentBlocksInLogAmount += 1
	}
	return nil
}

// WriteToBlock writes a record or record fragment to the current block with proper header.
// serializedPayload: the fragment payload to write.
// payloadType: the fragment type (FULL, FIRST, MIDDLE, LAST).
func (wal *WAL) WriteToBlock(serializedPayload []byte, payloadType byte) error {
	header := NewWALHeader(
		CRC32(serializedPayload),
		uint16(len(serializedPayload)),
		payloadType,
		wal.currentLogNumber,
	)

	headerBytes := header.Serialize()
	totalSize := HEADER_TOTAL_SIZE + len(serializedPayload)

	if int(wal.currentOffset)+totalSize > int(BLOCKSIZE) {
		return fmt.Errorf("not enough space in block to write record")
	}
	copy(wal.currentBlock[wal.currentOffset:], headerBytes)
	copy(wal.currentBlock[wal.currentOffset+HEADER_TOTAL_SIZE:], serializedPayload)

	wal.currentOffset += uint16(totalSize)
	// If the block is filled out to the brim - it's flushed.
	if wal.currentOffset == BLOCKSIZE {
		return wal.flushCurrentBlock()
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
	if wal.currentOffset > 0 {
		return wal.AddPaddingAndMakeNewBlock()
	}
	return nil
}

// createNewLog creates a new log file and resets block counters.
// TODO: When BlockManager is implemnted, add the function create a new log file.
func (wal *WAL) createNewLog() error {
	err := wal.AddPaddingAndMakeNewBlock()
	if err != nil {
		return err
	}
	wal.currentLogNumber++
	wal.currentBlocksInLogAmount = 1
	// Call BlockManager to create new log file.

	return nil
}

// DeleteOldLogs deletes all log files with numbers below the given low watermark.
// lowWatermark: the log number below which all logs should be deleted.
func (wal *WAL) DeleteOldLogs(lowWatermark uint32) error {
	if lowWatermark <= 0 {
		return nil
	}

	for logNum := uint32(0); logNum < lowWatermark; logNum++ {
		logFilePath := fmt.Sprintf("%s/%d.log", wal.filePath, logNum)
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
func (wal *WAL) ReadRecords() ([]Record, error) {
	blocks := make([][]byte, 0)
	records := make([]Record, 0)

	fragmentBuffer := make([]byte, 0)
	for _, block := range blocks {
		offset := 0

		for offset < len(block) {
			// Check if we hit padding (all zeros)
			if block[offset] == 0 {
				break // Rest of block is padding
			}
			// Read header
			header := DeserializeWALHeader(block[offset:])
			offset += HEADER_TOTAL_SIZE

			// Read payload
			payload := block[offset : offset+int(header.Size)]
			offset += int(header.Size)

			// Verify CRC32 checksum
			if header.CRC != CRC32(payload) {
				return nil, fmt.Errorf("corrupted record: CRC mismatch")
			}

			switch header.Type {
			case FRAGMENT_FULL:
				payload := Deserialize(payload)
				record := NewRecord(payload.Key, payload.Key, payload.Timestamp, payload.Tombstone)
				records = append(records, *record)

			case FRAGMENT_FIRST, FRAGMENT_MIDDLE:
				// Start new fragmented record or continue building it
				fragmentBuffer = append(fragmentBuffer, payload...)

			case FRAGMENT_LAST:
				// Complete fragmented record
				fragmentBuffer = append(fragmentBuffer, payload...)
				payload := Deserialize(fragmentBuffer)
				record := NewRecord(payload.Key, payload.Value, payload.Timestamp, payload.Tombstone)
				records = append(records, *record)

				fragmentBuffer = fragmentBuffer[:0]
			}
		}
	}
	return records, nil
}
