package wal

import (
	"bytes"
	"fmt"
	bm "hunddb/lsm/block_manager"
	block_location "hunddb/model/block_location"
	record "hunddb/model/record"
	crc "hunddb/utils/crc"
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
	offsetInBlock      uint64 // Current write position within the block
	blocksInCurrentLog uint64 // Number of blocks written in current log
	firstLogIndex      uint64 // First log segment index
	lastLogIndex       uint64 // Last log segment index
	logSize            uint64 // Maximum number of blocks per log file
	logsPath           string // Path to logs directory
}

// BuildWAL creates a new WAL instance with the specified directory path and starting log index,
// or initializes from existing logs if present.
func BuildWAL() (*WAL, error) {
	dirPath := "hunddb/lsm/wal/logs"
	metadataPath := fmt.Sprintf("%s/wal_metadata.json", dirPath)

	wal := &WAL{
		lastBlock:          make([]byte, BLOCK_SIZE),
		offsetInBlock:      crc.CRC_SIZE,
		blocksInCurrentLog: 0,
		firstLogIndex:      1,
		lastLogIndex:       1,
		logSize:            LOG_SIZE,
		logsPath:           dirPath,
	}
	return wal, err
}

// WriteRecord writes a WAL record to the log, handling both complete and fragmented records.
func (wal *WAL) WriteRecord(record *record.Record) error {
	payload := record.Serialize()
	spaceNeeded := HEADER_TOTAL_SIZE + len(payload)

	// Checks if there is enough space left in the block.
	if int(BLOCK_SIZE-wal.offsetInBlock) < spaceNeeded {
		err := wal.flushCurrentAndMakeNewBlock()
		if err != nil {
			return err
		}
		// If the record is larger than a whole block, fragment it
		if spaceNeeded > int(BLOCK_SIZE) {
			return wal.writeFragmentedRecord(payload)
		}
	}
	return wal.writeToBlock(payload, FRAGMENT_FULL)
}

// writeFragmentedRecord handles records larger than a single block by splitting them into fragments.
// All fragments for a record are kept within the same log file.
func (wal *WAL) writeFragmentedRecord(payload []byte) error {
	maxPayloadSize := int(BLOCK_SIZE) - HEADER_TOTAL_SIZE - crc.CRC_SIZE
	numberOfFragments := int(math.Ceil(float64(len(payload)) / float64(maxPayloadSize)))

	// Ensure all fragments fit in the current log
	if numberOfFragments > int(LOG_SIZE-wal.blocksInCurrentLog) {
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

	// Writes the fragments. Each fragment takes up a full block.
	// For example if a record is 1.5 blocks large, the first fragment
	// will be the size of maxPayloadSize, the second fragment will be the remaining data.
	payloadOffset := 0
	for i := range numberOfFragments {
		payloadFragmentSize := min(maxPayloadSize, len(payload)-payloadOffset)
		payloadFragment := payload[payloadOffset : payloadOffset+payloadFragmentSize]
		payloadOffset += payloadFragmentSize

		var fragmentType byte = FRAGMENT_MIDDLE
		switch i {
		case 0:
			fragmentType = FRAGMENT_FIRST
		case numberOfFragments - 1:
			fragmentType = FRAGMENT_LAST
		}
		err := wal.writeToBlock(payloadFragment, fragmentType)
		if err != nil {
			return err
		}
	}
	return nil
}

// writeToBlock writes a record or record fragment to the current block.
// fragmentType: the fragment type (FULL, FIRST, MIDDLE, LAST).
func (wal *WAL) writeToBlock(payload []byte, fragmentType byte) error {
	header := NewWALHeader(
		uint64(len(payload)),
		fragmentType,
		wal.lastLogIndex,
	).Serialize()

	totalSize := HEADER_TOTAL_SIZE + len(payload)

	if int(wal.offsetInBlock)+totalSize > int(BLOCK_SIZE) {
		return fmt.Errorf("not enough space in block to write record")
	}

	copy(wal.lastBlock[wal.offsetInBlock:], header)
	copy(wal.lastBlock[wal.offsetInBlock+HEADER_TOTAL_SIZE:], payload)

	wal.offsetInBlock += uint64(totalSize)

	// If the block is exactly full, flush it
	if wal.offsetInBlock == BLOCK_SIZE {
		return wal.flushCurrentAndMakeNewBlock()
	}

	return nil
}

// flushCurrentAndMakeNewBlock writes the current block to storage and prepares for the next block.
func (wal *WAL) flushCurrentAndMakeNewBlock() error {
	wal.lastBlock = crc.AddCRCToBlockData(wal.lastBlock)
	err := bm.GetBlockManager().WriteBlock(block_location.BlockLocation{
		FilePath:   fmt.Sprintf("%s/wal_%d.log", wal.logsPath, wal.lastLogIndex),
		BlockIndex: wal.blocksInCurrentLog,
	}, wal.lastBlock)
	if err != nil {
		return fmt.Errorf("failed to write block to disk: %w", err)
	}

	wal.lastBlock = make([]byte, BLOCK_SIZE)
	wal.offsetInBlock = crc.CRC_SIZE
	wal.blocksInCurrentLog++

	// Start new log if current log is full
	if wal.blocksInCurrentLog >= LOG_SIZE {
		wal.lastLogIndex++
		wal.blocksInCurrentLog = 0
	}
	return nil
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
func (wal *WAL) DeleteOldLogs(lowWatermark uint64) error {
	if lowWatermark <= 0 {
		return nil
	}
	for logNum := wal.firstLogIndex; logNum < lowWatermark; logNum++ {
		logFilePath := fmt.Sprintf("%s/wal_%d.log", wal.logsPath, logNum)
		err := os.Remove(logFilePath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to delete log file %s: %w", logFilePath, err)
		}
	}

	return nil
}

// ReconstructMemtable reads and reconstructs all records from the WAL logs.
// It handles both complete records and fragmented records across multiple blocks.
// This function is designed to be called during database recovery/restart.
//
// TODO: When memtable is implemented, pass memtable reference as parameter:
// TODO: Figure out log range specification (startLogIndex, endLogIndex), probably will need metadata file for wal
func (wal *WAL) ReconstructMemtable() error {
	blockManager := bm.GetBlockManager()
	fragmentBuffer := make([]byte, 0)

	for logIndex := wal.firstLogIndex; logIndex <= wal.lastLogIndex; logIndex++ {
		for blockIndex := range uint64(LOG_SIZE) {
			location := block_location.BlockLocation{
				FilePath:   fmt.Sprintf("%s/wal_%d.log", wal.logsPath, logIndex),
				BlockIndex: blockIndex,
			}

			block, err := blockManager.ReadBlock(location)
			if err != nil {
				// If we can't read a block, it might not exist or be incomplete
				continue
			}

			err = wal.processBlock(block, &fragmentBuffer)
			if err != nil {
				return fmt.Errorf("failed to process block %s:%d: %w", location.FilePath, location.BlockIndex, err)
			}
		}
	}

	return nil
}

// processBlock processes a single WAL block and reconstructs records from it
func (wal *WAL) processBlock(block []byte, fragmentBuffer *[]byte) error {
	offset := 0

	for offset < len(block) {
		// Check if the rest of the block is padding
		remainingBytes := block[offset:]
		paddingBytes := make([]byte, len(remainingBytes))
		if bytes.Equal(remainingBytes, paddingBytes) {
			break
		}

		header := DeserializeWALHeader(block[offset:])
		offset += HEADER_TOTAL_SIZE
		payload := block[offset : offset+int(header.Size)]
		offset += int(header.Size)

		err := crc.CheckBlockIntegrity(block)
		if err != nil {
			return fmt.Errorf("CRC check failed for record fragment: %w", err)
		}

		switch header.Type {
		case FRAGMENT_FULL:
			record := record.Deserialize(payload)
			// TODO: Insert into memtable when implemented:
			_ = record // Suppress unused variable warning, remove when used

		case FRAGMENT_FIRST, FRAGMENT_MIDDLE:
			*fragmentBuffer = append(*fragmentBuffer, payload...)

		case FRAGMENT_LAST:
			*fragmentBuffer = append(*fragmentBuffer, payload...)
			record := record.Deserialize(*fragmentBuffer)
			// TODO: Insert into memtable when implemented:
			_ = record // Suppress unused variable warning, remove when used

			// Clear fragment buffer for next fragmented record
			*fragmentBuffer = (*fragmentBuffer)[:0]

		default:
			return fmt.Errorf("unknown fragment type: %d", header.Type)
		}
	}

	return nil
}
