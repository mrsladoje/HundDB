package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	bm "hunddb/lsm/block_manager"
	memtable "hunddb/lsm/memtable"
	block_location "hunddb/model/block_location"
	record "hunddb/model/record"
	crc "hunddb/utils/crc"
	"math"
	"os"
)

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
	wal := &WAL{
		lastBlock:          make([]byte, BLOCK_SIZE),
		offsetInBlock:      crc.CRC_SIZE,
		blocksInCurrentLog: 0,
		firstLogIndex:      1,
		lastLogIndex:       1,
		logSize:            LOG_SIZE,
		logsPath:           dirPath,
	}
	err := wal.reloadMetadata()
	return wal, err
}

// WriteRecord writes a WAL record to the log, handling both complete and fragmented records.
func (wal *WAL) WriteRecord(record *record.Record) error {
	payload := record.Serialize()
	spaceNeeded := HEADER_TOTAL_SIZE + len(payload)

	// Checks if there is enough space left in the block.
	if int(BLOCK_SIZE-wal.offsetInBlock) < spaceNeeded {
		err := wal.flushBlock()
		if err != nil {
			return err
		}
		wal.makeNewBlock()

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
		err := wal.flushBlock()
		if err != nil {
			return err
		}
		wal.makeNewBlock()
	}

	return nil
}

// flushBlock writes the current block to storage and prepares for the next block.
func (wal *WAL) flushBlock() error {
	wal.lastBlock = crc.AddCRCToBlockData(wal.lastBlock)
	err := bm.GetBlockManager().WriteBlock(block_location.BlockLocation{
		FilePath:   fmt.Sprintf("%s/wal_%d.log", wal.logsPath, wal.lastLogIndex),
		BlockIndex: wal.blocksInCurrentLog,
	}, wal.lastBlock)
	if err != nil {
		return fmt.Errorf("failed to write block to disk: %w", err)
	}
	wal.blocksWrittenInLastLog++
	return nil
	}

// makeNewBlock initializes a new block for writing and updates WAL state accordingly.
func (wal *WAL) makeNewBlock() {
	wal.lastBlock = make([]byte, BLOCK_SIZE)
	wal.offsetInBlock = crc.CRC_SIZE

	// Start new log if current log is full
	if wal.blocksWrittenInLastLog >= LOG_SIZE {
		wal.lastLogIndex++
		wal.blocksWrittenInLastLog = 0
	}
}

// Close flushes any remaining data and closes the WAL.
// Should be called during graceful shutdown to avoid data loss.
func (wal *WAL) Close() error {
	err := wal.flushBlock()
		if err != nil {
			return fmt.Errorf("failed to flush current block: %w", err)
		}
	}
	err := syncMetadata(wal)
	if err != nil {
		return fmt.Errorf("failed to write WAL metadata on close: %w", err)
	}
	return nil
}

func (wal *WAL) reloadMetadata() error {
	metadataPath := fmt.Sprintf("%s/wal_metadata.bin", wal.logsPath)
	file, err := os.Open(metadataPath)
	if err == os.ErrNotExist {
		fmt.Println("WAL metadata file does not exist, starting fresh WAL")
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to open WAL metadata file: %w", err)
	}
	defer file.Close()
	data := make([]byte, 32)
	_, err = file.Read(data)
	if err != nil {
		return fmt.Errorf("failed to read WAL metadata file: %w", err)
	}
	wal.offsetInBlock = binary.LittleEndian.Uint64(data[0:8])
	wal.blocksInCurrentLog = binary.LittleEndian.Uint64(data[8:16])
	wal.firstLogIndex = binary.LittleEndian.Uint64(data[16:24])
	wal.lastLogIndex = binary.LittleEndian.Uint64(data[24:32])

	wal.lastBlock, err = bm.GetBlockManager().ReadBlock(block_location.BlockLocation{
		FilePath:   fmt.Sprintf("%s/wal_%d.log", wal.logsPath, wal.lastLogIndex),
		BlockIndex: uint64(wal.blocksInCurrentLog - 1),
	})
	if err != nil {
		err = fmt.Errorf("WAL data could not be recovered")
	}
	return err
}

// syncMetadata writes the WAL metadata to a file for recovery purposes.
func syncMetadata(wal *WAL) error {
	metadataPath := fmt.Sprintf("%s/wal_metadata.bin", wal.logsPath)
	file, err := os.Create(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to create WAL metadata file: %w", err)
	}
	defer file.Close()
	data := make([]byte, 32)
	binary.LittleEndian.PutUint64(data[0:8], wal.offsetInBlock)
	binary.LittleEndian.PutUint64(data[8:16], wal.blocksInCurrentLog)
	binary.LittleEndian.PutUint64(data[16:24], wal.firstLogIndex)
	binary.LittleEndian.PutUint64(data[24:32], wal.lastLogIndex)
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to serialize WAL metadata file: %w", err)
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

// WalPosition tracks the current position during WAL recovery.
type WalPosition struct {
	LogIndex   uint64 // Current log file being processed
	BlockIndex uint64 // Current block within the log
	Offset     uint64 // Current byte offset within the block
}

// RecoverMemtables replays WAL logs to reconstruct the state of the provided memtables.
// It processes logs sequentially, handling both complete and fragmented records.
// Updates the position as it processes records across multiple logs and blocks.
func (a *WAL) RecoverMemtables(memtables []*memtable.MemTable) error {
	position := &WalPosition{
		LogIndex:   a.firstLogIndex,
		BlockIndex: 0,
		Offset:     crc.CRC_SIZE,
	}

	for _, memtable := range memtables {
		err := a.recoverMemtable(memtable, position)
		if err != nil {
			return fmt.Errorf("failed to recover memtable: %w", err)
		}

		// If the memtable is not full after its recovery, that means we have processed all logs
		if !memtable.IsFull() {
			return nil
		}
	}
	return nil
}

// recoverMemtable reads and reconstructs records from the WAL logs starting from the given position.
// It handles both complete records and fragmented records across multiple blocks.
// Updates the position as it processes records.
func (wal *WAL) recoverMemtable(memtable *memtable.MemTable, position *WalPosition) error {
	blockManager := bm.GetBlockManager()
	fragmentBuffer := make([]byte, 0, BLOCK_SIZE)

	for position.LogIndex <= wal.lastLogIndex {
		endBlockIndex := wal.logSize
		if position.LogIndex == wal.lastLogIndex {
			endBlockIndex = wal.blocksInCurrentLog
		}

		for position.BlockIndex < endBlockIndex {
			location := block_location.BlockLocation{
				FilePath:   fmt.Sprintf("%s/wal_%d.log", wal.logsPath, position.LogIndex),
				BlockIndex: position.BlockIndex,
			}

			block, err := blockManager.ReadBlock(location)
			if err != nil {
				return fmt.Errorf("failed to read block %s:%d: %w", location.FilePath, location.BlockIndex, err)
			}

			err = crc.CheckBlockIntegrity(block)
			if err != nil {
				return fmt.Errorf("CRC failed %s:%d: %w", location.FilePath, location.BlockIndex, err)
			}

			memtableFull, err := wal.processBlockForRecovery(block, &fragmentBuffer, memtable, position)
			if err != nil {
				return fmt.Errorf("failed to process block %s:%d: %w", location.FilePath, location.BlockIndex, err)
			}
			position.BlockIndex++
			position.Offset = crc.CRC_SIZE

			if memtableFull {
				return nil
			}
		}

		// Move to next log
		position.LogIndex++
		position.BlockIndex = 0
	}

	return nil
}

// processBlockForRecovery processes a single WAL block and reconstructs records from it.
// Updates the position as it processes records within the block.
// Returns true if the memtable becomes full during processing.
func (wal *WAL) processBlockForRecovery(block []byte, fragmentBuffer *[]byte, memtable *memtable.MemTable, position *WalPosition) (bool, error) {
	offset := int(position.Offset)

	for offset < len(block) {
		// Check if rest of the block is padding
		remainingBytes := block[offset:]
		paddingBytes := make([]byte, len(remainingBytes))
		if bytes.Equal(remainingBytes, paddingBytes) {
			*fragmentBuffer = (*fragmentBuffer)[:0]
			break
		}

		header := DeserializeWALHeader(block[offset:])
		offset += HEADER_TOTAL_SIZE
		payload := block[offset : offset+int(header.PayloadSize)]
		offset += int(header.PayloadSize)
		position.Offset = uint64(offset)

		switch header.Type {
		case FRAGMENT_FULL:
			record := record.Deserialize(payload)
			memtable.Put(record)
			if memtable.IsFull() {
				return true, nil
			}

		case FRAGMENT_FIRST, FRAGMENT_MIDDLE:
			*fragmentBuffer = append(*fragmentBuffer, payload...)

		case FRAGMENT_LAST:
			*fragmentBuffer = append(*fragmentBuffer, payload...)
			record := record.Deserialize(*fragmentBuffer)
			*fragmentBuffer = (*fragmentBuffer)[:0]
			memtable.Put(record)
			if memtable.IsFull() {
				return true, nil
			}

		default:
			return false, fmt.Errorf("unknown fragment type: %d", header.Type)
		}
	}

	return false, nil
}
