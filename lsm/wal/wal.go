package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	bm "hunddb/lsm/block_manager"
	memtable "hunddb/lsm/memtable"
	block_location "hunddb/model/block_location"
	record "hunddb/model/record"
	byte_util "hunddb/utils/byte_util"
	"hunddb/utils/config"
	crc "hunddb/utils/crc"
	"math"
	"os"
	"regexp"
	"strconv"
)

// Configuration variables loaded from config file - no hardcoded defaults
var (
	BLOCK_SIZE uint64
	LOG_SIZE   uint64
)

// init loads WAL configuration from config file
func init() {
	cfg := config.GetConfig()
	// Always use config - no fallbacks here
	BLOCK_SIZE = cfg.BlockManager.BlockSize
	LOG_SIZE = cfg.WAL.LogSize
}

// WAL represents a Write-Ahead Log implementation for database persistence.
// It manages record writing, fragmentation across blocks, and crash recovery.
// It guarantees that all blocks will be written to durable storage, thus ensuring durability.
// With only exception being the last block that is being written to, due to performance reasons.
// Only happens if the program crashes, supporting graceful exit.
// That is a balance between performance and durability that is needed.
type WAL struct {
	lastBlock              []byte // Current block being written to
	offsetInBlock          uint64 // Current write position within the block
	blocksWrittenInLastLog uint64 // Number of blocks written in last log
	firstLogIndex          uint64 // First log segment index
	lastLogIndex           uint64 // Last log segment index
	logSize                uint64 // Maximum number of blocks per log file
	logsPath               string // Path to logs directory
}

// BuildWAL creates a new WAL instance with the specified directory path and starting log index,
// or initializes from existing logs if present.
func BuildWAL() (*WAL, error) {
	wal := &WAL{
		lastBlock:              make([]byte, BLOCK_SIZE),
		offsetInBlock:          crc.CRC_SIZE,
		blocksWrittenInLastLog: 0,
		firstLogIndex:          1,
		lastLogIndex:           1,
		logSize:                LOG_SIZE,
		logsPath:               "hunddb/lsm/wal/logs",
	}
	err := wal.reloadWAL()
	if err != nil {
		return nil, fmt.Errorf("failed to reload WAL: %w", err)
	}

	// If Overwrite metadata file to indicate unclean shutdown
	// This is done because if the program crashes, we want to know that it was not a graceful exit
	// and we need to recover the offset from the metadata file
	// If the program exits gracefully, we will update the metadata file to indicate a clean shutdown
	// and the offset will be restored from there on next startup
	metadataFile, err := os.OpenFile("hunddb/lsm/wal/metadata.bin", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer metadataFile.Close()

	data := make([]byte, 9)
	data[0] = byte_util.BoolToByte(false)
	_, err = metadataFile.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to write metadata file: %w", err)
	}

	return wal, err
}

// reloadWAL loads WAL metadata from a file to restore state after a crash or restart.
func (wal *WAL) reloadWAL() error {
	logs, err := os.ReadDir(wal.logsPath)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	if len(logs) == 0 {
		fmt.Println("WAL directory is empty, starting fresh")
		return nil
	}

	// Regex for wal_{number}.log
	re := regexp.MustCompile(`^wal_(\d+)\.log$`)

	minLogIndex := math.MaxInt32
	maxLogIndex := -1

	for _, log := range logs {
		name := log.Name()
		matches := re.FindStringSubmatch(name)
		if matches != nil {
			num, err := strconv.Atoi(matches[1])
			if err == nil {
				if num < minLogIndex {
					minLogIndex = num
				}
				if num > maxLogIndex {
					maxLogIndex = num
				}
			}
		}
	}

	if maxLogIndex == -1 {
		fmt.Println("No WAL logs found, starting fresh")
		return nil
	}

	wal.firstLogIndex = uint64(minLogIndex)
	wal.lastLogIndex = uint64(maxLogIndex)

	lastFile := fmt.Sprintf("%s/wal_%d.log", wal.logsPath, maxLogIndex)
	f, err := os.Open(lastFile)
	if err != nil {
		return fmt.Errorf("failed to open last WAL file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat last WAL file: %w", err)
	}
	size := info.Size()

	// How many blocks are written in the last log
	wal.blocksWrittenInLastLog = uint64(uint64(size) / BLOCK_SIZE)

	f, err = os.Open("hunddb/lsm/wal/metadata.bin")
	if err != nil {
		return fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer f.Close()
	data := make([]byte, 9)
	_, err = f.Read(data)
	if err != nil {
		return fmt.Errorf("failed to read metadata file: %w", err)
	}
	madeGracefulExit := byte_util.ByteToBool(data[0])
	if madeGracefulExit {
		wal.offsetInBlock = binary.LittleEndian.Uint64(data[1:])
	}

	return nil
}

// WriteRecord writes a WAL record to the log, handling both complete and fragmented records.
func (wal *WAL) WriteRecord(record *record.Record) (uint64, error) {
	payload := record.Serialize()
	spaceNeeded := HEADER_TOTAL_SIZE + len(payload)

	// Checks if there is enough space left in the block.
	if int(BLOCK_SIZE-wal.offsetInBlock) < spaceNeeded {
		err := wal.flushBlock()
		if err != nil {
			return 0, err
		}
		wal.makeNewBlock()

		// If the record is larger than a whole block, fragment it
		if spaceNeeded > int(BLOCK_SIZE) {
			return wal.writeFragmentedRecord(payload)
		}
	}
	return wal.lastLogIndex, wal.writeToBlock(payload, FRAGMENT_FULL)
}

// writeFragmentedRecord handles records larger than a single block by splitting them into fragments.
// All fragments for a record are kept within the same log file.
func (wal *WAL) writeFragmentedRecord(payload []byte) (uint64, error) {
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
			return 0, err
		}
	}
	return wal.lastLogIndex, nil
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
		BlockIndex: wal.blocksWrittenInLastLog,
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

	// Update metadata file to indicate graceful shutdown
	metadataFile, err := os.OpenFile("hunddb/lsm/wal/metadata.bin", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer metadataFile.Close()

	data := make([]byte, 9)
	data[0] = byte_util.BoolToByte(true) // Graceful exit flag
	binary.LittleEndian.PutUint64(data[1:], wal.offsetInBlock)

	_, err = metadataFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write metadata file: %w", err)
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
			endBlockIndex = wal.blocksWrittenInLastLog
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
