package lsm

import (
	"encoding/binary"
	"fmt"
	"hunddb/lsm/block_manager"
	cache "hunddb/lsm/cache"
	memtable "hunddb/lsm/memtable"
	"hunddb/lsm/sstable"
	wal "hunddb/lsm/wal"
	model "hunddb/model/record"
	"hunddb/utils/config"
	"os"
	"time"
)

/*
LSM represents a Log-Structured Merge Tree
*/
type LSM struct {
	// Each level holds the indexes of its SSTables
	levels    [][]int
	memtables []*memtable.MemTable
	wal       *wal.WAL
	cache     *cache.ReadPathCache

	// Configuration attributes - loaded from config file
	maxLevels         uint64
	maxTablesPerLevel uint64
	maxMemtables      uint64
	compactionType    string
	lsmPath          string
	crcSize          uint64

	// Flag to indicate if previous data was lost during loading
	DataLost bool
}

/*
Serialize the LSM parts that need to be persisted (the levels and their SSTable indexes).
*/
func (lsm *LSM) serialize() []byte {

	stringifiedLevels := ""

	for i, level := range lsm.levels {
		stringifiedLevels += fmt.Sprintf("%d[", i)
		for j, tblIndex := range level {
			stringifiedLevels += fmt.Sprintf("%d", tblIndex)
			if j < len(level)-1 {
				stringifiedLevels += ","
			}
		}
		stringifiedLevels += "]"
	}

	finalBytes := make([]byte, 8+len(stringifiedLevels))
	binary.LittleEndian.PutUint64(finalBytes[0:8], uint64(len(stringifiedLevels)))
	stringifiedLevelBytes := []byte(stringifiedLevels)
	copy(finalBytes[8:], stringifiedLevelBytes)

	return finalBytes
}

/*
PersistLSM persists the LSM parts that need to be persisted (the levels and their SSTable indexes).
*/
func (lsm *LSM) PersistLSM() error {
	// Get the serialized data
	data := lsm.serialize()

	blockManager := block_manager.GetBlockManager()

	err := blockManager.WriteToDisk(data, lsm.lsmPath, 0)

	return err
}

/*
Deserialize the LSM parts that need to be persisted (the levels and their SSTable indexes).
*/
func (lsm *LSM) deserialize(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("invalid data: empty")
	}

	// Convert bytes directly to string
	stringifiedLevels := string(data)

	// Parse the stringified levels back into the levels slice
	lsm.levels = make([][]int, lsm.maxLevels)

	i := 0
	for i < len(stringifiedLevels) {
		// Find level number
		levelStart := i
		for i < len(stringifiedLevels) && stringifiedLevels[i] != '[' {
			i++
		}
		if i >= len(stringifiedLevels) {
			break
		}

		levelNum := 0
		fmt.Sscanf(stringifiedLevels[levelStart:i], "%d", &levelNum)
		i++ // skip '['

		// Parse table indexes for this level
		var tableIndexes []int
		for i < len(stringifiedLevels) && stringifiedLevels[i] != ']' {
			numStart := i
			for i < len(stringifiedLevels) && stringifiedLevels[i] != ',' && stringifiedLevels[i] != ']' {
				i++
			}

			if i > numStart {
				var tableIndex int
				fmt.Sscanf(stringifiedLevels[numStart:i], "%d", &tableIndex)
				tableIndexes = append(tableIndexes, tableIndex)
			}

			if i < len(stringifiedLevels) && stringifiedLevels[i] == ',' {
				i++ // skip ','
			}
		}

		if i < len(stringifiedLevels) && stringifiedLevels[i] == ']' {
			i++ // skip ']'
		}

		if uint64(levelNum) < lsm.maxLevels {
			lsm.levels[levelNum] = tableIndexes
		}
	}

	return nil
}

/*
LoadLSM loads the LSM from disk, or creates a new one if it doesn't exist.
Always returns an LSM instance. If previous data couldn't be loaded, the DataLost flag will be set to true.
*/
func LoadLSM() *LSM {
	// Create a new LSM instance with config values
	cfg := config.GetConfig()
	lsm := &LSM{
		// Initialize config attributes
		maxLevels:         cfg.LSM.MaxLevels,
		maxTablesPerLevel: cfg.LSM.MaxTablesPerLevel,
		maxMemtables:      cfg.LSM.MaxMemtables,
		compactionType:    cfg.LSM.CompactionType,
		lsmPath:          cfg.LSM.LSMPath,
		crcSize:          cfg.CRC.Size,
		// Initialize slices with config values
		levels:    make([][]int, int(cfg.LSM.MaxLevels)),
		memtables: make([]*memtable.MemTable, 0, int(cfg.LSM.MaxMemtables)),
		wal:       wal.NewWAL("wal.db", 0), // TODO: implement actual logic here
		cache:     cache.NewReadPathCache(),
		DataLost:  false, // Initially assume no data loss
	}

	blockManager := block_manager.GetBlockManager()

	// Check if the file exists using os.Stat
	_, err := os.Stat(lsm.lsmPath)
	if os.IsNotExist(err) {
		// File doesn't exist - this is a fresh start (not data loss)
		firstMemtable, _ := memtable.NewMemtable()
		lsm.memtables = append(lsm.memtables, firstMemtable)
		return lsm
	}

	// File exists, so any errors from here on are considered data corruption

	// Try to read the levels size
	levelsSizeBytes, _, err := blockManager.ReadFromDisk(lsm.lsmPath, 0, 8)
	if err != nil {
		// File exists but can't read size header - corruption
		lsm.DataLost = true
		return lsm
	}

	levelsSize := binary.LittleEndian.Uint64(levelsSizeBytes)

	// Try to read the actual levels data
	data, _, err := blockManager.ReadFromDisk(lsm.lsmPath, 8+uint64(lsm.crcSize), uint64(levelsSize))
	if err != nil {
		// File exists but can't read data - corruption
		lsm.DataLost = true
		return lsm
	}

	// Try to deserialize the data
	err = lsm.deserialize(data)
	if err != nil {
		// File exists but data format is invalid - corruption
		lsm.DataLost = true
		return lsm
	}

	// Successfully loaded previous data
	return lsm
}

/*
IsDataLost returns true if the previous LSM data was lost during loading.
This can happen if the LSM file doesn't exist, is corrupted, or unreadable.
*/
func (lsm *LSM) IsDataLost() bool {
	return lsm.DataLost
}

// Get retrieves a record from the LSM by checking the memtables, cache, and SSTables in order.
func (lsm *LSM) Get(key string) (*model.Record, error, bool) {

	errorEncountered := false

	// 1. Check memtables first
	if record := lsm.checkMemtables(key); record != nil {
		return record, nil, false
	}

	// 2. Check cache
	record, err := lsm.cache.Get(key)
	if err != nil {
		errorEncountered = true
	}
	if err == nil {
		return record, nil, false
	}

	// 3. Check SSTables
	record, errorEncounteredInCheck, errorEncounteredInSSTable := lsm.checkSSTables(key)
	if errorEncounteredInSSTable {
		errorEncountered = true
		err = errorEncounteredInCheck
	}
	if record != nil {
		lsm.cache.Put(key, record)
		return record, nil, false
	}

	return nil, err, errorEncountered
}

/*
checkMemtables checks the memtables in reverse order (newest to oldest) for the given key.
*/
func (lsm *LSM) checkMemtables(key string) *model.Record {
	for i := len(lsm.memtables) - 1; i >= 0; i-- {
		mt := lsm.memtables[i]
		if record := mt.Get(key); record != nil {
			return record
		}
	}
	return nil
}

/*
checkSSTables checks the SSTables in reverse order (newest to oldest) for the given key.
*/
func (lsm *LSM) checkSSTables(key string) (*model.Record, error, bool) {
	errorEncountered := false
	var errorEncounteredInCheck error
	for i := 0; i < len(lsm.levels); i++ {
		levelIndexes := lsm.levels[i]
		for index := len(levelIndexes) - 1; index >= 0; index-- {
			tableIndex := levelIndexes[index]
			record, err := sstable.Get(key, tableIndex)
			if err != nil {
				errorEncountered = true
				errorEncounteredInCheck = err
			}
			if err == nil && record != nil {
				return record, nil, errorEncountered
			}
		}
	}
	return nil, errorEncounteredInCheck, errorEncountered
}

func (lsm *LSM) Put(key string, value []byte) error {

	record := model.NewRecord(key, value, uint64(time.Now().UnixNano()), false)

	// TODO: WAL write ahead logging - I removed it because it was failing for larger values
	// err := lsm.wal.WriteRecord(record)
	// if err != nil {
	// 	return err
	// }

	err := lsm.memtables[len(lsm.memtables)-1].Put(record)
	if err != nil {
		return err
	}

	err = lsm.checkIfToFlush(key)
	if err != nil {
		return err
	}

	lsm.cache.Invalidate(key)

	return nil
}

func (lsm *LSM) Delete(key string) (bool, error) {

	record := model.NewRecord(key, nil, uint64(time.Now().UnixNano()), true)

	// TODO: WAL write ahead logging - I removed it because it was failing for larger values
	// err := lsm.wal.WriteRecord(record)
	// if err != nil {
	// 	return err
	// }

	keyExists := lsm.memtables[len(lsm.memtables)-1].Delete(record)

	err := lsm.checkIfToFlush(key)
	if err != nil {
		return keyExists, err
	}

	lsm.cache.Invalidate(key)

	return keyExists, nil
}

/*
GetNextForPrefix retrieves the next record for a given prefix and start key.
*/
func (lsm *LSM) GetNextForPrefix(prefix string, key string) (*model.Record, error) {

	tomstonedKeys := make([]string, 0)
	nextRecord := lsm.checkMemtablesForPrefixIterate(prefix, key, &tomstonedKeys)
	nextRecordFromSSTable, err := lsm.checkSSTableForPrefixIterate(prefix, key, &tomstonedKeys)

	if err != nil {
		return nil, err
	}

	if nextRecordFromSSTable != nil && (nextRecord == nil || nextRecordFromSSTable.Key < nextRecord.Key) {
		nextRecord = nextRecordFromSSTable
	}

	return nextRecord, nil
}

/*
GetNextForRange retrieves the next record within a given [rangeStart, rangeEnd) for a start key.
*/
func (lsm *LSM) GetNextForRange(rangeStart string, rangeEnd string, key string) (*model.Record, error) {
	tombstonedKeys := make([]string, 0)
	nextRecord := lsm.checkMemtablesForRangeIterate(rangeStart, rangeEnd, key, &tombstonedKeys)
	nextRecordFromSSTable, err := lsm.checkSSTableForRangeIterate(rangeStart, rangeEnd, key, &tombstonedKeys)

	if err != nil {
		return nil, err
	}

	if nextRecordFromSSTable != nil && (nextRecord == nil || nextRecordFromSSTable.Key < nextRecord.Key) {
		nextRecord = nextRecordFromSSTable
	}

	return nextRecord, nil
}

/*
RangeScan scans all memtables and SSTables for keys within the given range [rangeStart, rangeEnd).
Returns a slice of keys for the specified page.
Parameters:
- rangeStart: the start of the range (inclusive)
- rangeEnd: the end of the range (exclusive)
- pageSize: maximum number of results per page
- pageNumber: which page to return (0-based)
*/
func (lsm *LSM) RangeScan(rangeStart string, rangeEnd string, pageSize int, pageNumber int) ([]string, error) {
	tombstonedKeys := make([]string, 0)
	bestKeys := make([]string, 0)

	// Check memtables first (newest to oldest)
	// We use a large page size initially to collect all relevant keys
	for i := len(lsm.memtables) - 1; i >= 0; i-- {
		mt := lsm.memtables[i]
		mt.ScanForRange(rangeStart, rangeEnd, &tombstonedKeys, &bestKeys, 10000, 0) // Large page size to get all keys
	}

	// Check SSTables (newest to oldest)
	for i := 0; i < len(lsm.levels); i++ {
		levelIndexes := lsm.levels[i]
		for index := len(levelIndexes) - 1; index >= 0; index-- {
			tableIndex := levelIndexes[index]
			err := sstable.ScanForRange(rangeStart, rangeEnd, &tombstonedKeys, &bestKeys, 10000, 0, tableIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to scan SSTable %d: %v", tableIndex, err)
			}
		}
	}

	// Apply pagination to final results
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize

	if startIndex >= len(bestKeys) {
		return []string{}, nil // Return empty slice for pages beyond available data
	}

	if endIndex > len(bestKeys) {
		endIndex = len(bestKeys)
	}

	return bestKeys[startIndex:endIndex], nil
}

/*
checkMemtablesForRangeIterate checks the memtables in reverse order (newest to oldest) for the next key in range.
*/
func (lsm *LSM) checkMemtablesForRangeIterate(rangeStart string, rangeEnd string, key string, tombstonedKeys *[]string) *model.Record {
	var smallestRecord *model.Record = nil
	for i := len(lsm.memtables) - 1; i >= 0; i-- {
		mt := lsm.memtables[i]
		if record := mt.GetNextForRange(rangeStart, rangeEnd, key, tombstonedKeys); record != nil {
			if smallestRecord == nil || record.Key < smallestRecord.Key {
				smallestRecord = record
			}
		}
	}
	return smallestRecord
}

/*
checkSSTableForRangeIterate checks the SSTables in reverse order (newest to oldest) for the next key in range.
*/
func (lsm *LSM) checkSSTableForRangeIterate(rangeStart string, rangeEnd string, key string, tombstonedKeys *[]string) (*model.Record, error) {
	var err error
	var nextRecord *model.Record = nil
	for i := 0; i < len(lsm.levels); i++ {
		levelIndexes := lsm.levels[i]
		for index := len(levelIndexes) - 1; index >= 0; index-- {
			tableIndex := levelIndexes[index]
			record, err := sstable.GetNextForRange(rangeStart, rangeEnd, key, tombstonedKeys, tableIndex)
			if err != nil {
				return nil, err
			}
			if record != nil && (nextRecord == nil || record.Key < nextRecord.Key) {
				nextRecord = record
			}
		}
	}
	return nextRecord, err
}

/*
checkMemtables checks the memtables in reverse order (newest to oldest) for the given key.
*/
func (lsm *LSM) checkMemtablesForPrefixIterate(prefix string, key string, tomstonedKeys *[]string) *model.Record {
	var smallestRecord *model.Record = nil
	for i := len(lsm.memtables) - 1; i >= 0; i-- {
		mt := lsm.memtables[i]
		if record := mt.GetNextForPrefix(prefix, key, tomstonedKeys); record != nil {
			if smallestRecord == nil || record.Key < smallestRecord.Key {
				smallestRecord = record
			}
		}
	}
	return smallestRecord
}

/*
checkSSTableForPrefixIterate checks the SSTables in reverse order (newest to oldest) for the given key.
*/
func (lsm *LSM) checkSSTableForPrefixIterate(prefix string, key string, tomstonedKeys *[]string) (*model.Record, error) {
	var err error
	var nextRecord *model.Record = nil
	for i := 0; i < len(lsm.levels); i++ {
		levelIndexes := lsm.levels[i]
		for index := len(levelIndexes) - 1; index >= 0; index-- {
			tableIndex := levelIndexes[index]
			record, err := sstable.GetNextForPrefix(prefix, key, tomstonedKeys, tableIndex)
			if err != nil {
				return nil, err
			}
			if record != nil && (nextRecord == nil || record.Key < nextRecord.Key) {
				nextRecord = record
			}
		}
	}
	return nextRecord, err
}

/*
PrefixScan scans all memtables and SSTables for keys with the given prefix.
Returns a slice of keys for the specified page.
Parameters:
- prefix: the key prefix to search for
- pageSize: maximum number of results per page
- pageNumber: which page to return (0-based)
*/
func (lsm *LSM) PrefixScan(prefix string, pageSize int, pageNumber int) ([]string, error) {
	tombstonedKeys := make([]string, 0)
	bestKeys := make([]string, 0)

	// Check memtables first (newest to oldest)
	// We use a large page size initially to collect all relevant keys
	for i := len(lsm.memtables) - 1; i >= 0; i-- {
		mt := lsm.memtables[i]
		mt.ScanForPrefix(prefix, &tombstonedKeys, &bestKeys, 10000, 0) // Large page size to get all keys
	}

	// Check SSTables (newest to oldest)
	for i := 0; i < len(lsm.levels); i++ {
		levelIndexes := lsm.levels[i]
		for index := len(levelIndexes) - 1; index >= 0; index-- {
			tableIndex := levelIndexes[index]
			err := sstable.ScanForPrefix(prefix, &tombstonedKeys, &bestKeys, 10000, 0, tableIndex)
			if err != nil {
				return nil, fmt.Errorf("failed to scan SSTable %d: %v", tableIndex, err)
			}
		}
	}

	// Apply pagination to final results
	startIndex := pageNumber * pageSize
	endIndex := startIndex + pageSize

	if startIndex >= len(bestKeys) {
		return []string{}, nil // Return empty slice for pages beyond available data
	}

	if endIndex > len(bestKeys) {
		endIndex = len(bestKeys)
	}

	return bestKeys[startIndex:endIndex], nil
}

func (lsm *LSM) checkIfToFlush(key string) error {
	n := lsm.memtables[len(lsm.memtables)-1]
	if uint64(len(lsm.memtables)) == lsm.maxMemtables && n.IsFull() {
		// Flush the memtable to disk
		// TODO: concurrently flush
	}
	return nil
}
