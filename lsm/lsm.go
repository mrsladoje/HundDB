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
	"os"
	"time"
)

// TODO: load from config
const (
	MAX_LEVELS           = 7
	COMPACTION_TYPE      = "size"
	MAX_TABLES_PER_LEVEL = 4
	MAX_MEMTABLES        = 4
	LSM_PATH             = "lsm.db"
	CRC_SIZE             = 4
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

	err := blockManager.WriteToDisk(data, LSM_PATH, 0)

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
	lsm.levels = make([][]int, MAX_LEVELS)

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

		if levelNum < MAX_LEVELS {
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
	dataLost := false
	wal, err := wal.BuildWAL()
	if err != nil {
		dataLost = true
	}
	lsm := &LSM{
		levels:    make([][]int, MAX_LEVELS),
		memtables: make([]*memtable.MemTable, 0, MAX_MEMTABLES),
		wal:       wal,
		cache:     cache.NewReadPathCache(),
		DataLost:  dataLost, // Initially assume no data loss
	}

	blockManager := block_manager.GetBlockManager()

	// Check if the file exists using os.Stat
	_, err = os.Stat(LSM_PATH)
	if os.IsNotExist(err) {
		// File doesn't exist - this is a fresh start (not data loss)
		firstMemtable, _ := memtable.NewMemtable()
		lsm.memtables = append(lsm.memtables, firstMemtable)
		return lsm
	}

	err = wal.RecoverMemtables(lsm.memtables)
	if err != nil {
		// WAL recovery failed - consider it data loss
		lsm.DataLost = true
		return lsm
	}

	// File exists, so any errors from here on are considered data corruption

	// Try to read the levels size
	levelsSizeBytes, _, err := blockManager.ReadFromDisk(LSM_PATH, 0, 8)
	if err != nil {
		// File exists but can't read size header - corruption
		lsm.DataLost = true
		return lsm
	}

	levelsSize := binary.LittleEndian.Uint64(levelsSizeBytes)

	// Try to read the actual levels data
	data, _, err := blockManager.ReadFromDisk(LSM_PATH, 8+CRC_SIZE, uint64(levelsSize))
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
	if len(lsm.memtables) == MAX_MEMTABLES && n.IsFull() {
		// Flush the memtable to disk
		// TODO: concurrently flush
	}
	return nil
}
