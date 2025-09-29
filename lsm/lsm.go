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
	"sync"
	"time"
)

// Global configuration variables loaded from config in init()
var (
	MAX_LEVELS           uint64
	MAX_TABLES_PER_LEVEL uint64
	MAX_MEMTABLES        uint64
	COMPACTION_TYPE      string
	LSM_PATH             string
	CRC_SIZE             uint64
)

// init loads the LSM settings into global variables from the config
func init() {
	cfg := config.GetConfig()
	MAX_LEVELS = cfg.LSM.MaxLevels
	MAX_TABLES_PER_LEVEL = cfg.LSM.MaxTablesPerLevel
	MAX_MEMTABLES = cfg.LSM.MaxMemtables
	COMPACTION_TYPE = cfg.LSM.CompactionType
	LSM_PATH = cfg.LSM.LSMPath
	CRC_SIZE = cfg.CRC.Size
}

/*
LSM represents a Log-Structured Merge Tree
*/
type LSM struct {
	// Each level holds the indexes of its SSTables
	levels    [][]uint64
	memtables []*memtable.MemTable
	wal       *wal.WAL
	cache     *cache.ReadPathCache

	// Flag to indicate if previous data was lost during loading
	DataLost bool

	// NextSSTableIndex holds the next available SSTable index to use when creating a new SSTable
	// It is computed at load time using GetNextSSTableIndex() and can be used by the app layer
	NextSSTableIndex uint64

	// mu protects concurrent access to LSM shared state (levels, memtables metadata, DataLost, NextSSTableIndex)
	mu sync.RWMutex

	// flushPool is the worker pool used for concurrent memtable flushes (lazy-initialized)
	flushPool *FlushPool

	// levelLocks ensures only one compaction operates on a given level at a time
	levelLocks []sync.Mutex
}

/*
Serialize the LSM parts that need to be persisted (the levels and their SSTable indexes).
*/
func (lsm *LSM) serialize() []byte {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

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
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	if len(data) == 0 {
		return fmt.Errorf("invalid data: empty")
	}

	// Convert bytes directly to string
	stringifiedLevels := string(data)

	// Parse the stringified levels back into the levels slice
	lsm.levels = make([][]uint64, int(MAX_LEVELS))

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
		var tableIndexes []uint64
		for i < len(stringifiedLevels) && stringifiedLevels[i] != ']' {
			numStart := i
			for i < len(stringifiedLevels) && stringifiedLevels[i] != ',' && stringifiedLevels[i] != ']' {
				i++
			}

			if i > numStart {
				var tableIndex uint64
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

		if uint64(levelNum) < MAX_LEVELS {
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
	lsm := &LSM{
		// Initialize slices with config values
		levels:     make([][]uint64, int(MAX_LEVELS)),
		memtables:  make([]*memtable.MemTable, 0, int(MAX_MEMTABLES)),
		wal:        wal.NewWAL("wal.db", 0), // TODO: implement actual logic here
		cache:      cache.NewReadPathCache(),
		DataLost:   false, // Initially assume no data loss
		flushPool:  nil,
		levelLocks: make([]sync.Mutex, int(MAX_LEVELS)),
	}

	blockManager := block_manager.GetBlockManager()

	// Check if the file exists using os.Stat
	_, err := os.Stat(LSM_PATH)
	if os.IsNotExist(err) {
		// File doesn't exist - this is a fresh start (not data loss)
		firstMemtable, _ := memtable.NewMemtable()
		lsm.memtables = append(lsm.memtables, firstMemtable)
		// Initialize next SSTable index on fresh start
		lsm.mu.Lock()
		lsm.NextSSTableIndex = lsm.getNextSSTableIndexUnsafe()
		lsm.mu.Unlock()
		return lsm
	}

	// File exists, so any errors from here on are considered data corruption

	// Try to read the levels size
	levelsSizeBytes, _, err := blockManager.ReadFromDisk(LSM_PATH, 0, 8)
	if err != nil {
		// File exists but can't read size header - corruption
		lsm.mu.Lock()
		lsm.DataLost = true
		// Initialize with current state
		lsm.NextSSTableIndex = lsm.getNextSSTableIndexUnsafe()
		lsm.mu.Unlock()
		return lsm
	}

	levelsSize := binary.LittleEndian.Uint64(levelsSizeBytes)

	// Try to read the actual levels data
	data, _, err := blockManager.ReadFromDisk(LSM_PATH, 8, uint64(levelsSize))
	if err != nil {
		// File exists but can't read data - corruption
		lsm.mu.Lock()
		lsm.DataLost = true
		// Initialize with current state
		lsm.NextSSTableIndex = lsm.getNextSSTableIndexUnsafe()
		lsm.mu.Unlock()
		return lsm
	}

	// Try to deserialize the data
	err = lsm.deserialize(data)
	if err != nil {
		// File exists but data format is invalid - corruption
		lsm.mu.Lock()
		lsm.DataLost = true
		// Initialize with current state
		lsm.NextSSTableIndex = lsm.getNextSSTableIndexUnsafe()
		lsm.mu.Unlock()
		return lsm
	}

	// Successfully loaded previous data
	lsm.mu.Lock()
	lsm.NextSSTableIndex = lsm.getNextSSTableIndexUnsafe()
	lsm.mu.Unlock()
	return lsm
}

// initFlushPoolOnce lazily initializes the flush worker pool
func (lsm *LSM) initFlushPoolOnce(workers int) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	if lsm.flushPool == nil {
		lsm.flushPool = NewFlushPool(workers)
	}
}

/*
IsDataLost returns true if the previous LSM data was lost during loading.
This can happen if the LSM file doesn't exist, is corrupted, or unreadable.
*/
func (lsm *LSM) IsDataLost() bool {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	return lsm.DataLost
}

// Get retrieves a record from the LSM by checking the memtables, cache, and SSTables in order.
func (lsm *LSM) Get(key string) (*model.Record, error, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	errorEncountered := false

	// 1. Check memtables first
	if record := lsm.checkMemtables(key); record != nil {
		return record, nil, false
	}

	// 2. Check cache
	record, err := lsm.cache.Get(key)
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
			record, err := sstable.Get(key, int(tableIndex))
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
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

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
	lsm.mu.Lock()
	defer lsm.mu.Unlock()

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
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

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
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
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
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
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
			err := sstable.ScanForRange(rangeStart, rangeEnd, &tombstonedKeys, &bestKeys, 10000, 0, int(tableIndex))
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
			record, err := sstable.GetNextForRange(rangeStart, rangeEnd, key, tombstonedKeys, int(tableIndex))
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
			record, err := sstable.GetNextForPrefix(prefix, key, tomstonedKeys, int(tableIndex))
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
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
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
			err := sstable.ScanForPrefix(prefix, &tombstonedKeys, &bestKeys, 10000, 0, int(tableIndex))
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
getNextSSTableIndexUnsafe returns the next available SSTable index without acquiring locks.
This is an internal helper method that should only be called when locks are already held.
*/
func (lsm *LSM) getNextSSTableIndexUnsafe() uint64 {
	var maxIndex uint64 = 0
	var hasAnyTables bool = false

	// Iterate through all levels
	for _, level := range lsm.levels {
		// Iterate through all SSTable indexes in this level
		for _, tableIndex := range level {
			if !hasAnyTables || tableIndex > maxIndex {
				maxIndex = tableIndex
				hasAnyTables = true
			}
		}
	}

	// Return the next available index (maxIndex + 1)
	// If no SSTables exist, this returns 0
	if !hasAnyTables {
		return 0
	}
	return maxIndex + 1
}

/*
GetNextSSTableIndex returns the next available SSTable index by finding the largest
existing index across all levels and adding 1. Returns 0 if no SSTables exist.
*/
func (lsm *LSM) GetNextSSTableIndex() uint64 {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	return lsm.getNextSSTableIndexUnsafe()
}

/*
GetNextSSTableIndexWithIncrement returns the current value of NextSSTableIndex
and then increments it for the next call. This provides a simple monotonic
counter for assigning new SSTable indexes based on the value computed at load
time. Note: This method is not concurrency-safe.
*/
func (lsm *LSM) GetNextSSTableIndexWithIncrement() uint64 {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	current := lsm.NextSSTableIndex
	lsm.NextSSTableIndex++
	return current
}

func (lsm *LSM) checkIfToFlush(key string) error {
	n := lsm.memtables[len(lsm.memtables)-1]
	if uint64(len(lsm.memtables)) == MAX_MEMTABLES && n.IsFull() {
		// Prepare batch: copy current memtables in order (oldest->newest)
		batch := make([]*memtable.MemTable, len(lsm.memtables))
		copy(batch, lsm.memtables)

		// Assign indices for each memtable using the monotonic counter
		indexes := make([]int, len(batch))
		for i := 0; i < len(batch); i++ {
			indexes[i] = int(lsm.GetNextSSTableIndexWithIncrement())
		}

		// Reset memtables with a fresh empty one so writers can continue immediately
		fresh, _ := memtable.NewMemtable()
		lsm.memtables = []*memtable.MemTable{fresh}

		// Ensure flush pool exists (lazy init) with 4 workers
		lsm.initFlushPoolOnce(4)

		// Submit batch to pool (concurrently flushed, but committed oldest->newest)
		lsm.flushPool.submitBatch(lsm, batch, indexes)
	}
	return nil
}

// maybeStartCompactions checks compaction triggers after a flush; left empty for now
func (lsm *LSM) maybeStartCompactions() {
	switch COMPACTION_TYPE {
	case "size":
		go lsm.sizeTieredCompaction()
	case "level", "leveled":
		go lsm.leveledCompaction()
	default:
		// unsupported type: do nothing
	}
}

// sizeTieredCompaction performs size-tiered compaction starting from level 0 and cascading upwards
func (lsm *LSM) sizeTieredCompaction() {
	maxLevels := int(MAX_LEVELS)
	maxPer := int(MAX_TABLES_PER_LEVEL)
	if maxPer < 2 { // nothing sensible to do
		return
	}

	for lvl := 0; lvl < maxLevels; lvl++ {
		for {
			// Exclusively reserve this level for compaction
			lsm.levelLocks[lvl].Lock()

			// Check current count
			lsm.mu.RLock()
			count := 0
			if lvl < len(lsm.levels) {
				count = len(lsm.levels[lvl])
			}
			lsm.mu.RUnlock()

			if count <= maxPer {
				lsm.levelLocks[lvl].Unlock()
				break
			}

			// Choose oldest group (first maxPer tables)
			groupSize := maxPer
			if count < groupSize {
				groupSize = count
			}
			if groupSize < 2 {
				lsm.levelLocks[lvl].Unlock()
				break
			}

			// Snapshot group under read lock
			group := make([]int, groupSize)
			lsm.mu.RLock()
			for i := 0; i < groupSize; i++ {
				group[i] = int(lsm.levels[lvl][i])
			}
			lsm.mu.RUnlock()

			// Assign new SSTable index
			newIndex := int(lsm.GetNextSSTableIndexWithIncrement())

			// Perform compaction (heavy IO), keep the level lock held to serialize same-level compactions
			if err := sstable.Compact(group, newIndex); err != nil {
				// If compaction fails, release and stop attempting this level for now
				lsm.levelLocks[lvl].Unlock()
				return
			}

			// Determine target level (next level if exists, else same level)
			target := lvl
			if lvl < maxLevels-1 {
				target = lvl + 1
			}

			// Lock target level (if different) to avoid concurrent mutations there
			if target != lvl {
				lsm.levelLocks[target].Lock()
			}

			// Apply metadata changes atomically
			lsm.mu.Lock()
			// Remove first groupSize from current level
			cur := lsm.levels[lvl]
			if groupSize <= len(cur) {
				cur = cur[groupSize:]
			} else {
				cur = []uint64{}
			}
			lsm.levels[lvl] = cur
			// Append new index to target level
			lsm.levels[target] = append(lsm.levels[target], uint64(newIndex))
			lsm.mu.Unlock()

			if target != lvl {
				lsm.levelLocks[target].Unlock()
			}

			// Keep looping on the same level until within capacity; outer loop will then handle the target level
			lsm.levelLocks[lvl].Unlock()
		}
	}
}

// leveledCompaction compacts oldest table(s) from level L with overlapping tables from level L+1
// and places the result into level L+1, cascading upwards if levels exceed capacity.
func (lsm *LSM) leveledCompaction() {
	maxLevels := int(MAX_LEVELS)
	maxPer := int(MAX_TABLES_PER_LEVEL)
	if maxLevels < 2 || maxPer < 1 {
		return
	}

	// Iterate from L0 upwards (up to the second-to-last level, since we promote to next)
	for lvl := 0; lvl < maxLevels-1; lvl++ {
		for {
			// Exclusively reserve source level
			lsm.levelLocks[lvl].Lock()

			// Check current count
			lsm.mu.RLock()
			count := len(lsm.levels[lvl])
			lsm.mu.RUnlock()

			// Trigger rule: compact while level exceeds capacity
			if count <= maxPer {
				lsm.levelLocks[lvl].Unlock()
				break
			}

			target := lvl + 1
			// Lock target level as well to avoid races with its compactions
			lsm.levelLocks[target].Lock()

			// Snapshot candidate from source (oldest first)
			var srcIdx uint64
			lsm.mu.RLock()
			if len(lsm.levels[lvl]) > 0 {
				srcIdx = lsm.levels[lvl][0]
			} else {
				srcIdx = 0
			}
			lsm.mu.RUnlock()

			if srcIdx == 0 && count == 0 {
				// nothing to do
				lsm.levelLocks[target].Unlock()
				lsm.levelLocks[lvl].Unlock()
				break
			}

			// Determine overlap window for candidate
			minK, maxK, err := sstable.GetSSBoundaries(int(srcIdx))
			if err != nil {
				// On error, give up this round for safety
				lsm.levelLocks[target].Unlock()
				lsm.levelLocks[lvl].Unlock()
				return
			}

			// Collect overlapping tables from target level
			overlaps := make([]int, 0)
			lsm.mu.RLock()
			targetSlice := lsm.levels[target]
			lsm.mu.RUnlock()
			for _, tIdx := range targetSlice {
				tMin, tMax, e := sstable.GetSSBoundaries(int(tIdx))
				if e != nil {
					// Skip this table if boundaries are unreadable
					continue
				}
				// Overlap if ranges intersect
				if !(maxK < tMin || tMax < minK) {
					overlaps = append(overlaps, int(tIdx))
				}
			}

			// Build compaction list: candidate + overlaps
			// Order newest first as required by sstable.Compact
			compactionList := make([]int, 0, 1+len(overlaps))
			// Candidate from lvl is newer than those in target level typically; put first
			compactionList = append(compactionList, int(srcIdx))
			// For target overlaps, add newest first by scanning from end to start
			if len(overlaps) > 1 {
				// overlaps order currently matches targetSlice order; we need newest first (end to start)
				// Create a set for quick membership test
				overlapSet := make(map[int]struct{}, len(overlaps))
				for _, v := range overlaps {
					overlapSet[v] = struct{}{}
				}
				for i := len(targetSlice) - 1; i >= 0; i-- {
					idx := int(targetSlice[i])
					if _, ok := overlapSet[idx]; ok {
						compactionList = append(compactionList, idx)
					}
				}
			} else if len(overlaps) == 1 {
				compactionList = append(compactionList, overlaps[0])
			}

			// Assign new SSTable index
			newIndex := int(lsm.GetNextSSTableIndexWithIncrement())

			// Perform compaction with both levels reserved
			if err := sstable.Compact(compactionList, newIndex); err != nil {
				lsm.levelLocks[target].Unlock()
				lsm.levelLocks[lvl].Unlock()
				return
			}

			// Remove src and overlaps; append newIndex to target level
			lsm.mu.Lock()
			// Remove source candidate from level lvl (first occurrence)
			lsm.levels[lvl] = removeFirstOccurrence(lsm.levels[lvl], uint64(srcIdx))

			// Remove each overlap in target level
			for _, oi := range overlaps {
				lsm.levels[target] = removeFirstOccurrence(lsm.levels[target], uint64(oi))
			}

			// Append new compacted table to target level
			lsm.levels[target] = append(lsm.levels[target], uint64(newIndex))
			lsm.mu.Unlock()

			// Release locks and iterate again while over capacity
			lsm.levelLocks[target].Unlock()
			lsm.levelLocks[lvl].Unlock()
		}
	}
}

// removeFirstOccurrence removes the first match of val from slice s, if present
func removeFirstOccurrence(s []uint64, val uint64) []uint64 {
	for i, v := range s {
		if v == val {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

// GetLevels returns a copy of the current SSTable levels structure
func (lsm *LSM) GetLevels() [][]int {
	// Create a deep copy to prevent external modification
	levelsCopy := make([][]int, len(lsm.levels))
	for i, level := range lsm.levels {
		if level != nil {
			levelsCopy[i] = make([]int, len(level))
			for j, v := range level {
				levelsCopy[i][j] = int(v)
			}
		}
	}
	return levelsCopy
}
