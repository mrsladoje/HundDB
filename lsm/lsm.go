package lsm

import (
	"encoding/binary"
	"fmt"
	cache "hunddb/lsm/cache"
	memtable "hunddb/lsm/memtable"
	wal "hunddb/lsm/wal"
)

// TODO: load from config
const (
	MAX_LEVELS           = 7
	COMPACTION_TYPE      = "size"
	MAX_TABLES_PER_LEVEL = 4
	MAX_MEMTABLES        = 4
	LSM_PATH             = "lsm.db"
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

// func (lsm *LSM) PersistLSM() error {
// 	// Get the serialized data
// 	data := lsm.serialize()

// 	return nil
// }

/*
Deserialize the LSM parts that need to be persisted (the levels and their SSTable indexes).
*/
// func (lsm *LSM) Deserialize(data []byte) error {
