// memtable/memtable.go
package memtable

import (
	"fmt"
	"hunddb/lsm/memtable/btree"
	"hunddb/lsm/memtable/hashmap"
	mi "hunddb/lsm/memtable/memtable_interface"
	"hunddb/lsm/memtable/skip_list"
	model "hunddb/model/record"
	"sync"
)

type MemtableType string

const (
	BTree    MemtableType = "btree"
	SkipList MemtableType = "skiplist"
	HashMap  MemtableType = "hashmap"
)

// TODO: Get these from config
const (
	CAPACITY      = 1000
	MEMTABLE_TYPE = BTree
)

// MemTable is a concrete struct that wraps the implementation
type MemTable struct {
	impl mi.MemtableInterface
	mu   sync.RWMutex
}

// NewMemtable returns a concrete *MemTable, not an interface
func NewMemtable() (*MemTable, error) {
	var impl mi.MemtableInterface

	switch MEMTABLE_TYPE {
	case BTree:
		impl = btree.NewBTree(btree.DefaultOrder, CAPACITY)
	case SkipList:
		impl = skip_list.New(16, CAPACITY)
	case HashMap:
		impl = hashmap.NewHashMap(CAPACITY)
	default:
		return nil, fmt.Errorf("unknown memtable type: %s", MEMTABLE_TYPE)
	}

	return &MemTable{
		impl: impl,
	}, nil
}

// All methods implement the interface with thread safety
func (mt *MemTable) Put(record *model.Record) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.impl.Put(record)
}

func (mt *MemTable) Delete(record *model.Record) bool {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.impl.Delete(record)
}

func (mt *MemTable) Get(key string) *model.Record {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.Get(key)
}

func (mt *MemTable) GetNextForPrefix(prefix string, key string, tombstonedKeys *[]string) *model.Record {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.GetNextForPrefix(prefix, key, tombstonedKeys)
}

func (mt *MemTable) ScanForPrefix(
	prefix string,
	tombstonedKeys *[]string,
	bestKeys *[]string,
	pageSize int,
	pageNumber int) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	mt.impl.ScanForPrefix(prefix, tombstonedKeys, bestKeys, pageSize, pageNumber)
}

func (mt *MemTable) GetNextForRange(rangeStart string, rangeEnd string, key string, tombstonedKeys *[]string) *model.Record {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.GetNextForRange(rangeStart, rangeEnd, key, tombstonedKeys)
}

func (mt *MemTable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.Size()
}

func (mt *MemTable) Capacity() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.Capacity()
}

func (mt *MemTable) TotalEntries() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.TotalEntries()
}

func (mt *MemTable) IsFull() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.impl.IsFull()
}

func (mt *MemTable) Flush(index int) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.impl.Flush(index)
}

// Verify at compile time that MemTable implements MemtableInterface
var _ mi.MemtableInterface = (*MemTable)(nil)
