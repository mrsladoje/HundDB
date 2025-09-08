package memtable

import (
	"fmt"
	model "hunddb/model/record"
	"hunddb/structures/memtable/btree"
	"hunddb/structures/memtable/hashmap"
	mi "hunddb/structures/memtable/memtable_interface"
	"hunddb/structures/memtable/skip_list"
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

func NewMemtable() (mi.MemtableInterface, error) {
	var base mi.MemtableInterface

	switch MEMTABLE_TYPE {
	case BTree:
		base = btree.NewBTree(btree.DefaultOrder, CAPACITY)
	case SkipList:
		base = skip_list.New(16, CAPACITY)
	case HashMap:
		base = hashmap.NewHashMap(CAPACITY)
	default:
		return btree.NewBTree(btree.DefaultOrder, CAPACITY), fmt.Errorf("unknown memtable type: %s", MEMTABLE_TYPE)
	}

	return NewThreadSafeMemtable(base), nil
}

/*
Thread-safe wrapper for MemtableInterface.
Decorator pattern.
*/
type ThreadSafeMemtable struct {
	memtable mi.MemtableInterface
	mu       sync.RWMutex
}

func NewThreadSafeMemtable(memtable mi.MemtableInterface) *ThreadSafeMemtable {
	return &ThreadSafeMemtable{
		memtable: memtable,
	}
}

// Put implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) Put(record *model.Record) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.memtable.Put(record)
}

// Delete implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) Delete(record *model.Record) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.memtable.Delete(record)
}

// Get implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) Get(key string) *model.Record {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.memtable.Get(key)
}

// Size implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) Size() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.memtable.Size()
}

// Capacity implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) Capacity() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.memtable.Capacity()
}

// TotalEntries implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) TotalEntries() int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.memtable.TotalEntries()
}

// IsFull implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) IsFull() bool {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.memtable.IsFull()
}

// Flush implements MemtableInterface with thread safety
func (ts *ThreadSafeMemtable) Flush() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.memtable.Flush()
}
