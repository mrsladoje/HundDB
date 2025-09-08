package skip_list

import (
	"errors"
	model "hunddb/model/record"
	memtable "hunddb/structures/memtable/memtable_interface"
	"math/rand"
	"time"
)

// Compile-time assertion that SkipList implements the Memtable interface.
var _ memtable.MemtableInterface = (*SkipList)(nil)

// Node holds the latest model.Record for a key.
// We do NOT physically remove nodes; Delete() sets tombstone (logical delete).
type Node struct {
	key       string
	rec       *model.Record
	nextNodes []*Node // i-th pointer is for level i
}

// newNode creates a node with given record and height.
func newNode(rec *model.Record, height uint64) *Node {
	return &Node{
		key:       rec.Key,
		rec:       rec,
		nextNodes: make([]*Node, height),
	}
}

// SkipList is a probabilistic, sorted in-memory structure that stores records by key.
// It implements Memtable semantics with logical deletion (tombstones).
type SkipList struct {
	maxHeight     uint64
	currentHeight uint64
	head          *Node

	// Capacity and counters (distinct keys)
	capacity    int // max distinct keys (active + tombstoned)
	totalCount  int // current distinct keys
	activeCount int // current non-tombstoned keys

	// RNG for level selection
	rng *rand.Rand
}

// New creates a SkipList memtable with the given parameters.
// maxHeight >= 1; capacity > 0.
func New(maxHeight uint64, capacity int) *SkipList {
	if maxHeight == 0 {
		maxHeight = 1
	}
	headRec := &model.Record{Key: "", Value: nil, Tombstone: true, Timestamp: 0}
	head := newNode(headRec, maxHeight)
	return &SkipList{
		maxHeight:     maxHeight,
		currentHeight: 1,
		head:          head,
		capacity:      capacity,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ===== Internal helpers =====

func (s *SkipList) roll() uint64 {
	h := uint64(1)
	for s.rng.Int31n(2) == 1 && h < s.maxHeight {
		h++
	}
	return h
}

// search returns update path and the found node (if any).
func (s *SkipList) search(target string, update []*Node) (found *Node) {
	cur := s.head
	for lvl := int(s.currentHeight) - 1; lvl >= 0; lvl-- {
		for cur.nextNodes[lvl] != nil && cur.nextNodes[lvl].key < target {
			cur = cur.nextNodes[lvl]
		}
		if update != nil {
			update[lvl] = cur
		}
	}
	if cur.nextNodes[0] != nil && cur.nextNodes[0].key == target {
		return cur.nextNodes[0]
	}
	return nil
}

func (s *SkipList) insert(rec *model.Record, update []*Node) *Node {
	height := s.roll()
	if height > s.currentHeight {
		for i := s.currentHeight; i < height; i++ {
			update[i] = s.head
		}
		s.currentHeight = height
	}
	n := newNode(rec, height)
	for i := uint64(0); i < height; i++ {
		n.nextNodes[i] = update[i].nextNodes[i]
		update[i].nextNodes[i] = n
	}
	return n
}

// ===== Memtable interface =====

var ErrCapacityExceeded = errors.New("memtable capacity exceeded")

// Put inserts or updates a record for its key.
// NEW key: if IsFull() -> error; else insert and update counters.
// EXISTING key: replace record and adjust activeCount on tombstone transitions.
// If record.Tombstone == true, this acts as a logical delete update.
func (s *SkipList) Put(record *model.Record) error {
	if record == nil {
		return errors.New("nil record")
	}
	update := make([]*Node, s.maxHeight)
	existing := s.search(record.Key, update)

	if existing == nil {
		// New distinct key
		if s.IsFull() {
			return ErrCapacityExceeded
		}
		s.insert(record, update)
		s.totalCount++
		if !record.Tombstone {
			s.activeCount++
		}
		return nil
	}

	// Update existing key
	prevDel := existing.rec.Tombstone
	newDel := record.Tombstone
	existing.rec = record
	if prevDel && !newDel {
		s.activeCount++
	} else if !prevDel && newDel {
		s.activeCount--
	}
	return nil
}

// Delete marks the key as tombstoned using the provided record.
// record.Tombstone will be forced to true.
// Returns true if the key existed before this call; false otherwise.
// For a non-existing key, we insert a tombstone if capacity allows (returning false).
func (s *SkipList) Delete(record *model.Record) bool {
	if record == nil {
		return false
	}
	record.Tombstone = true

	update := make([]*Node, s.maxHeight)
	existing := s.search(record.Key, update)

	if existing == nil {
		// Insert a tombstone for unseen key.
		if s.IsFull() {
			return false
		}
		s.insert(record, update)
		s.totalCount++ // tombstoned new key
		// activeCount unchanged
		return false
	}

	// Key exists: if previously active, decrement activeCount.
	if existing.rec != nil && !existing.rec.Tombstone {
		s.activeCount--
	}
	existing.rec = record
	return true
}

// Get returns the latest non-tombstoned record by key, or nil if absent/tombstoned.
func (s *SkipList) Get(key string) *model.Record {
	n := s.search(key, nil)
	if n == nil || n.rec == nil || n.rec.Tombstone {
		return nil
	}
	return n.rec
}

func (s *SkipList) Size() int         { return s.activeCount }
func (s *SkipList) Capacity() int     { return s.capacity }
func (s *SkipList) TotalEntries() int { return s.totalCount }
func (s *SkipList) IsFull() bool      { return s.totalCount >= s.capacity }

// Flush is a stub; implement sorted SSTable write by iterating bottom-level list.
func (s *SkipList) Flush() error {
	// TODO: emit (key, rec) in sorted order by walking head.nextNodes[0]
	return nil
}
