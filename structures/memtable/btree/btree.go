// Package btree implements a B-tree data structure for efficient key-value storage
// with automatic compaction and balancing capabilities.
package btree

import (
	"fmt"
	model "hunddb/model/record"
	memtable "hunddb/structures/memtable" // memtable interface
	"math"
	"sort"
)

// Compile-time assertion that BTree implements the Memtable interface.
var _ memtable.MemtableInterface = (*BTree)(nil)

const (
	// DefaultOrder defines the default order (degree) of the B-tree.
	// A B-tree of order m can have at most m-1 keys and m children per node.
	DefaultOrder = 5

	// CompactionThreshold is the percentage of tombstoned records that triggers compaction.
	CompactionThreshold = 0.30 // 30%
)

// Node represents a node in the B-tree containing records and child nodes.
// Each node maintains a sorted order of records and references to child nodes.
type Node struct {
	// parent references the parent node (nil for root)
	parent *Node

	// children contains pointers to child nodes
	children []*Node

	// records contains the key-value records stored in this node
	records []*model.Record

	// isLeaf indicates whether this node is a leaf (has no children)
	isLeaf bool
}

// BTree represents a B-tree data structure with automatic balancing and compaction.
type BTree struct {
	// root points to the root node of the tree
	root *Node

	// order defines the maximum number of children a node can have
	order int

	// stats tracks tree statistics for compaction decisions
	stats *TreeStats

	// compacting flag to prevent recursive compaction
	compacting bool

	// capacity is the maximum number of distinct keys (active + tombstoned)
	capacity int
}

// TreeStats maintains statistics about the B-tree for compaction decisions.
type TreeStats struct {
	TotalRecords      int
	TombstonedRecords int
	ActiveRecords     int
}

// NewBTree creates a B-tree with an explicit capacity (distinct keys).
func NewBTree(order, capacity int) *BTree {
	if order <= 0 {
		order = DefaultOrder
	}
	if capacity <= 0 {
		capacity = math.MaxInt
	}
	return &BTree{
		order:    order,
		capacity: capacity,
		stats:    &TreeStats{},
	}
}

// Get retrieves a record from the B-tree by key.
//
// Parameters:
//   - key: string containing the search key
//
// Returns:
//   - *model.Record: the record if found and not tombstoned, nil otherwise
func (bt *BTree) Get(key string) *model.Record {
	if bt.root == nil {
		return nil
	}

	node, index := bt.search(key, bt.root)
	if node != nil && index >= 0 && !node.records[index].IsDeleted() {
		return node.records[index]
	}

	return nil
}

// Add inserts or updates a record in the B-tree.
//
// Parameters:
//   - record: the record to insert or update
//
// Returns:
//   - error: any error that occurred during insertion
func (bt *BTree) Add(record *model.Record) error {
	if record == nil || len(record.Key) == 0 {
		return fmt.Errorf("invalid record: record and key cannot be nil/empty")
	}

	// If tree is empty, this is a NEW distinct key → respect capacity.
	if bt.root == nil {
		// Capacity applies only to NEW distinct keys.
		if bt.IsFull() {
			return fmt.Errorf("memtable is full (capacity=%d)", bt.capacity)
		}
		bt.root = &Node{
			isLeaf:  true,
			records: []*model.Record{record},
		}
		bt.updateStats(record, false) // new distinct key accounted
		return nil
	}

	// Check if key already exists
	existingNode, existingIndex := bt.search(record.Key, bt.root)
	if existingNode != nil && existingIndex >= 0 {
		// Update existing key .
		oldRecord := existingNode.records[existingIndex]
		existingNode.records[existingIndex] = record
		bt.updateStatsOnUpdate(oldRecord, record)
	} else {
		// NEW distinct key → must respect capacity.
		if bt.IsFull() {
			return fmt.Errorf("memtable is full (capacity=%d)", bt.capacity)
		}
		bt.insertRecord(bt.root, record)
		bt.updateStats(record, false)
	}

	// Trigger compaction if needed (avoid recursion).
	if !bt.compacting && !record.IsDeleted() && bt.needsCompaction() {
		bt.compact()
	}
	return nil
}

// IsFull reports whether inserting a NEW distinct key would exceed capacity.
func (bt *BTree) IsFull() bool {
	return bt.stats.TotalRecords >= bt.capacity
}

// Delete marks the key as tombstoned.
// Behavior:
//   - If key exists: replace the record with a tombstone and update stats; return true.
//   - If key does not exist: insert a tombstone via Add() (capacity-checked); return false.
func (bt *BTree) Delete(record *model.Record) bool {
	if record == nil || record.Key == "" {
		return false
	}

	record.MarkDeleted()

	// Try to find existing key.
	if bt.root != nil {
		if node, idx := bt.search(record.Key, bt.root); node != nil && idx >= 0 {
			old := node.records[idx]
			// Already tombstoned → nothing changes except we keep latest pointer.
			if old.IsDeleted() {
				return true
			}
			// Replace with tombstone and adjust stats.
			node.records[idx] = record
			bt.updateStatsOnUpdate(old, record)
			return true
		}
	}

	// Not found → delegate to Add().
	if err := bt.Add(record); err != nil { // capacity reached for new key
		return false
	}
	return false
}

// search recursively searches for a key in the B-tree starting from the given node.
//
// Parameters:
//   - key: byte slice containing the search key
//   - node: starting node for the search
//
// Returns:
//   - *Node: node containing the key if found, nil otherwise
//   - int: index of the record in the node if found, -1 otherwise
func (bt *BTree) search(key string, node *Node) (*Node, int) {
	if node == nil {
		return nil, -1
	}

	// Binary search within the node
	index := bt.findKeyIndex(node, key)

	// If exact match found
	if index < len(node.records) && key == node.records[index].Key {
		return node, index
	}

	// If leaf node and no exact match, key doesn't exist
	if node.isLeaf {
		return nil, -1
	}

	// Search in appropriate child
	if index < len(node.children) {
		return bt.search(key, node.children[index])
	}

	return nil, -1
}

// findKeyIndex finds the appropriate index for a key in a node using binary search.
func (bt *BTree) findKeyIndex(node *Node, key string) int {
	return sort.Search(len(node.records), func(i int) bool {
		return node.records[i].Key >= key
	})
}

// insertRecord inserts a record into the appropriate position in the tree.
func (bt *BTree) insertRecord(node *Node, record *model.Record) {
	if node.isLeaf {
		bt.insertIntoLeaf(node, record)
	} else {
		bt.insertIntoInternal(node, record)
	}
}

// insertIntoLeaf inserts a record into a leaf node.
func (bt *BTree) insertIntoLeaf(node *Node, record *model.Record) {
	index := bt.findKeyIndex(node, record.Key)

	// Insert record at the correct position
	node.records = append(node.records, nil)
	copy(node.records[index+1:], node.records[index:])
	node.records[index] = record

	// Check if node needs to be split
	if len(node.records) >= bt.order {
		bt.splitNode(node)
	}
}

// insertIntoInternal inserts a record into an internal node by finding the correct child.
func (bt *BTree) insertIntoInternal(node *Node, record *model.Record) {
	index := bt.findKeyIndex(node, record.Key)

	// Find the correct child to insert into
	childIndex := index
	if index >= len(node.children) {
		childIndex = len(node.children) - 1
	}

	bt.insertRecord(node.children[childIndex], record)
}

// splitNode splits a node when it becomes too full.
func (bt *BTree) splitNode(node *Node) {
	mid := len(node.records) / 2
	midRecord := node.records[mid]

	// Create new right node
	rightNode := &Node{
		parent:  node.parent,
		isLeaf:  node.isLeaf,
		records: make([]*model.Record, len(node.records)-mid-1),
	}
	copy(rightNode.records, node.records[mid+1:])

	// If not leaf, split children too
	if !node.isLeaf {
		rightNode.children = make([]*Node, len(node.children)-mid-1)
		copy(rightNode.children, node.children[mid+1:])

		// Update parent pointers
		for _, child := range rightNode.children {
			child.parent = rightNode
		}

		node.children = node.children[:mid+1]
	}

	// Truncate left node
	node.records = node.records[:mid]

	// Handle root split
	if node.parent == nil {
		newRoot := &Node{
			isLeaf:   false,
			records:  []*model.Record{midRecord},
			children: []*Node{node, rightNode},
		}
		node.parent = newRoot
		rightNode.parent = newRoot
		bt.root = newRoot
	} else {
		// Insert middle record into parent
		bt.insertRecordIntoParent(node.parent, midRecord, rightNode)
	}
}

// insertRecordIntoParent inserts a record and right child into a parent node.
func (bt *BTree) insertRecordIntoParent(parent *Node, record *model.Record, rightChild *Node) {
	index := bt.findKeyIndex(parent, record.Key)

	// Insert record
	parent.records = append(parent.records, nil)
	copy(parent.records[index+1:], parent.records[index:])
	parent.records[index] = record

	// Insert right child
	parent.children = append(parent.children, nil)
	copy(parent.children[index+2:], parent.children[index+1:])
	parent.children[index+1] = rightChild
	rightChild.parent = parent

	// Check if parent needs to be split
	if len(parent.records) >= bt.order {
		bt.splitNode(parent)
	}
}

// needsCompaction determines if the tree needs compaction based on tombstoned records.
func (bt *BTree) needsCompaction() bool {
	if bt.stats.TotalRecords == 0 {
		return false
	}

	tombstoneRatio := float64(bt.stats.TombstonedRecords) / float64(bt.stats.TotalRecords)
	return tombstoneRatio >= CompactionThreshold
}

// compact rebuilds the tree removing all tombstoned records.
func (bt *BTree) compact() {
	if bt.root == nil || bt.compacting {
		return
	}

	bt.compacting = true
	defer func() {
		bt.compacting = false
	}()

	// Collect all active records
	var activeRecords []*model.Record
	bt.collectActiveRecords(bt.root, &activeRecords)

	// Sort records by key
	sort.Slice(activeRecords, func(i, j int) bool {
		return activeRecords[i].Key < activeRecords[j].Key
	})

	// Rebuild tree - reset everything
	bt.root = nil
	bt.stats = &TreeStats{
		TotalRecords:      0,
		TombstonedRecords: 0,
		ActiveRecords:     0,
	}

	// Re-insert only active records
	for _, record := range activeRecords {
		// Make sure the record is not marked as tombstoned
		record.Tombstone = false

		if bt.root == nil {
			bt.root = &Node{
				isLeaf:  true,
				records: []*model.Record{record},
			}
		} else {
			bt.insertRecord(bt.root, record)
		}
		// Update stats - each record should be active
		bt.stats.TotalRecords++
		bt.stats.ActiveRecords++
	}
}

// collectActiveRecords recursively collects all active (non-tombstoned) records.
func (bt *BTree) collectActiveRecords(node *Node, records *[]*model.Record) {
	if node == nil {
		return
	}

	// Collect active records from this node
	for _, record := range node.records {
		if !record.IsDeleted() {
			*records = append(*records, record)
		}
	}

	// Recursively collect from children
	for _, child := range node.children {
		bt.collectActiveRecords(child, records)
	}
}

// updateStats updates tree statistics when adding/modifying records.
func (bt *BTree) updateStats(record *model.Record, isDeleting bool) {
	if isDeleting {
		// This path is used when marking records as deleted
		bt.stats.TombstonedRecords++
		bt.stats.ActiveRecords--
	} else {
		// This path is used when adding new records
		bt.stats.TotalRecords++
		if record.IsDeleted() {
			bt.stats.TombstonedRecords++
		} else {
			bt.stats.ActiveRecords++
		}
	}
}

// updateStatsOnUpdate updates statistics when updating an existing record.
func (bt *BTree) updateStatsOnUpdate(oldRecord, newRecord *model.Record) {
	// Remove old record stats
	if oldRecord.IsDeleted() {
		bt.stats.TombstonedRecords--
	} else {
		bt.stats.ActiveRecords--
	}

	// Add new record stats
	if newRecord.IsDeleted() {
		bt.stats.TombstonedRecords++
	} else {
		bt.stats.ActiveRecords++
	}
}

// GetStats returns a copy of the current tree statistics.
func (bt *BTree) GetStats() TreeStats {
	if bt.stats == nil {
		return TreeStats{}
	}
	return *bt.stats
}

// Size returns the number of active (non-tombstoned) keys.
func (bt *BTree) Size() int {
	return bt.stats.ActiveRecords
}

// Capacity returns the maximum number of distinct keys allowed.
func (bt *BTree) Capacity() int {
	return bt.capacity
}

// Flush persists the memtable contents to disk (implementation-specific).
func (bt *BTree) Flush() error {
	// TODO: Implement SSTable flush logic here.
	return nil
}

// TotalEntries returns the number of distinct keys (active + tombstoned).
func (bt *BTree) TotalEntries() int {
	return bt.stats.TotalRecords
}

// Height returns the height of the B-tree.
func (bt *BTree) Height() int {
	if bt.root == nil {
		return 0
	}

	height := 1
	node := bt.root
	for !node.isLeaf {
		height++
		if len(node.children) > 0 {
			node = node.children[0]
		} else {
			break
		}
	}

	return height
}
