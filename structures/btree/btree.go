// Package btree implements a B-tree data structure for efficient key-value storage
// with automatic compaction and balancing capabilities.
package btree

import (
	"bytes"
	"fmt"
	"hund-db/model"
	"sort"
)

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
}

// TreeStats maintains statistics about the B-tree for compaction decisions.
type TreeStats struct {
	TotalRecords      int
	TombstonedRecords int
	ActiveRecords     int
}

// NewBTree creates a new B-tree with the specified order.
// If order is 0 or negative, DefaultOrder is used.
func NewBTree(order int) *BTree {
	if order <= 0 {
		order = DefaultOrder
	}

	return &BTree{
		order: order,
		stats: &TreeStats{},
	}
}

// Get retrieves a record from the B-tree by key.
//
// Parameters:
//   - key: byte slice containing the search key
//
// Returns:
//   - *model.Record: the record if found and not tombstoned, nil otherwise
func (bt *BTree) Get(key []byte) *model.Record {
	if bt.root == nil {
		return nil
	}

	node, index := bt.search(key, bt.root)
	if node != nil && index >= 0 && !node.records[index].IsDeleted() {
		return node.records[index]
	}

	return nil
}

// Put inserts or updates a record in the B-tree.
//
// Parameters:
//   - record: the record to insert or update
//
// Returns:
//   - error: any error that occurred during insertion
func (bt *BTree) Put(record *model.Record) error {
	if record == nil || len(record.Key) == 0 {
		return fmt.Errorf("invalid record: record and key cannot be nil/empty")
	}

	// Initialize root if tree is empty
	if bt.root == nil {
		bt.root = &Node{
			isLeaf:  true,
			records: []*model.Record{record},
		}
		bt.updateStats(record, false)
		return nil
	}

	// Check if key already exists
	existingNode, existingIndex := bt.search(record.Key, bt.root)
	if existingNode != nil && existingIndex >= 0 {
		// Update existing record
		oldRecord := existingNode.records[existingIndex]
		existingNode.records[existingIndex] = record
		bt.updateStatsOnUpdate(oldRecord, record)
	} else {
		// Insert new record
		bt.insertRecord(bt.root, record)
		bt.updateStats(record, false)
	}

	// Check if compaction is needed
	if bt.needsCompaction() {
		bt.compact()
	}

	return nil
}

// Delete marks a record as deleted (tombstoned) in the B-tree.
//
// Parameters:
//   - key: byte slice containing the key to delete
//
// Returns:
//   - bool: true if the record was found and marked as deleted, false otherwise
func (bt *BTree) Delete(key []byte) bool {
	if bt.root == nil {
		return false
	}

	node, index := bt.search(key, bt.root)
	if node != nil && index >= 0 && !node.records[index].IsDeleted() {
		node.records[index].MarkDeleted()
		bt.updateStats(node.records[index], true)

		// Check if compaction is needed
		if bt.needsCompaction() {
			bt.compact()
		}

		return true
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
func (bt *BTree) search(key []byte, node *Node) (*Node, int) {
	if node == nil {
		return nil, -1
	}

	// Binary search within the node
	index := bt.findKeyIndex(node, key)

	// If exact match found
	if index < len(node.records) && bytes.Equal(key, node.records[index].Key) {
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
func (bt *BTree) findKeyIndex(node *Node, key []byte) int {
	return sort.Search(len(node.records), func(i int) bool {
		return bytes.Compare(node.records[i].Key, key) >= 0
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
	if bt.root == nil {
		return
	}

	// Collect all active records
	var activeRecords []*model.Record
	bt.collectActiveRecords(bt.root, &activeRecords)

	// Sort records by key
	sort.Slice(activeRecords, func(i, j int) bool {
		return bytes.Compare(activeRecords[i].Key, activeRecords[j].Key) < 0
	})

	// Rebuild tree
	bt.root = nil
	bt.stats = &TreeStats{}

	for _, record := range activeRecords {
		bt.Put(record)
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
		bt.stats.TombstonedRecords++
		bt.stats.ActiveRecords--
	} else {
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

// Size returns the total number of records (including tombstoned) in the tree.
func (bt *BTree) Size() int {
	return bt.stats.TotalRecords
}

// ActiveSize returns the number of active (non-tombstoned) records in the tree.
func (bt *BTree) ActiveSize() int {
	return bt.stats.ActiveRecords
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
