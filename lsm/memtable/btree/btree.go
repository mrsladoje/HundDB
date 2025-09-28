package btree

import (
	"fmt"
	memtable "hunddb/lsm/memtable/memtable_interface"
	sstable "hunddb/lsm/sstable"
	model "hunddb/model/record"
	"math"
	"sort"
)

// Compile-time assertion that BTree implements the Memtable interface.
var _ memtable.MemtableInterface = (*BTree)(nil)

const (
	// DefaultOrder defines the default order (degree) of the B-tree.
	// A B-tree of order m can have at most m-1 keys and m children per node.
	DefaultOrder = 5
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

// BTree represents a B-tree data structure
type BTree struct {
	// root points to the root node of the tree
	root *Node

	// order defines the maximum number of children a node can have
	order int

	// totalRecords tracks the total number of distinct keys (active + tombstoned)
	totalRecords int

	// activeRecords tracks the number of non-tombstoned keys
	activeRecords int

	// capacity is the maximum number of distinct keys (active + tombstoned)
	capacity int
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
		order:        order,
		capacity:     capacity,
		totalRecords: 0,
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

// GetNextForPrefix returns the next record in lexicographical order after the given key,
// constrained to the given prefix, or nil if none exists.
// tombstonedKeys is used to track keys that have been tombstoned in more recent structures.
func (bt *BTree) GetNextForPrefix(prefix string, key string, tombstonedKeys *[]string) *model.Record {
	if bt.root == nil {
		return nil
	}

	return bt.findNextPrefixMatchAfterKey(prefix, key, bt.root, tombstonedKeys)
}

// GetNextForRange returns the next record in lexicographical order after the given key,
// constrained to the given range [rangeStart, rangeEnd] (inclusive), or nil if none exists.
// tombstonedKeys is used to track keys that have been tombstoned in more recent structures.
func (bt *BTree) GetNextForRange(rangeStart string, rangeEnd string, key string, tombstonedKeys *[]string) *model.Record {
	if bt.root == nil {
		return nil
	}

	return bt.findNextRangeMatchAfterKey(rangeStart, rangeEnd, key, bt.root, tombstonedKeys)
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
		bt.totalRecords++
		if !record.Tombstone {
			bt.activeRecords++
		}
		return nil
	}

	// Check if key already exists
	existingNode, existingIndex := bt.search(record.Key, bt.root)
	if existingNode != nil && existingIndex >= 0 {
		oldRecord := existingNode.records[existingIndex]
		wasActive := !oldRecord.Tombstone
		isActive := !record.Tombstone
		if wasActive && !isActive {
			bt.activeRecords--
		} else if !wasActive && isActive {
			bt.activeRecords++
		}
		existingNode.records[existingIndex] = record
	} else {
		// NEW distinct key → must respect capacity.
		if bt.IsFull() {
			return fmt.Errorf("memtable is full (capacity=%d)", bt.capacity)
		}
		bt.insertRecord(bt.root, record)
		bt.totalRecords++
		if !record.Tombstone {
			bt.activeRecords++
		}
	}

	return nil
}

// IsFull reports whether inserting a NEW distinct key would exceed capacity.
func (bt *BTree) IsFull() bool {
	return bt.totalRecords >= bt.capacity
}

// Delete marks the key as tombstoned.
// Behavior:
//   - If key exists: replace the record with a tombstone and update stats; return true.
//   - If key does not exist: insert a tombstone via Put() (capacity-checked); return false.
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
			bt.activeRecords--
			return true
		}
	}

	// Not found → delegate to Put().
	if err := bt.Put(record); err != nil { // capacity reached for new key
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

// isKeyTombstoned checks if a key is in the tombstoned keys slice
func isKeyTombstoned(key string, tombstonedKeys *[]string) bool {
	if tombstonedKeys == nil {
		return false
	}
	for _, tombKey := range *tombstonedKeys {
		if tombKey == key {
			return true
		}
	}
	return false
}

// addToTombstoned adds a key to the tombstoned keys slice if it's not already there
func addToTombstoned(key string, tombstonedKeys *[]string) {
	if tombstonedKeys == nil {
		return
	}
	// Check if already exists to avoid duplicates
	for _, tombKey := range *tombstonedKeys {
		if tombKey == key {
			return
		}
	}
	*tombstonedKeys = append(*tombstonedKeys, key)
}

// findNextPrefixMatchAfterKey finds the first non-tombstoned record with the given prefix
// that comes lexicographically after the given key.
func (bt *BTree) findNextPrefixMatchAfterKey(prefix string, afterKey string, node *Node, tombstonedKeys *[]string) *model.Record {
	if node == nil {
		return nil
	}

	if node.isLeaf {
		// Find the first record > afterKey that matches prefix
		startIndex := sort.Search(len(node.records), func(i int) bool {
			return node.records[i].Key > afterKey
		})

		// Check each record from startIndex onwards
		for i := startIndex; i < len(node.records); i++ {
			record := node.records[i]
			// If key doesn't start with prefix, we've gone too far
			if len(record.Key) < len(prefix) || record.Key[:len(prefix)] != prefix {
				return nil
			}

			// Check if this record is tombstoned locally
			if record.IsDeleted() {
				addToTombstoned(record.Key, tombstonedKeys)
				continue
			}

			// Check if key is tombstoned in more recent structures
			if isKeyTombstoned(record.Key, tombstonedKeys) {
				continue
			}

			// Found a valid, non-tombstoned record
			return record
		}
		return nil
	}

	// For internal nodes, find the appropriate child to start searching from
	startChildIndex := sort.Search(len(node.records), func(i int) bool {
		return node.records[i].Key > afterKey
	})

	// Search the appropriate child subtree first
	if startChildIndex < len(node.children) {
		if result := bt.findNextPrefixMatchAfterKey(prefix, afterKey, node.children[startChildIndex], tombstonedKeys); result != nil {
			return result
		}
	}

	// Check records in this internal node and their right subtrees
	for i := startChildIndex; i < len(node.records); i++ {
		record := node.records[i]

		// Only consider records that come after afterKey and match prefix
		if record.Key > afterKey && len(record.Key) >= len(prefix) && record.Key[:len(prefix)] == prefix {
			// Check if this record is tombstoned locally
			if record.IsDeleted() {
				addToTombstoned(record.Key, tombstonedKeys)
			} else if !isKeyTombstoned(record.Key, tombstonedKeys) {
				// Found a valid, non-tombstoned record
				return record
			}
		}

		// If record key doesn't start with prefix and is > prefix, we've gone too far
		if record.Key > prefix && (len(record.Key) < len(prefix) || record.Key[:len(prefix)] != prefix) {
			return nil
		}

		// Search right subtree of this record
		if i+1 < len(node.children) {
			if result := bt.findNextPrefixMatchAfterKey(prefix, afterKey, node.children[i+1], tombstonedKeys); result != nil {
				return result
			}
		}
	}

	return nil
}

// findNextRangeMatchAfterKey finds the first non-tombstoned record within the given range
// that comes lexicographically after the given key.
func (bt *BTree) findNextRangeMatchAfterKey(rangeStart string, rangeEnd string, afterKey string, node *Node, tombstonedKeys *[]string) *model.Record {
	if node == nil {
		return nil
	}

	if node.isLeaf {
		// Find the first record > afterKey
		startIndex := sort.Search(len(node.records), func(i int) bool {
			return node.records[i].Key > afterKey
		})

		// Check each record from startIndex onwards
		for i := startIndex; i < len(node.records); i++ {
			record := node.records[i]

			// Check if key is within range [rangeStart, rangeEnd] (inclusive)
			if record.Key < rangeStart {
				continue
			}
			if record.Key > rangeEnd {
				// We've gone beyond the range
				return nil
			}

			// Check if this record is tombstoned locally
			if record.IsDeleted() {
				addToTombstoned(record.Key, tombstonedKeys)
				continue
			}

			// Check if key is tombstoned in more recent structures
			if isKeyTombstoned(record.Key, tombstonedKeys) {
				continue
			}

			// Found a valid, non-tombstoned record within range
			return record
		}
		return nil
	}

	// For internal nodes, find the appropriate child to start searching from
	startChildIndex := sort.Search(len(node.records), func(i int) bool {
		return node.records[i].Key > afterKey
	})

	// Search the appropriate child subtree first
	if startChildIndex < len(node.children) {
		if result := bt.findNextRangeMatchAfterKey(rangeStart, rangeEnd, afterKey, node.children[startChildIndex], tombstonedKeys); result != nil {
			return result
		}
	}

	// Check records in this internal node and their right subtrees
	for i := startChildIndex; i < len(node.records); i++ {
		record := node.records[i]

		// Only consider records that come after afterKey and are within range [rangeStart, rangeEnd] (inclusive)
		if record.Key > afterKey && record.Key >= rangeStart && record.Key <= rangeEnd {
			// Check if this record is tombstoned locally
			if record.IsDeleted() {
				addToTombstoned(record.Key, tombstonedKeys)
			} else if !isKeyTombstoned(record.Key, tombstonedKeys) {
				// Found a valid, non-tombstoned record
				return record
			}
		}

		// If record key is > rangeEnd, we've gone too far
		if record.Key > rangeEnd {
			return nil
		}

		// Search right subtree of this record
		if i+1 < len(node.children) {
			if result := bt.findNextRangeMatchAfterKey(rangeStart, rangeEnd, afterKey, node.children[i+1], tombstonedKeys); result != nil {
				return result
			}
		}
	}

	return nil
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

// ScanForPrefix scans records with the given prefix and adds keys to bestKeys.
// Only keys are added for memory efficiency - use Get() to retrieve full records.
func (bt *BTree) ScanForPrefix(
	prefix string,
	tombstonedKeys *[]string,
	bestKeys *[]string,
	pageSize int,
	pageNumber int,
) {
	if bt.root == nil {
		return
	}

	// Create a set of tombstoned keys for O(1) lookup
	tombstonedSet := make(map[string]bool)
	if tombstonedKeys != nil {
		for _, key := range *tombstonedKeys {
			tombstonedSet[key] = true
		}
	}

	// Create a set of existing best keys to avoid duplicates
	bestKeysSet := make(map[string]bool)
	if bestKeys != nil {
		for _, key := range *bestKeys {
			bestKeysSet[key] = true
		}
	}

	// Collect all matching keys from this memtable
	bt.scanForPrefixRecursive(bt.root, prefix, tombstonedSet, bestKeysSet, tombstonedKeys, bestKeys)
}

// scanForPrefixRecursive performs the actual recursive scan of the B-tree
func (bt *BTree) scanForPrefixRecursive(
	node *Node,
	prefix string,
	tombstonedSet map[string]bool,
	bestKeysSet map[string]bool,
	tombstonedKeys *[]string,
	bestKeys *[]string,
) {
	if node == nil {
		return
	}

	if node.isLeaf {
		// Scan all records in this leaf node
		for _, record := range node.records {
			bt.processRecordForScan(record, prefix, tombstonedSet, bestKeysSet, tombstonedKeys, bestKeys)
		}
		return
	}

	// For internal nodes, we need to traverse both records and children
	for i := 0; i < len(node.records); i++ {
		// Visit left child first
		if i < len(node.children) {
			bt.scanForPrefixRecursive(node.children[i], prefix, tombstonedSet, bestKeysSet, tombstonedKeys, bestKeys)
		}

		// Process the record at this position
		bt.processRecordForScan(node.records[i], prefix, tombstonedSet, bestKeysSet, tombstonedKeys, bestKeys)
	}

	// Visit the rightmost child if it exists
	if len(node.children) > len(node.records) {
		bt.scanForPrefixRecursive(node.children[len(node.records)], prefix, tombstonedSet, bestKeysSet, tombstonedKeys, bestKeys)
	}
}

// processRecordForScan processes a single record during the scan operation
func (bt *BTree) processRecordForScan(
	record *model.Record,
	prefix string,
	tombstonedSet map[string]bool,
	bestKeysSet map[string]bool,
	tombstonedKeys *[]string,
	bestKeys *[]string,
) {
	// Check if key matches prefix
	if len(record.Key) < len(prefix) || record.Key[:len(prefix)] != prefix {
		return
	}

	// Skip if already tombstoned in newer structures
	if tombstonedSet[record.Key] {
		return
	}

	// Skip if already found in newer memtables
	if bestKeysSet[record.Key] {
		return
	}

	// If this record is a tombstone, add to tombstoned set
	if record.IsDeleted() {
		if tombstonedKeys != nil {
			*tombstonedKeys = append(*tombstonedKeys, record.Key)
			tombstonedSet[record.Key] = true
		}
		return
	}

	// Add to best keys (maintaining sorted order)
	if bestKeys != nil {
		*bestKeys = insertKeySorted(*bestKeys, record.Key)
		bestKeysSet[record.Key] = true
	}
}

// insertKeySorted inserts a key in sorted order into the slice
func insertKeySorted(keys []string, newKey string) []string {
	// Binary search for insertion point
	left, right := 0, len(keys)
	for left < right {
		mid := (left + right) / 2
		if keys[mid] < newKey {
			left = mid + 1
		} else {
			right = mid
		}
	}

	// Insert at the found position
	keys = append(keys, "")
	copy(keys[left+1:], keys[left:])
	keys[left] = newKey
	return keys
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

// RetrieveSortedRecords returns all records in sorted order (in-order traversal).
func (bt *BTree) RetrieveSortedRecords() []model.Record { // Changed return type
	var records []model.Record // Changed slice element type
	bt.inOrderTraversal(bt.root, &records)

	return records
}

// inOrderTraversal performs an in-order traversal of the B-tree to collect records.
func (bt *BTree) inOrderTraversal(node *Node, records *[]model.Record) { // Changed slice element type in parameter
	if node == nil {
		return
	}

	for i, rec := range node.records { // Assuming node.records still holds []*model.Record
		// Visit left child before processing record
		if !node.isLeaf {
			bt.inOrderTraversal(node.children[i], records)
		}
		// Deep copy the Value field to avoid sharing the underlying array
		copiedRecord := *rec
		if rec.Value != nil {
			copiedRecord.Value = make([]byte, len(rec.Value))
			copy(copiedRecord.Value, rec.Value)
		}
		*records = append(*records, copiedRecord)
	}

	// Visit the rightmost child after all records
	if !node.isLeaf && len(node.children) > len(node.records) {
		bt.inOrderTraversal(node.children[len(node.records)], records)
	}
}

// Size returns the number of active (non-tombstoned) keys.
func (bt *BTree) Size() int {
	return bt.activeRecords
}

// Capacity returns the maximum number of distinct keys allowed.
func (bt *BTree) Capacity() int {
	return bt.capacity
}

// Flush persists the memtable contents to disk (SSTable).
func (bt *BTree) Flush(index int) error {

	sortedRecords := bt.RetrieveSortedRecords()

	err := sstable.PersistMemtable(sortedRecords, index)
	if err != nil {
		return fmt.Errorf("failed to flush B-tree memtable: %v", err)
	}

	return nil
}

// TotalEntries returns the number of distinct keys (active + tombstoned).
func (bt *BTree) TotalEntries() int {
	return bt.totalRecords
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
