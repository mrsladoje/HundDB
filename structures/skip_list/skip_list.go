package skip_list

import (
	"bytes"
	"encoding/binary"
	"math/rand"
)

type Node struct {
	key       string
	value     string
	nextNodes []*Node // i-th Node is at the i-th level
}

// NewNode creates a new node for the Skip List.
// key: the key of the node.
// value: the value of the node.
// height: the number of levels the node spans.
func NewNode(key string, value string, height uint64) *Node {
	return &Node{
		key:       key,
		value:     value,
		nextNodes: make([]*Node, height),
	}
}

// SkipList is a probabilistic data structure that allows for fast search, insertion, and deletion operations.
// It maintains multiple levels of linked lists, where each level is a subset of the level below it.
// The lowest level contains all the elements, while higher levels contain fewer elements, providing a hierarchical structure.
// This structure allows for efficient logarithmic time complexity for search, insertion, and deletion operations.
// It works with string keys and values, and uses a randomization technique to determine the level of each new node.
// Nodes are linked so that nextNodes[i] points to the next node at i-th height.
type SkipList struct {
	maxHeight     uint64 // Maximum height of the Skip List
	currentHeight uint64 // Current height of the Skip List
	head          *Node  // Pointer to the head node of the Skip List
}

// NewSkipList creates a new SkipList instance.
// maxHeight: the maximum number of levels in the Skip List.
func NewSkipList(maxHeight uint64) *SkipList {
	head := NewNode("", "", maxHeight) // Empty string is lexicographically lesser than any other string.
	return &SkipList{
		maxHeight:     maxHeight,
		currentHeight: 1,
		head:          head,
	}
}

// Check verifies whether a given key-value pair exists in the Skip List.
// key: the key to check for.
func (s *SkipList) Check(key string) bool {
	currentNode := s.head

	// Iterates through each level as long as the target key is larger than the next node.
	for i := int(s.currentHeight) - 1; i >= 0; i-- {
		for currentNode.nextNodes[i] != nil && key >= currentNode.nextNodes[i].key {
			currentNode = currentNode.nextNodes[i]
		}

		if currentNode.key == key {
			return true
		}
	}

	return false
}

// roll generates a random height for a new node.
// The height is limited by the maximum height of the Skip List.
func (s *SkipList) roll() uint64 {
	var height uint64 = 1
	for rand.Int31n(2) == 1 && height < s.maxHeight {
		height++
	}
	return height
}

// Add inserts a new key-value pair into the Skip List, while checking for duplicates.
// key: the key to be added.
// value: the value to be added.
func (s *SkipList) Add(key string, value string) {
	if s.Check(key) {
		return
	}

	nodesToUpdate := make([]*Node, s.maxHeight)
	currentNode := s.head

	// Iterates through each level and records the last visited node at that level.
	for i := int(s.currentHeight) - 1; i >= 0; i-- {
		for currentNode.nextNodes[i] != nil && currentNode.nextNodes[i].key < key {
			currentNode = currentNode.nextNodes[i]
		}
		nodesToUpdate[i] = currentNode
	}

	height := s.roll() // Random height for the new node.

	// If the new height exceeds the current height, link the head node to the new node.
	if height > s.currentHeight {
		for i := s.currentHeight; i < height; i++ {
			nodesToUpdate[i] = s.head
		}
		s.currentHeight = height
	}

	newNode := NewNode(key, value, height)

	// Links the new node with the existing nodes at all levels.
	for i := uint64(0); i < height; i++ {
		newNode.nextNodes[i] = nodesToUpdate[i].nextNodes[i]
		nodesToUpdate[i].nextNodes[i] = newNode
	}
}

// Delete removes a key-value pair from the Skip List.
// key: the key of the node to be removed.
func (s *SkipList) Delete(key string) {
	if !s.Check(key) {
		return
	}

	nodesToUpdate := make([]*Node, s.maxHeight)
	currentNode := s.head
	var nodeToDelete *Node

	// Iterates through the Skip List to locate the node to delete.
	for i := int(s.currentHeight) - 1; i >= 0; i-- {
		for currentNode.nextNodes[i] != nil && key > currentNode.nextNodes[i].key {
			currentNode = currentNode.nextNodes[i]
		}

		// Records all nodes that point to the node to be deleted.
		if currentNode.nextNodes[i] != nil && currentNode.nextNodes[i].key == key {
			nodesToUpdate[i] = currentNode
			nodeToDelete = currentNode.nextNodes[i]
		}
	}

	// Logically removes the node by adjusting pointers.
	for i := uint64(0); i < s.currentHeight; i++ {
		if nodesToUpdate[i] != nil && nodesToUpdate[i].nextNodes[i] != nil {
			nodesToUpdate[i].nextNodes[i] = nodeToDelete.nextNodes[i]
		}
	}

	// Removes the top level if it becomes empty.
	for s.currentHeight > 1 && s.head.nextNodes[s.currentHeight-1] == nil {
		s.currentHeight--
	}
}

// Serializes the Skip List into a byte array.
// Iterates through the bottom level and serializes each node.
func (s *SkipList) Serialize() []byte {
	var buffer bytes.Buffer

	binary.Write(&buffer, binary.LittleEndian, s.maxHeight)
	binary.Write(&buffer, binary.LittleEndian, s.currentHeight)

	currentNode := s.head
	for currentNode != nil {
		binary.Write(&buffer, binary.LittleEndian, uint64(len(currentNode.key)))
		buffer.WriteString(currentNode.key)

		binary.Write(&buffer, binary.LittleEndian, uint64(len(currentNode.value)))
		buffer.WriteString(currentNode.value)

		currentNode = currentNode.nextNodes[0]
	}

	return buffer.Bytes()
}

// Deserializes the byte array into a Skip List.
func Deserialize(data []byte) *SkipList {
	buffer := bytes.NewReader(data)

	var maxHeight, currentHeight uint64

	binary.Read(buffer, binary.LittleEndian, &maxHeight)
	binary.Read(buffer, binary.LittleEndian, &currentHeight)

	skipList := NewSkipList(maxHeight)
	skipList.currentHeight = currentHeight

	var keyLen, valueLen uint64
	for buffer.Len() > 0 {
		binary.Read(buffer, binary.LittleEndian, &keyLen)
		key := make([]byte, keyLen)
		buffer.Read(key)

		binary.Read(buffer, binary.LittleEndian, &valueLen)
		value := make([]byte, valueLen)
		buffer.Read(value)

		skipList.Add(string(key), string(value))
	}

	return skipList
}
