package merkle_tree

import (
	"container/list"
	"crypto/md5"
	"errors"
	"math"
)

var ErrEmptyTree = errors.New("cannot create Merkle tree from empty data")

// MerkleNode represents a node in the Merkle tree.
// Each node contains a hashed value and pointers to its left and right children.
// The hashed value is a hash of the combined hash of its child nodes.
type MerkleNode struct {
	hashedValue [16]byte    // The hash value of the node
	leftChild   *MerkleNode // Pointer to the left child node
	rightChild  *MerkleNode // Pointer to the right child node
}

// A Merkle tree is a binary tree used to efficiently verify the integrity of data.
// The tree is defined by its root node, known as the Merkle root, which is a hash that represents the entire tree.
type MerkleTree struct {
	merkleRoot *MerkleNode // Pointer to the root node of the Merkle tree
}

// TODO: Params will be blocks when that code is done, string for NOW, it will use block manager!
// NewMerkleTree creates a new Merkle tree from the given blocks, and to make it a binary tree
// it will add neutral nodes(nil for all attributes) at the right most side of the tree
// blocks: a slice of blocks with which the Merkle tree will be created.
// returns: Merkle tree instance and an error if there was no blocks to construct the tree.
func NewMerkleTree(blocks []string) (*MerkleTree, error) {
	if len(blocks) == 0 {
		return nil, ErrEmptyTree
	}

	// Create leaf nodes
	nodes := make([]*MerkleNode, 0, len(blocks))
	for _, block := range blocks {
		hashedValue := md5.Sum([]byte(block))
		nodes = append(nodes, &MerkleNode{hashedValue: hashedValue})
	}

	for len(nodes) > 1 {
		if len(nodes)%2 == 1 {
			nodes = append(nodes, &MerkleNode{})
		}
		newNodes := make([]*MerkleNode, 0, len(nodes)/2)
		for i := 0; i < len(nodes); i += 2 {
			leftNode := nodes[i]
			rightNode := nodes[i+1]
			combinedHash := make([]byte, 32)
			copy(combinedHash[:16], leftNode.hashedValue[:])
			copy(combinedHash[16:], rightNode.hashedValue[:])
			hashedValue := md5.Sum(combinedHash)
			newNodes = append(newNodes, &MerkleNode{
				hashedValue: hashedValue,
				leftChild:   leftNode,
				rightChild:  rightNode,
			})
		}
		nodes = newNodes
	}
	return &MerkleTree{nodes[0]}, nil

}

// Height calculates the height of the Merkle tree. Runs in O(logN) time.
// The height of a tree is the number of nodes on the longest path from the root to a leaf,
// excluding the root. Tree that is only made out of the root has height of 0.
// returns: The height of the Merkle tree as a uint64.
func (mTree *MerkleTree) Height() uint64 {
	var height uint64 = 0
	currentNode := mTree.merkleRoot
	for currentNode.leftChild != nil {
		currentNode = currentNode.leftChild
		height++
	}
	return height
}

// MaxNumOfNodes calculates the maximum number of nodes that could possibly be present in this merkle tree.
// The formula used to get it is: 2^(h+1) - 1
// It bases its approach on the preposition that the tree is a perfect binary tree, and if not it has a smaller number
// of nodes based on the Merkle Tree creation algorithm.
// returns: The max possible number of nodes for a Merkle tree of this height
func (mTree *MerkleTree) MaxNumOfNodes() uint64 {
	return uint64(math.Pow(2, float64(mTree.Height())+1)) - 1
}

// MaxNumOfLeafs calculates the maximum number of leaf nodes that could possibly be present in this merkle tree.
// The formula used to get it is: 2^h
// It bases its approach on the preposition that the tree is a perfect binary tree, and if not it has a smaller number
// of nodes based on the Merkle Tree creation algorithm.
// returns: The max possible number of nodes for a Merkle tree of this height
func (mTree *MerkleTree) MaxNumOfLeafs() uint64 {
	return uint64(math.Pow(2, float64(mTree.Height())))
}

// TODO: There are issues in this validate logic, when trees have different numbers of leaf nodes, also how should tree
// validation be done in theory, how should it behave if trees have different heights and different number of leafs.
// That will be checked and done when the use cases for validation become more clear.

// Validate method of a Merkle tree compares two Merkle trees and returns
// a bool value to represent the result of the comparison, and two slices of pointers to the
// leaf nodes that differ in order from left to right.
func (mTree *MerkleTree) Validate(otherMTree *MerkleTree) (bool, []*MerkleNode, []*MerkleNode) {

	if mTree.merkleRoot.hashedValue == otherMTree.merkleRoot.hashedValue {
		return true, nil, nil
	}

	var mismatchesTree1 []*MerkleNode
	var mismatchesTree2 []*MerkleNode

	DeepValidate(mTree.merkleRoot, otherMTree.merkleRoot, &mismatchesTree1, &mismatchesTree2)
	return false, mismatchesTree1, mismatchesTree2
}

// DeepValidate is a recursive helper function for the Validation method of the merkle tree.
// More documentation of it will be done when validation is perfected.
func DeepValidate(mNode, otherMNode *MerkleNode, mismatchesTree1, mismatchesTree2 *[]*MerkleNode) {
	if mNode.hashedValue == otherMNode.hashedValue {
		return
	} else if mNode.leftChild == nil && mNode.rightChild == nil && otherMNode.leftChild == nil && otherMNode.rightChild == nil {
		*mismatchesTree1 = append(*mismatchesTree1, mNode)
		*mismatchesTree2 = append(*mismatchesTree2, otherMNode)
	} else if mNode.hashedValue != [16]byte{} || otherMNode.hashedValue != [16]byte{} {
		DeepValidate(mNode.leftChild, otherMNode.leftChild, mismatchesTree1, mismatchesTree2)
		DeepValidate(mNode.rightChild, otherMNode.rightChild, mismatchesTree1, mismatchesTree2)
	}
}

// BFS(Breadth First Search) is a method of the Merkle Tree struct that will
// traverse the tree level by level, from left to right, starting from the root and going down to the leafs.
// The method takes in a method of a Merkle Node as a parameter, so the code is more elegant,
// rather then returning a slice of nodes in BFS order.
func (mTree *MerkleTree) BFS(visit func(*MerkleNode)) {
	queue := list.New()
	queue.PushBack(mTree.merkleRoot)

	for queue.Len() != 0 {
		currentNode := queue.Remove(queue.Front()).(*MerkleNode)
		visit(currentNode)
		if currentNode.leftChild != nil {
			queue.PushBack(currentNode.leftChild)
		}
		if currentNode.rightChild != nil {
			queue.PushBack(currentNode.rightChild)
		}
	}
}

// DFS(Depth First Search) is a method of the Merkle Tree struct that will
// traverse the tree in order: parent -> left child -> right child
// The method takes in a method of a Merkle Node as a parameter, so the code is more elegant,
// rather then returning a slice of nodes in DFS order.
func (mTree *MerkleTree) DFS(visit func(*MerkleNode)) {
	mTree.merkleRoot.DFS(visit)
}

func (mNode *MerkleNode) DFS(visit func(*MerkleNode)) {
	visit(mNode)
	if mNode.leftChild != nil {
		mNode.leftChild.DFS(visit)
	}
	if mNode.rightChild != nil {
		mNode.rightChild.DFS(visit)
	}

}

// Serialize, serializes the Merkle Node by taking in
// the pointer to a byte slice where the data is written
// The other attributes of the Merkle Node struct are pointers,
// and they are left out since when deserializing the Merkle Tree
// we cant allocate that memory piece for the Node.
// The structure is handled by the traversal method.
func (mNode *MerkleNode) Serialize(data *[]byte) {
	*data = append(*data, mNode.hashedValue[:]...)
}

// Serialize, serializes the whole Merkle Tree into a byte slice.
// The order is by depth first search.
// The method allocates the max num of possible nodes for that tree
// since continuous reallocation of the slice could be time expensive.
func (mTree *MerkleTree) Serialize() []byte {
	data := make([]byte, 0, mTree.MaxNumOfNodes()*16)
	mTree.DFS(func(node *MerkleNode) {
		node.Serialize(&data)
	})
	return data
}

// Deserialize, deserializes the merkle tree from the byte slice using its
// recursive helper function DeserializeDFS.
// It returns a Merkle Tree instance.
func Deserialize(data []byte) *MerkleTree {
	offset := 0
	root := DeserializeDFS(data, &offset)
	return &MerkleTree{merkleRoot: root}
}

// DeserializeDFS is a helper recursive helper function of the Deserialize function.
// It takes in the byte slice from whose content will be the merkle tree made and an
// offset, a pointer to an int value. The serialized data was in a DFS order, so the
// deserialization reads in that order too. It returns the pointer to a Root Merkle Node.
func DeserializeDFS(data []byte, offset *int) *MerkleNode {
	if *offset >= len(data) {
		return nil
	}

	var hash [16]byte
	copy(hash[:], data[*offset:*offset+16])
	*offset += 16

	node := &MerkleNode{hashedValue: hash}
	node.leftChild = DeserializeDFS(data, offset)
	node.rightChild = DeserializeDFS(data, offset)

	return node
}
