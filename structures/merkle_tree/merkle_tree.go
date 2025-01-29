package merkle_tree

//cSpell:ignore merkle

import (
	"crypto/sha256"
	"math"
)

// MerkleNode represents a node in the Merkle tree.
// Each node contains a hashed value and pointers to its left and right children.
// The hashed value is a hash of the combined hash of its child nodes.
type MerkleNode struct {
	hashedValue [32]byte    // The hash value of the node
	leftChild   *MerkleNode // Pointer to the left child node
	rightChild  *MerkleNode // Pointer to the right child node
}

// TODO: Consult with TA for the size of the hash!
// A Merkle tree is a binary tree used to efficiently verify the integrity of data.
// The tree is defined by its root node, known as the Merkle root, which is a hash that represents the entire tree.
type MerkleTree struct {
	merkleRoot *MerkleNode // Pointer to the root node of the Merkle tree
}

// TODO: Params will be blocks when that code is done, string for NOW, it will use block manager!
// NewMerkleTree creates a new Merkle tree from the given blocks, and to make it a binary tree
// it will add neutral nodes(nil for all attributes) at the right most side of the tree
// blocks: a slice of blocks with which the Merkle tree will be created.
// returns: Merkle tree instance or nil if blocks is empty.
func NewMerkleTree(blocks []string) *MerkleTree {
	if len(blocks) == 0 {
		return nil
	}

	// Create leaf nodes
	nodes := make([]*MerkleNode, 0, len(blocks))
	for _, block := range blocks {
		hashedValue := sha256.Sum256([]byte(block))
		nodes = append(nodes, &MerkleNode{hashedValue: hashedValue})
	}

	for len(nodes) > 1 {
		if len(nodes)%2 == 1 {
			nodes = append(nodes, &MerkleNode{})
		}
		newNodes := make([]*MerkleNode, 0, len(nodes)/2)
		for i := 0; i < len(nodes); i += 2 {
			left := nodes[i]
			right := nodes[i+1]
			combinedHash := make([]byte, 0, 64)
			combinedHash = append(combinedHash, left.hashedValue[:]...)
			combinedHash = append(combinedHash, right.hashedValue[:]...)
			hashedValue := sha256.Sum256(combinedHash)
			newNodes = append(newNodes, &MerkleNode{hashedValue: hashedValue, leftChild: left, rightChild: right})
		}
		nodes = newNodes
	}
	return &MerkleTree{nodes[0]}

}

// Height calculates the height of the Merkle tree. Runs in O(logN) time.
// The height of a tree is the number of nodes on the longest path from the root to a leaf,
// including the root itself. Tree that is only made out of the root has height of 1.
// returns: The height of the Merkle tree as a uint64.
func (mTree *MerkleTree) Height() uint64 {
	var height uint64 = 1
	currentNode := mTree.merkleRoot
	for currentNode.leftChild != nil {
		currentNode = currentNode.leftChild
		height++
	}
	return height
}

// MaxNumOfNodes calculates the maximum number of nodes that could possibly be present in this merkle tree.
// It bases its approach on the preposition that the tree is a perfect binary tree, and if not it has a smaller number
// of nodes based on the Merkle Tree creation algorithm.
// returns: The max possible number of nodes for a Merkle tree of this height
func (mTree *MerkleTree) MaxNumOfNodes() uint64 {
	return uint64(math.Pow(2, float64(mTree.Height()))) - 1
}

// Validate method of a Merkle tree compares the roots of the two Merkle trees and returns
// a bool value to represent the result of the comparison.
func (mTree *MerkleTree) Validate(otherMTree *MerkleTree) bool {
	for i := 0; i < 32; i++ {
		if mTree.merkleRoot.hashedValue[i] != otherMTree.merkleRoot.hashedValue[i] {
			return false
		}
	}
	return true
}

// TODO: Serialization
// TODO: Deserialization
// TODO: Consult with TA on witch method to use for serialization and deserialization
//  1. For Exact Reconstruction:
//     Serialize all nodes, including empty ones, simple deserialization.
//  2. For Compact Storage or Transmission:
//     Serialize only relevant nodes (non-empty nodes).
//     Ensure deserialization logic can infer the complete tree structure.
//  3. For Data-Centric Applications:
//     Serialize only the leaf nodes.
//     Reconstruct the internal nodes and root hash as needed.
//  4. For Validation (Most likely the optimal one):
//     Only serialize the merkle root
//
// TODO: Check with the TA if the merkle proof is needed (99% sure it isn't)
//
// Traverse tree will rely on the needed serialization method(If not the 4. one).
func (mTree *MerkleTree) Traverse() {
}
