package merkle_tree

import (
	"crypto/sha256"
)

// MerkleNode represents a node in the Merkle tree.
// Each node contains a hashed value and pointers to its left and right children.
// The hashed value is a hash of the combined hash of its child nodes.
type MerkleNode struct {
	hashedValue []byte      // The hash value of the node
	leftChild   *MerkleNode // Pointer to the left child node
	rightChild  *MerkleNode // Pointer to the right child node
}

// TODO: Consult with TA for the size of the hash!
// A Merkle tree is a binary tree used to efficiently verify the integrity of data.
// The tree is defined by its root node, known as the Merkle root, which is a hash that represents the entire tree.
type MerkleTree struct {
	merkleRoot *MerkleNode // Pointer to the root node of the Merkle tree
}

// TODO: params will be blocks when that code is done, string for NOW
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
		nodes = append(nodes, &MerkleNode{hashedValue: hashedValue[:]})
	}

	for len(nodes) > 1 {
		if len(nodes)%2 == 1 {
			nodes = append(nodes, &MerkleNode{})
		}
		newNodes := make([]*MerkleNode, 0, len(nodes)/2)
		for i := 0; i < len(nodes); i += 2 {
			left := nodes[i]
			right := nodes[i+1]
			combinedHash := append(left.hashedValue, right.hashedValue...)
			hashedValue := sha256.Sum256(combinedHash)
			newNodes = append(newNodes, &MerkleNode{hashedValue: hashedValue[:], leftChild: left, rightChild: right})
		}
		nodes = newNodes
	}
	return &MerkleTree{nodes[0]}

}

//TODO: Compare merkle trees
//TODO: Traverse tree
//TODO: Serialization
//TODO: Derialization
