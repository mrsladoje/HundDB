package merkle_tree

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestNewMerkleTree tests the NewMerkleTree function.
func TestNewMerkleTree(t *testing.T) {
	tests := []struct {
		blocks       []string
		expectedRoot string
	}{
		{[]string{"block1", "block2", "block3", "block4"}, computeExpectedRootHash([]string{"block1", "block2", "block3", "block4"})},
		{[]string{"block1", "block2", "block3"}, computeExpectedRootHash([]string{"block1", "block2", "block3"})},
		{[]string{"block1"}, computeExpectedRootHash([]string{"block1"})},
		{[]string{}, ""},
	}

	for _, test := range tests {
		tree := NewMerkleTree(test.blocks)

		if len(test.blocks) == 0 {
			if tree != nil {
				t.Fatal("Expected nil MerkleTree for empty blocks")
			}
			continue
		}

		if tree == nil {
			t.Fatal("Expected non-nil MerkleTree")
		}

		actualRootHash := hex.EncodeToString(tree.merkleRoot.hashedValue)
		if actualRootHash != test.expectedRoot {
			t.Errorf("Expected root hash %s, got %s", test.expectedRoot, actualRootHash)
		}
	}
}

// computeExpectedRootHash computes the expected root hash for the given blocks.
func computeExpectedRootHash(blocks []string) string {
	if len(blocks) == 0 {
		return ""
	}

	var nodes [][]byte
	for _, block := range blocks {
		hash := sha256.Sum256([]byte(block))
		nodes = append(nodes, hash[:])
	}

	for len(nodes) > 1 {
		if len(nodes)%2 == 1 {
			nodes = append(nodes, nodes[len(nodes)-1])
		}
		var newNodes [][]byte
		for i := 0; i < len(nodes); i += 2 {
			combinedHash := append(nodes[i], nodes[i+1]...)
			hash := sha256.Sum256(combinedHash)
			newNodes = append(newNodes, hash[:])
		}
		nodes = newNodes
	}
	return hex.EncodeToString(nodes[0])
}
