package merkle_tree

//cSpell:ignore merkle

import (
	"encoding/hex"
	"testing"
)

// TestNewMerkleTree tests the NewMerkleTree function.
func TestNewMerkleTree(t *testing.T) {
	tests := []struct {
		blocks       []string
		expectedRoot string
	}{
		{[]string{"block1", "block2", "block3", "block4"}, "eae5935f45caf2924b95ca42f623023e857c2d8a4953fd5a41509c0040fdc6c3"},
		{[]string{"block1", "block2", "block3"}, "b7acd0f2addac5607313fd9f0eeb8b769d083c945d12a975ae90a972644f48ad"},
		{[]string{"block1"}, "9a59c5f8229aab55e9f855173ef94485aab8497eea0588f365c871d6d0561722"},
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

		actualRootHash := hex.EncodeToString(tree.merkleRoot.hashedValue[:])
		if actualRootHash != test.expectedRoot {
			t.Errorf("Expected root hash %s, got %s", test.expectedRoot, actualRootHash)
		}
	}
}

// TestHeight tests the Height method of the MerkleTree.
func TestHeight(t *testing.T) {
	tests := []struct {
		blocks         []string
		expectedHeight uint64
	}{
		{[]string{"block1", "block2", "block3", "block4"}, 3},
		{[]string{"block1", "block2", "block3"}, 3},
		{[]string{"block1"}, 1},
		{[]string{}, 0},
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

		actualHeight := tree.Height()
		if actualHeight != test.expectedHeight {
			t.Errorf("Expected height %d, got %d", test.expectedHeight, actualHeight)
		}
	}
}

// TestMaxNumOfNodes tests the MaxNumOfNodes method of the MerkleTree.
func TestMaxNumOfNodes(t *testing.T) {
	tests := []struct {
		blocks           []string
		expectedMaxNodes uint64
	}{
		{[]string{"block1", "block2", "block3", "block4"}, 7},
		{[]string{"block1", "block2", "block3"}, 7},
		{[]string{"block1"}, 1},
		{[]string{}, 0},
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

		actualMaxNodes := tree.MaxNumOfNodes()
		if actualMaxNodes != test.expectedMaxNodes {
			t.Errorf("Expected max nodes %d, got %d", test.expectedMaxNodes, actualMaxNodes)
		}
	}
}

// TestValidate tests the Validate method of the MerkleTree.
func TestValidate(t *testing.T) {
	tree1 := NewMerkleTree([]string{"block1", "block2", "block3", "block4"})
	tree2 := NewMerkleTree([]string{"block1", "block2", "block3", "block4"})
	tree3 := NewMerkleTree([]string{"block1", "block2", "block3"})

	if !tree1.Validate(tree2) {
		t.Error("Expected tree1 to validate tree2")
	}

	if tree1.Validate(tree3) {
		t.Error("Expected tree1 not to validate tree3")
	}
}
