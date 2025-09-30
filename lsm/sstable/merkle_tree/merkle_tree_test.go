package merkle_tree

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestNewMerkleTree(t *testing.T) {
	tests := []struct {
		blocks       []string
		expectedRoot string
	}{
		{[]string{"block1", "block2", "block3", "block4"}, "52b6ec49b1ed0eed625adcef9073f0c2"},
		{[]string{"block1", "block2", "block3"}, "ac491d1ea728dc2fb488cf3bc8b3a898"},
		{[]string{"block1", "block2"}, "423a3d793bb8c91da536a90361dc09ff"},
		{[]string{"block1"}, "9dd085e96a8813854138d29b8a6fdf58"},
		{[]string{}, ""},
	}

	for _, test := range tests {
		tree, err := NewMerkleTree(test.blocks, false)
		if err != nil {
			t.Error(err.Error())
		}

		if len(test.blocks) == 0 {
			if tree == nil {
				t.Fatal("Expected non-nil MerkleTree for empty blocks")
			}
			if tree.merkleRoot == nil || tree.merkleRoot.hashedValue != md5.Sum([]byte{}) {
				t.Fatal("Expected MerkleTree.merkleRoot.hashedValue to be md5.Sum([]byte{}) for empty blocks")
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
		{[]string{"block1", "block2", "block3", "block4"}, 2},
		{[]string{"block1", "block2", "block3"}, 2},
		{[]string{"block1"}, 0},
	}

	for _, test := range tests {
		tree, err := NewMerkleTree(test.blocks, false)
		if err != nil {
			t.Error(err.Error())
		}

		if len(test.blocks) == 0 {
			if tree == nil {
				t.Fatal("Expected non-nil MerkleTree for empty blocks")
			}
			if tree.merkleRoot != nil {
				t.Fatal("Expected MerkleTree.merkleRoot to be nil for empty blocks")
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
	}

	for _, test := range tests {
		tree, err := NewMerkleTree(test.blocks, false)
		if err != nil {
			t.Error(err.Error())
		}

		if len(test.blocks) == 0 {
			if tree == nil {
				t.Fatal("Expected non-nil MerkleTree for empty blocks")
			}
			if tree.merkleRoot != nil {
				t.Fatal("Expected MerkleTree.merkleRoot to be nil for empty blocks")
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

	tree1, err := NewMerkleTree([]string{"block1", "block2", "block3", "block4"}, false)
	if err != nil {
		t.Error(err.Error())
	}
	tree2, err := NewMerkleTree([]string{"block1", "block2", "block3", "block4"}, false)
	if err != nil {
		t.Error(err.Error())
	}
	tree3, err := NewMerkleTree([]string{"block1", "block2", "block3"}, false)
	if err != nil {
		t.Error(err.Error())
	}
	// tree4, err := NewMerkleTree([]string{"block1", "block3"})
	// if err != nil {
	// 	t.Error(err.Error())
	// }

	same, _, _ := tree1.Validate(tree2)
	if !same {
		t.Error("Expected tree1 to be the same as tree2")
	}

	same, differences1, differences2 := tree1.Validate(tree3)
	if same {
		t.Error("Expected tree1 not to be the same as tree3")
	}
	if len(differences1) != len(differences2) {
		t.Errorf("Expected tree1 to have the same number of differences as tree3, got %d and %d", len(differences1), len(differences2))
	}
	if differences1[0].hashedValue != md5.Sum([]byte("block4")) {
		t.Error("Unexpected difference node!")
	}
	if differences2[0].hashedValue != [16]byte{} {
		t.Error("Unexpected difference node!")
	}

	// same, differences1, differences2 = tree1.Validate(tree4)
	// if same {
	// 	t.Error("Expected tree1 not to be the same as tree4")
	// }
	// if len(differences1) != len(differences2) {
	// 	t.Errorf("Expected tree1 to have the same number of differences as tree4, got %d and %d", len(differences1), len(differences2))
	// }
	// if differences1[0].hashedValue != md5.Sum([]byte("block4")) {
	// 	t.Error("Unexpected difference node!")
	// }
	// if differences2[0].hashedValue != [16]byte{} {
	// 	t.Error("Unexpected difference node!")
	// }

}

// Test BFS Traversal
func TestBFS(t *testing.T) {
	blocks := []string{"A", "B", "C", "D"}
	tree, err := NewMerkleTree(blocks, false)
	if err != nil {
		t.Error(err.Error())
	}

	var bfsHashes [][]byte
	tree.BFS(func(node *MerkleNode) {
		bfsHashes = append(bfsHashes, node.hashedValue[:])
	})

	expectedHashes := [][]byte{
		tree.merkleRoot.hashedValue[:],
		tree.merkleRoot.leftChild.hashedValue[:],
		tree.merkleRoot.rightChild.hashedValue[:],
		tree.merkleRoot.leftChild.leftChild.hashedValue[:],
		tree.merkleRoot.leftChild.rightChild.hashedValue[:],
		tree.merkleRoot.rightChild.leftChild.hashedValue[:],
		tree.merkleRoot.rightChild.rightChild.hashedValue[:],
	}

	for i := range expectedHashes {
		if i >= len(bfsHashes) {
			t.Errorf("BFS traversal did not visit all expected nodes")
			break
		}
		if string(bfsHashes[i]) != string(expectedHashes[i]) {
			t.Errorf("BFS order mismatch at index %d", i)
		}
	}
}

// Test DFS Traversal
func TestDFS(t *testing.T) {
	blocks := []string{"A", "B", "C", "D"}
	tree, err := NewMerkleTree(blocks, false)
	if err != nil {
		t.Error(err.Error())
	}

	var dfsHashes [][]byte
	tree.DFS(func(node *MerkleNode) {
		dfsHashes = append(dfsHashes, node.hashedValue[:])
	})

	expectedHashes := [][]byte{
		tree.merkleRoot.hashedValue[:],
		tree.merkleRoot.leftChild.hashedValue[:],
		tree.merkleRoot.leftChild.leftChild.hashedValue[:],
		tree.merkleRoot.leftChild.rightChild.hashedValue[:],
		tree.merkleRoot.rightChild.hashedValue[:],
		tree.merkleRoot.rightChild.leftChild.hashedValue[:],
		tree.merkleRoot.rightChild.rightChild.hashedValue[:],
	}

	for i := range expectedHashes {
		if i >= len(dfsHashes) {
			t.Errorf("DFS traversal did not visit all expected nodes")
			break
		}
		if string(dfsHashes[i]) != string(expectedHashes[i]) {
			t.Errorf("DFS order mismatch at index %d", i)
		}
	}
}

// Test Serialization & Deserialization
func TestSerializeDeserialize(t *testing.T) {
	tests := []struct {
		blocks []string
	}{
		{[]string{"block1", "block2", "block3", "block4"}},
		{[]string{"block1", "block2", "block3"}},
		{[]string{"block1", "block2"}},
		{[]string{"block1"}},
	}

	for _, test := range tests {

		originalTree, err := NewMerkleTree(test.blocks, false)
		if err != nil {
			t.Error(err.Error())
		}
		var dfsOriginalHashes [][]byte
		originalTree.DFS(func(node *MerkleNode) {
			dfsOriginalHashes = append(dfsOriginalHashes, node.hashedValue[:])
		})

		serializedData := originalTree.Serialize()

		deserializedTree := Deserialize(serializedData)
		var dfsCopyHashes [][]byte
		deserializedTree.DFS(func(node *MerkleNode) {
			dfsCopyHashes = append(dfsCopyHashes, node.hashedValue[:])
		})

		if len(dfsOriginalHashes) != len(dfsCopyHashes) {
			t.Errorf("Expected deserializes tree to have the same number of nodes as the original")
		}

		for i := 0; i < len(dfsOriginalHashes); i++ {
			if !bytes.Equal(dfsOriginalHashes[i], dfsCopyHashes[i]) {
				t.Errorf("Expected serialized-deserialized tree to match original")
				fmt.Println("ORIGINAL:", hex.EncodeToString(dfsOriginalHashes[i]), "\nCOPY: ", hex.EncodeToString(dfsCopyHashes[i]))
				if i >= len(dfsOriginalHashes)-1 {
					fmt.Println()
				}
			}
		}

		deserializedData := deserializedTree.Serialize()

		if len(deserializedData) != len(serializedData) {
			t.Errorf("Expected deserializes data to have the same number of bytes as the original")
		}

		if !bytes.Equal(serializedData, deserializedData) {
			t.Errorf("Expected serialized-deserialized data to match original")
			fmt.Println("Original data: \t", hex.EncodeToString(serializedData))
			fmt.Println("Copy data: \t", hex.EncodeToString(deserializedData))

			// Detailed report
			for i := 0; i < len(serializedData); i += 16 {
				fmt.Println("Original data: \t", hex.EncodeToString(serializedData[i:i+16]))
				fmt.Println("Copy data: \t", hex.EncodeToString(deserializedData[i:i+16]))
			}
		}
	}
}
