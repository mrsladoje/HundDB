package btree

import (
	"bytes"
	"hund-db/model"
)

type Node struct {
	// parent   *Node
	children []*Node
	records  []*model.Record
}

type BTree struct {
	root *Node
}

func (btree *BTree) Get(key []byte) *Node {
	if btree.root == nil {
		return nil
	}
	node, _ := btree.getKey(key, btree.root)
	return node
}

func (btree *BTree) getKey(key []byte, current *Node) (*Node, int) {
	for i, record := range current.records {
		if bytes.Equal(key, record.Key) {
			return current, i
		}
	}

	for _, child := range current.children {
		node, index := btree.getKey(key, child)
		if node != nil {
			return node, index
		}
	}

	return nil, -1
}

// TODO: automatic compaction at ~30% for deletion (chat with claude), beautiful go doc comments (chat with gpt for go) 2.1.
