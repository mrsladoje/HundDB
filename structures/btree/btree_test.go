package btree

import (
	"hund-db/model"
	"testing"
)

func TestBTree_Get(t *testing.T) {
	record1 := &model.Record{Key: []byte("key1"), Value: []byte("value1"), Active: true, Timestamp: 123456}
	record2 := &model.Record{Key: []byte("key2"), Value: []byte("value2"), Active: true, Timestamp: 123457}
	record3 := &model.Record{Key: []byte("key3"), Value: []byte("value3"), Active: true, Timestamp: 123457}
	record4 := &model.Record{Key: []byte("key4"), Value: []byte("value4"), Active: true, Timestamp: 123457}
	record5 := &model.Record{Key: []byte("key5"), Value: []byte("value5"), Active: true, Timestamp: 123457}

	grandgrandchildNode := &Node{
		records: []*model.Record{record5},
	}

	grandchildNode := &Node{
		children: []*Node{grandgrandchildNode},
		records:  []*model.Record{record3},
	}

	childNode1 := &Node{
		children: []*Node{grandchildNode},
		records:  []*model.Record{record2},
	}

	childNode2 := &Node{
		records: []*model.Record{record4},
	}

	rootNode := &Node{
		records:  []*model.Record{record1},
		children: []*Node{childNode1, childNode2},
	}

	btree := &BTree{root: rootNode}

	tests := []struct {
		name     string
		key      []byte
		expected *Node
	}{
		{"KeyExistsInRoot", []byte("key1"), rootNode},
		{"KeyExistsInChild1", []byte("key2"), childNode1},
		{"KeyExistsInChild2", []byte("key4"), childNode2},
		{"KeyExistsInGrandchild", []byte("key3"), grandchildNode},
		{"KeyExistsInGrandgrandchild", []byte("key5"), grandgrandchildNode},
		{"KeyNotFound", []byte("key69"), nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := btree.Get(tc.key)
			if node != tc.expected {
				t.Errorf("Get(%q) = %v; want %v", tc.key, node, tc.expected)
			}
		})
	}
}
