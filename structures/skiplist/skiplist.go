package main

import (
	"math"
	"math/rand"
)

type Node struct {
	key       int
	nextNodes []*Node // i-ti Node je na i-toj visini
}

func NewNode(key int, height int) *Node {
	return &Node{
		key:       key,
		nextNodes: make([]*Node, height),
	}
}

type SkipList struct {
	maxHeight     int
	currentHeight int
	head          *Node
}

func NewSkipList(maxHeight int) *SkipList {
	head := NewNode(math.MinInt64, maxHeight) // head.key je najmanji moguci broj (zamena za -inf)
	return &SkipList{
		maxHeight:     maxHeight,
		currentHeight: 1,
		head:          head,
	}
}

func (s *SkipList) Check(key int) bool {
	currentNode := s.head

	// iterira kroz svaku visinu sve dok trazeni key nije veci od narednog
	for i := s.currentHeight - 1; i >= 0; i-- {
		for currentNode.nextNodes[i] != nil && key >= currentNode.nextNodes[i].key {
			currentNode = currentNode.nextNodes[i]
		}

		if currentNode.key == key {
			return true
		}
	}

	return false
}

// random ograniceno izracunavanje visine za novi Node
func (s *SkipList) roll() int {
	height := 1
	for rand.Int31n(2) == 1 && height < s.maxHeight {
		height++
	}
	return height
}

func (s *SkipList) Add(key int) {
	if s.Check(key) {
		println("Element vec postoji.")
		return
	}

	nodesToUpdate := make([]*Node, s.maxHeight)
	currentNode := s.head

	// iterira kroz svaku visinu i zapisuje u nodesToUpdate poslednji Node procitan na visini
	for i := s.currentHeight - 1; i >= 0; i-- {
		for currentNode.nextNodes[i] != nil && currentNode.nextNodes[i].key < key {
			currentNode = currentNode.nextNodes[i]
		}
		nodesToUpdate[i] = currentNode
	}

	height := s.roll() // random visina za novi Node

	// ako je nova visina veca od trenutne povezuje head sa novim Node i novi Node postaje head
	if height > s.currentHeight {
		for i := s.currentHeight; i < height; i++ {
			nodesToUpdate[i] = s.head
		}
		s.currentHeight = height
	}

	newNode := NewNode(key, height)

	// linkuje novi Node sa starim po svim visinama
	for i := 0; i < height; i++ {
		newNode.nextNodes[i] = nodesToUpdate[i].nextNodes[i]
		nodesToUpdate[i].nextNodes[i] = newNode
	}
}

func (s *SkipList) Delete(key int) {
	if !s.Check(key) {
		print("Element ne postoji.")
		return
	}

	nodesToUpdate := make([]*Node, s.maxHeight)
	currentNode := s.head
	var nodeToDelete *Node

	for i := s.currentHeight - 1; i >= 0; i-- {
		// iterira kroz skip listu
		for currentNode.nextNodes[i] != nil && key > currentNode.nextNodes[i].key {
			currentNode = currentNode.nextNodes[i]
		}

		// zapisuje u nodesToUpdate svaki Node koji pokazuje na nodeToDelete
		if currentNode.nextNodes[i] != nil && currentNode.nextNodes[i].key == key {
			nodesToUpdate[i] = currentNode
			nodeToDelete = currentNode.nextNodes[i]
		}
	}

	// logicko brisanje nodeToDelete
	for i := 0; i < s.currentHeight; i++ {
		if nodesToUpdate[i] != nil && nodesToUpdate[i].nextNodes[i] != nil {
			nodesToUpdate[i].nextNodes[i] = nodeToDelete.nextNodes[i]
		}
	}

	// brise visinu na kojoj nema elemenata (ako je potrebno)
	for s.currentHeight > 1 && s.head.nextNodes[s.currentHeight-1] == nil {
		s.currentHeight--
	}
}
