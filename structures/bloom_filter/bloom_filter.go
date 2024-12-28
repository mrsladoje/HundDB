package main

import (
	"math"
)

type BloomFilter struct {
	m uint32         // m - velicina niza u bitovima
	k uint32         // k - broj hes funkcija
	h []HashWithSeed // h - niz hes funkcija
	b []byte         // b - niz bajtova
}

func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	return &BloomFilter{
		m: uint32(m),
		k: uint32(k),
		h: CreateHashFunctions(uint32(k)),
		b: make([]byte, uint32(math.Ceil(float64(m)/8))), // Izracunava duzinu niza bajtova
	}
}

func (bf *BloomFilter) Add(item string) {
	data := []byte(item)
	for i := uint32(0); i < bf.k; i++ {
		hash := bf.h[i].Hash(data) % uint64(bf.m) // Indeks bita u nizu bitova
		bit_mask := byte(1 << (hash % 8))         // Bajt koji na odredjenom bitu ima 1 a na ostalim 0
		bf.b[hash/8] |= bit_mask                  // Setuje tacno jedan bit na 1
	}
}

func (bf *BloomFilter) Contains(item string) bool {
	data := []byte(item)
	for i := uint32(0); i < bf.k; i++ {
		hash := bf.h[i].Hash(data) % uint64(bf.m) // Indeks bita u nizu bitova
		bit_mask := byte(1 << (hash % 8))         // Bajt koji na odredjenom bitu ima 1 a na ostalim 0
		if bf.b[hash/8]&bit_mask == 0 {           // Proverava da li je bit 0
			return false // Cim jeste znamo da podatak nije u BloomFileru
		}
	}
	return true // Ako su svi bitovi 1 VEROVARNO je sadrzan
}

// TODO: Serijalizacija
// TODO: Deserijalizacija
// TODO: napisati testove
// (pogledati primer 5 sa trecih vezbi, i nalik toga napisati funkcije)
// TODO: Kada odradimo sve strukture ujediniti hash u jedan hash fajl, da nema nepotrebih ponavljanja
