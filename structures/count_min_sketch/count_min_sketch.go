package main

type CMS struct {
	// Radi sa uint32 kako je on efektivnije resenje za kolicinu podataka naseg projekta
	m     uint32         // m - duzina setova (u ovom slucaju kolona)
	k     uint32         // k - broj hash funkcija (u ovom slucaju redova)
	h     []HashWithSeed // h - niz hes funkcija
	table [][]uint32     // table - matrica(tabela) uint32 vrednosti
}

// Kreira novu instancu Count-min sketcha
func NewCMS(epsilon float64, delta float64) *CMS {
	m := CalculateM(epsilon)
	k := CalculateK(delta)
	matrix := make([][]uint32, k)
	for i := range matrix {
		matrix[i] = make([]uint32, m) // Nephodna kroz petlju zbog alokacije memorije svakom redu
	}
	return &CMS{
		m:     uint32(m),
		k:     uint32(k),
		h:     CreateHashFunctions(k),
		table: matrix,
	}
}

// Dodaje element tako sto inkrementira zeljena polja tabele
func (cms *CMS) Add(item string) {
	data := []byte(item)
	for i := uint32(0); i < cms.k; i++ {
		j := cms.h[i].Hash(data) % uint64(cms.m)
		cms.table[i][j]++
	}
}

// Proverava (sa greskom) koliko je puta sadrzan item u Count-min sketcu
func (cms *CMS) Count(item string) uint32 {
	data := []byte(item)
	min := uint32(4294967295) // max uint32
	for i := uint32(0); i < cms.k; i++ {
		j := cms.h[i].Hash(data) % uint64(cms.m)
		if cms.table[i][j] < min {
			min = cms.table[i][j]
		}
	}
	return min
}

// TODO: Serijalizacija
// TODO: Deserijalizacija
// TODO: napisati testove
// (pogledati primer 5 sa trecih vezbi, i nalik toga napisati funkcije)
// TODO: TEK Kada odradimo sve strukture ujediniti hash u jedan hash fajl, da nema nepotrebih ponavljanja
