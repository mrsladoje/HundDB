package seeded_hash

import (
	"crypto/md5"
	"encoding/binary"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint64) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint64(time.Now().Unix())
	for i := uint64(0); i < k; i++ {
		seed := make([]byte, 8)
		binary.BigEndian.PutUint64(seed, ts+i)
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

// Serialize a single HashWithSeed
func (h HashWithSeed) Serialize() []byte {
	return h.Seed
}

// Deserialize a single HashWithSeed
func Deserialize(data []byte) HashWithSeed {
	return HashWithSeed{Seed: data}
}
