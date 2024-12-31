package hyperloglog

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

// HLL is a probabilistic data structure used to estimate the cardinality of a set.
// It works with uint32 for efficiency given the data size in our project.
type HLL struct {
	m   uint32  // Size of the register array, 2^p
	p   uint8   // Precision, determines the number of bits used for the bucket index
	reg []uint8 // Array of registers, each storing the maximum number of trailing zero bits + 1
}

// NewHLL creates a new instance of a HyperLogLog.
// precision: the precision parameter, must be between 4 and 16.
func NewHLL(precision uint8) *HLL {
	if precision < HLL_MIN_PRECISION || precision > HLL_MAX_PRECISION {
		panic("precision must be between 4 and 16")
	}
	m := uint32(1 << precision) // Equivalent to 2^p
	return &HLL{
		m:   m,
		p:   precision,
		reg: make([]uint8, m),
	}
}

// Add inserts an element into the HyperLogLog by updating the corresponding register.
// item: the element to be added to the HyperLogLog.
func (hll *HLL) Add(item string) {
	rawHash := sha256.Sum256([]byte(item))
	hash := binary.BigEndian.Uint64(rawHash[:8])
	regBucket := firstKbits(hash, hll.p)
	zeroCount := trailingZeroBits(hash) + 1
	if zeroCount > hll.reg[regBucket] {
		hll.reg[regBucket] = zeroCount
	}
}

// Estimate estimates the cardinality of the set represented by the HyperLogLog.
func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += 1.0 / math.Pow(2.0, float64(val))
	}
	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimate := alpha * float64(hll.m*hll.m) / sum

	emptyRegs := hll.emptyCount()
	if estimate <= 2.5*float64(hll.m) && emptyRegs > 0 {
		return float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
	} else if estimate > 1.0/30.0*math.Pow(2.0, 32.0) {
		return -math.Pow(2.0, 32.0) * math.Log(1.0-estimate/math.Pow(2.0, 32.0))
	}
	return estimate
}

// emptyCount returns the number of registers that are zero.
func (hll *HLL) emptyCount() int {
	count := 0
	for _, val := range hll.reg {
		if val == 0 {
			count++
		}
	}
	return count
}

// firstKbits returns the first k bits of the value.
func firstKbits(value uint64, k uint8) uint64 {
	return value >> (64 - k)
}

// trailingZeroBits returns the number of trailing zero bits in the value.
func trailingZeroBits(value uint64) uint8 {
	return uint8(bits.TrailingZeros64(value))
}

// TODO: Serijalizacija
// TODO: Deserijalizacija
// (pogledati primer 5 sa trecih vezbi, i nalik toga napisati funkcije)
