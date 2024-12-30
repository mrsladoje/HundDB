package hll

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

type HLL struct {
	m   uint32
	p   uint8
	reg []uint8
}

func NewHLL(precision uint8) *HLL {
	if precision < HLL_MIN_PRECISION || precision > HLL_MAX_PRECISION {
		panic("precision must be between 4 and 16")
	}
	m := uint32(1 << precision)
	return &HLL{
		m:   m,
		p:   precision,
		reg: make([]uint8, m),
	}
}

func (hll *HLL) Add(item string) {
	rawHash := sha256.Sum256([]byte(item))
	hash := binary.BigEndian.Uint64(rawHash[:8])
	regBucket := firstKbits(hash, hll.p)
	zeroCount := trailingZeroBits(hash) + 1
	if zeroCount > hll.reg[regBucket] {
		hll.reg[regBucket] = zeroCount
	}
}

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

func (hll *HLL) emptyCount() int {
	count := 0
	for _, val := range hll.reg {
		if val == 0 {
			count++
		}
	}
	return count
}

func firstKbits(value uint64, k uint8) uint64 {
	return value >> (64 - k)
}

func trailingZeroBits(value uint64) uint8 {
	return uint8(bits.TrailingZeros64(value))
}
