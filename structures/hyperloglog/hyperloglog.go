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
	m   uint64
	p   uint8
	reg []uint8
}

func NewHLL(precision uint8) *HLL {
	if precision < HLL_MIN_PRECISION || precision > HLL_MAX_PRECISION {
		panic("precision must be between 4 and 16")
	}
	m := uint64(1 << precision) 
	return &HLL{
		m:   m,
		p:   precision,
		reg: make([]uint8, m),
	}
}

func (hll *HLL) Add(value []byte) {
	hash := sha256.Sum256(value)
	x := binary.BigEndian.Uint64(hash[:8])
	j := firstKbits(x, hll.p)
	w := x << hll.p
	zeroCount := trailingZeroBits(w) + 1
	if zeroCount > hll.reg[j] {
		hll.reg[j] = zeroCount
	}
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += 1.0 / math.Pow(2.0, float64(val))
	}
	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	rawEstimate := alpha * float64(hll.m*hll.m) / sum

	emptyRegs := hll.emptyCount()
	if rawEstimate <= 2.5*float64(hll.m) && emptyRegs > 0 {
		return float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
	}

	if rawEstimate > 1.0/30.0*math.Pow(2.0, 32.0) {
		return -math.Pow(2.0, 32.0) * math.Log(1.0-rawEstimate/math.Pow(2.0, 32.0))
	}

	return rawEstimate
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
