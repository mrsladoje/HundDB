package hyperloglog

// cSpell:ignore hyperloglog, Kbits

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
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
func NewHLL(precision uint8) (*HLL, error) {
	if precision < HLL_MIN_PRECISION || precision > HLL_MAX_PRECISION {
		return nil, errors.New("precision out of bounds")
	}
	m := uint32(1 << precision) // Equivalent to 2^p
	return &HLL{
		m:   m,
		p:   precision,
		reg: make([]uint8, m),
	}, nil
}

// Add inserts an element into the HyperLogLog by updating the corresponding register.
// item: the element to be added to the HyperLogLog.
func (hll *HLL) Add(item []byte) {
	rawHash := sha256.Sum256(item)
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

// Serialize serializes the HyperLogLog into a byte slice.
// The byte slice returned contains the size of the uint8 slice, precision as the uint8, the uint8 slice.
// The format is as follows:
// - 4 bytes for the size of the register array (m)
// - 1 byte for the number of of bits used for the bucket index (p)
// - m bytes, representing the slice of registers
func (hll *HLL) Serialize() []byte {
	// totalSize is the size of the whole bloom filter structure
	totalSize := 5 + hll.m
	data := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(data[0:4], hll.m)
	data[4] = hll.p
	copy(data[5:], hll.reg)

	return data
}

// // The first byte stores the precision (p)
// data := []byte{hll.p}

// // The next 4 bytes store the size (m)
// mBytes := make([]byte, 4)
// binary.LittleEndian.PutUint32(mBytes, hll.m)
// data = append(data, mBytes...)

// // Append the register values (reg)
// data = append(data, hll.reg...)
// return data

// Deserialize initializes an HLL structure from a byte slice.
// The byte slice returned contains the size of the uint8 slice, precision as the uint8, the uint8 slice.
// The format is as follows:
// - 4 bytes for the size of the register array (m)
// - 1 byte for the number of of bits used for the bucket index (p)
// - m bytes, representing the slice of registers
func Deserialize(data []byte) *HLL {
	m := binary.LittleEndian.Uint32(data[0:4])
	p := data[4]
	reg := make([]uint8, m)
	copy(reg, data[5:])
	return &HLL{
		m:   m,
		p:   p,
		reg: reg,
	}
}
