package wal

import "encoding/binary"

const (
	// Header field sizes
	HEADER_CRC_SIZE        = 4
	HEADER_SIZE_SIZE       = 2
	HEADER_TYPE_SIZE       = 1
	HEADER_LOG_NUMBER_SIZE = 4

	// Header field positions
	HEADER_CRC_START        = 0
	HEADER_SIZE_START       = HEADER_CRC_START + HEADER_CRC_SIZE
	HEADER_TYPE_START       = HEADER_SIZE_START + HEADER_SIZE_SIZE
	HEADER_LOG_NUMBER_START = HEADER_TYPE_START + HEADER_TYPE_SIZE

	// Total header size
	HEADER_TOTAL_SIZE = HEADER_CRC_SIZE + HEADER_SIZE_SIZE + HEADER_TYPE_SIZE + HEADER_LOG_NUMBER_SIZE // 11 bytes

	// Fragment types
	FRAGMENT_FIRST  = 1 // First fragment of a multi-block user record
	FRAGMENT_MIDDLE = 2 // Middle fragment of a multi-block user record
	FRAGMENT_LAST   = 3 // Last fragment of a multi-block user record
	FRAGMENT_FULL   = 4 // Whole user record fits in one WAL record
)

/*
   WAL Header Format:
   +---------------+---------------+---------------+---------------+
   |    CRC (4B)   |   Size (2B)   |   Type (1B)   | LogNumber(4B) |
   +---------------+---------------+---------------+---------------+
   CRC = 32bit hash computed over the fragment payload using CRC32
   Size = Length of the fragment payload in bytes
   Type = Fragment type: 1=FIRST, 2=MIDDLE, 3=LAST, 4=FULL
   LogNumber = Identifies which WAL log this fragment belongs to
*/

// WALHeader represents the header for WAL record fragments.
// It contains metadata about the fragment including CRC, size, type, and log number.
type WALHeader struct {
	CRC       uint32 // 4 bytes (computed over the payload)
	Size      uint16 // 2 byte (size of payload)
	Type      byte   // indicates which part of fragment
	LogNumber uint32 // indicates which log
}

// NewWALHeader creates a new WALHeader with the provided values.
func NewWALHeader(crc uint32, size uint16, typ byte, logNumber uint32) *WALHeader {
	return &WALHeader{
		CRC:       crc,
		Size:      size,
		Type:      typ,
		LogNumber: logNumber,
	}
}

// Serialize serializes a WALHeader into a byte array. The byte array contains the following fields:
// - CRC: 4 bytes for the CRC32 checksum of the fragment payload
// - Size: 2 bytes for the size of the fragment payload
// - Type: 1 byte for the fragment type (FIRST/MIDDLE/LAST/FULL)
// - LogNumber: 4 bytes for the log number identifier
func (h *WALHeader) Serialize() []byte {
	data := make([]byte, HEADER_TOTAL_SIZE)
	binary.LittleEndian.PutUint32(data[HEADER_CRC_START:HEADER_CRC_START+HEADER_CRC_SIZE], h.CRC)
	binary.LittleEndian.PutUint16(data[HEADER_SIZE_START:HEADER_SIZE_START+HEADER_SIZE_SIZE], h.Size)
	data[HEADER_TYPE_START] = h.Type
	binary.LittleEndian.PutUint32(data[HEADER_LOG_NUMBER_START:HEADER_LOG_NUMBER_START+HEADER_LOG_NUMBER_SIZE], h.LogNumber)
	return data
}

// DeserializeWALHeader takes a byte array and reconstructs a WALRecordHeader from it.
// It reads the data in the format defined by the Serialize function.
// Returns nil if the input data is too short (less than 11 bytes).
func DeserializeWALHeader(data []byte) *WALHeader {
	if len(data) < 11 {
		return nil
	}
	return &WALHeader{
		CRC:       binary.LittleEndian.Uint32(data[0:4]),
		Size:      binary.LittleEndian.Uint16(data[4:6]),
		Type:      data[6],
		LogNumber: binary.LittleEndian.Uint32(data[7:11]),
	}
}
