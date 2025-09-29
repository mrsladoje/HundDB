package wal

import "encoding/binary"

const (
	// Header field sizes
	PAYLOAD_SIZE_SIZE      = 8
	HEADER_TYPE_SIZE       = 1
	HEADER_LOG_NUMBER_SIZE = 8

	// Header field positions
	PAYLOAD_SIZE_START      = 0
	HEADER_TYPE_START       = PAYLOAD_SIZE_START + PAYLOAD_SIZE_SIZE
	HEADER_LOG_NUMBER_START = HEADER_TYPE_START + HEADER_TYPE_SIZE

	// Total header size
	HEADER_TOTAL_SIZE = PAYLOAD_SIZE_SIZE + HEADER_TYPE_SIZE + HEADER_LOG_NUMBER_SIZE

	// Fragment types
	FRAGMENT_FIRST  = 1 // First fragment of a multi-block user record
	FRAGMENT_MIDDLE = 2 // Middle fragment of a multi-block user record
	FRAGMENT_LAST   = 3 // Last fragment of a multi-block user record
	FRAGMENT_FULL   = 4 // Whole user record fits in one WAL record
)

/*
   WAL Header Format:
   +---------------+---------------+---------------+
   |    Size (8B)  |   Type (1B)   | LogNumber(8B) |
   +---------------+---------------+---------------+
   Size = Length of the fragment payload in bytes
   Type = Fragment type: 1=FIRST, 2=MIDDLE, 3=LAST, 4=FULL
   LogNumber = Identifies which WAL log this fragment belongs to
*/

// WALHeader represents the header for WAL record fragments.
// Contains metadata including size, fragment type, and log number.
type WALHeader struct {
	PayloadSize uint64 // 8 bytes (size of payload)
	Type        byte   // indicates which part of fragment
	LogNumber   uint64 // indicates which log
}

// NewWALHeader creates a new WALHeader with the provided values.
func NewWALHeader(size uint64, typ byte, logNumber uint64) *WALHeader {
	return &WALHeader{
		PayloadSize: size,
		Type:        typ,
		LogNumber:   logNumber,
	}
}

// Serialize serializes a WALHeader into a byte array. The byte array contains the following fields:
// - Size: 2 bytes for the size of the fragment payload
// - Type: 1 byte for the fragment type (FIRST/MIDDLE/LAST/FULL)
// - LogNumber: 4 bytes for the log number identifier
func (h *WALHeader) Serialize() []byte {
	data := make([]byte, HEADER_TOTAL_SIZE)
	binary.LittleEndian.PutUint64(data[PAYLOAD_SIZE_START:PAYLOAD_SIZE_START+PAYLOAD_SIZE_SIZE], h.PayloadSize)
	data[HEADER_TYPE_START] = h.Type
	binary.LittleEndian.PutUint64(data[HEADER_LOG_NUMBER_START:HEADER_LOG_NUMBER_START+HEADER_LOG_NUMBER_SIZE], h.LogNumber)
	return data
}

// DeserializeWALHeader takes a byte array and reconstructs a WALRecordHeader from it.
// It reads the data in the format defined by the Serialize function.
// Returns nil if the input data is too short (less than 11 bytes).
func DeserializeWALHeader(data []byte) *WALHeader {
	if len(data) < HEADER_TOTAL_SIZE {
		return nil
	}
	return &WALHeader{
		PayloadSize: binary.LittleEndian.Uint64(data[PAYLOAD_SIZE_START : PAYLOAD_SIZE_START+PAYLOAD_SIZE_SIZE]),
		Type:        data[HEADER_TYPE_START],
		LogNumber:   binary.LittleEndian.Uint64(data[HEADER_LOG_NUMBER_START : HEADER_LOG_NUMBER_START+HEADER_LOG_NUMBER_SIZE]),
	}
}
