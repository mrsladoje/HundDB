package byte_util

func BoolToBytes(b bool) []byte {
	if b {
		return []byte{1} // Represents 'true'
	}
	return []byte{0} // Represents 'false'
}

func BoolToByte(b bool) byte {
	if b {
		return 1 // Represents 'true'
	}
	return 0 // Represents 'false'
}

/*
Adds padding - fills up byte array until it fills the last block.
Modifies the slice in-place by extending it.
*/
func AddPadding(data *[]byte, blockSize uint64) {
	paddingSize := len(*data) % int(blockSize)
	if paddingSize == 0 {
		return
	}
	padding := make([]byte, int(blockSize)-paddingSize)
	*data = append(*data, padding...)
}

// ByteToBool converts a byte to bool (non-zero = true, zero = false)
func ByteToBool(b byte) bool {
	return b != 0
}
