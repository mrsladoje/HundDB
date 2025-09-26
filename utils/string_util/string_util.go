package string_util

/*
FindLexicographicallySmaller finds the largest lexicographically smaller string than the input.
*/
func FindLexicographicallySmaller(str string) string {
	// If empty string, return empty string (can't get smaller)
	if len(str) == 0 {
		return ""
	}

	// Convert string to runes to handle UTF-8 properly
	runes := []rune(str)
	n := len(runes)

	// Try to find a position where we can decrement a character
	for i := n - 1; i >= 0; i-- {
		currentRune := runes[i]

		// If we can decrement this character (not at minimum value)
		if currentRune > 0 {
			// Decrement the character
			runes[i] = currentRune - 1

			// Set all characters after this position to the maximum UTF-8 character
			// to get the lexicographically largest suffix
			for j := i + 1; j < n; j++ {
				runes[j] = 0x10FFFF // Maximum Unicode code point
			}

			return string(runes)
		}
		// If current character is at minimum (0), continue to the next position
	}

	// If we get here, all characters were at minimum value
	// The only string smaller would be a shorter string
	if n == 1 {
		return "" // Single character at minimum becomes empty string
	}

	result := make([]rune, n-1)
	for i := 0; i < n-1; i++ {
		result[i] = 0x10FFFF
	}

	return string(result)
}
