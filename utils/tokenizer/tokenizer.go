package tokenizer

import (
	"regexp"
	"strings"
)

// TODO: This isn't good, needs to be improved and redone, not a priority as of now
func ProcessText(text string) []string {
	text = strings.ToLower(text)
	text = strings.ReplaceAll(text, "'", "")
	reg := regexp.MustCompile(`[^\w\s]`)
	text = reg.ReplaceAllString(text, "")
	words := strings.Fields(text)
	for i, word := range words {
		words[i] = strings.TrimSuffix(word, ".")
	}
	return words
}
