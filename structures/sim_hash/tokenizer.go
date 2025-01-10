package sim_hash

import (
	"regexp"
	"strings"
)

func processText(text string) []string {
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
