package main

import (
	"log"
	"os"
)

func main() {
	err := os.Remove("./build/appicon.png")
	// If the file doesn't exist, we don't consider it a fatal error for a cleanup script.
	// We only want to fail the build if there's an actual error like a permissions issue.
	if err != nil && !os.IsNotExist(err) {
		log.Fatalf("failed to remove appicon.png: %s", err)
	}
}
