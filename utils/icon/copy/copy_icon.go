package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

func main() {
	rootDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get working directory: %s", err)
	}

	sourcePath := filepath.Join(rootDir, "frontend", "public", "pics", "rokica_icon.png")
	buildDir := filepath.Join(rootDir, "build")
	destPath := filepath.Join(buildDir, "appicon.png")

	// --- IMPORTANT ADDITION ---
	// Ensure the build directory exists before trying to create a file in it.
	if _, err := os.Stat(buildDir); os.IsNotExist(err) {
		// 0755 is standard permissions for a new directory
		if err := os.MkdirAll(buildDir, 0755); err != nil {
			log.Fatalf("failed to create build directory '%s': %s", buildDir, err)
		}
	}
	// --- END ADDITION ---

	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		log.Fatalf("failed to open source file '%s': %s", sourcePath, err)
	}
	defer sourceFile.Close()

	destFile, err := os.Create(destPath)
	if err != nil {
		log.Fatalf("failed to create destination file '%s': %s", destPath, err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		log.Fatalf("failed to copy file: %s", err)
	}
}
