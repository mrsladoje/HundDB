package main

import (
	"embed"
	"log"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	// Create the application (this will handle LSM loading internally)
	app := NewApp()

	// Log data loss status
	if app.IsDataLost() {
		log.Println("Warning: Previous LSM data was lost or corrupted. Starting with fresh instance.")
	} else {
		log.Println("LSM loaded successfully.")
	}

	// Run the Wails applicationl
	err := wails.Run(&options.App{
		Title:     "HundDB",
		Width:     800,
		Height:    600,
		MinWidth:  400,
		MinHeight: 300,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 27, G: 38, B: 54, A: 1},
		OnStartup:        app.startup,
		Bind:             []interface{}{app},
	})

	if err != nil {
		log.Fatal("Failed to start application:", err)
	}
}
