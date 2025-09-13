package main

import (
	"context"
	"fmt"
	"hunddb/lsm"
	model "hunddb/model/record"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// App struct - application layer wrapper for LSM
type App struct {
	ctx context.Context
	lsm *lsm.LSM
}

// NewApp creates a new App application struct and loads the LSM instance
func NewApp() *App {
	lsmInstance := lsm.LoadLSM()

	return &App{
		lsm: lsmInstance,
	}
}

// startup is called when the app starts up
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	runtime.WindowMaximise(a.ctx)
}

/*
Helper method to convert Record to a serializable map.
We use this because wails has trouble serializing complex types directly.
*/
func (a *App) recordToMap(record *model.Record) map[string]interface{} {
	if record == nil {
		return nil
	}

	return map[string]interface{}{
		"key":       record.Key,
		"value":     string(record.Value),
		"timestamp": record.Timestamp,
		"deleted":   record.Tombstone,
	}
}

// LSM Wrapper Methods - Convert complex types to Wails-compatible types

// Get retrieves a record by key from the LSM
func (a *App) Get(key string) (map[string]interface{}, error) {
	record, errorEncountered := a.lsm.Get(key)
	var err error
	err = nil
	if errorEncountered {
		err = fmt.Errorf("error retrieving record")
	}
	if record == nil {
		return nil, err
	}

	return a.recordToMap(record), nil
}

// Put stores a key-value pair in the LSM
func (a *App) Put(key string, value string) error {
	valueBytes := []byte(value)
	return a.lsm.Put(key, valueBytes)
}

// IsDataLost checks if data was lost during LSM loading
func (a *App) IsDataLost() bool {
	return a.lsm.IsDataLost()
}

// PersistLSM manually triggers LSM persistence
func (a *App) PersistLSM() error {
	return a.lsm.PersistLSM()
}

func (a *App) Delete() string {
	return "Not implemented yet"
}
func (a *App) PrefixScan() string {
	return "Not implemented yet"
}
func (a *App) RangeScan() string {
	return "Not implemented yet"
}
func (a *App) PrefixIterate() string {
	return "Not implemented yet"
}
func (a *App) RangeIterate() string {
	return "Not implemented yet"
}
