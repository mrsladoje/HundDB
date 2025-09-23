package main

import (
	"context"
	"errors"
	"fmt"
	"hunddb/lsm"
	model "hunddb/model/record"
	"strings"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

var (
	ErrKeyNotFound = errors.New("key not found")
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
	record, errorEncounteredInCheck, isErrorEncountered := a.lsm.Get(key)
	var err error
	err = nil
	if isErrorEncountered {
		if isKeyNotFoundError(errorEncounteredInCheck) {
			// Key not found is not considered an error in this context
			return nil, nil
		}

		err = errorEncounteredInCheck
	}
	if record == nil {
		return nil, err
	}

	return a.recordToMap(record), nil
}

// Put stores a key-value pair in the LSM
func (a *App) Put(key string, value string) error {
	valueBytes := []byte(value)

	err := a.lsm.Put(key, valueBytes)
	if err != nil {
		return fmt.Errorf("error storing record: %v", err)
	}

	return nil
}

// Delete deletes a key in the LSM
func (a *App) Delete(key string) (bool, error) {
	record, err := a.Get(key)
	if (err == nil && record == nil) || record["deleted"] == true {
		return false, nil
	}
	keyExists, err := a.lsm.Delete(key)
	if err != nil {
		return false, fmt.Errorf("error deleting record: %v", err)
	}
	return keyExists, nil
}

// IsDataLost checks if data was lost during LSM loading
func (a *App) IsDataLost() bool {
	return a.lsm.IsDataLost()
}

// PersistLSM manually triggers LSM persistence
func (a *App) PersistLSM() error {
	return a.lsm.PersistLSM()
}

func (a *App) PrefixScan() string {
	return "Not implemented yet"
}

func (a *App) RangeScan() string {
	return "Not implemented yet"
}

func (a *App) PrefixIterate(prefix string, key string) (map[string]interface{}, error) {
	record, err := a.lsm.GetNextForPrefix(prefix, key)
	if err != nil {
		return nil, err
	}
	return a.recordToMap(record), nil
}

func (a *App) RangeIterate() string {
	return "Not implemented yet"
}

// Helper function to check if an error is or contains ErrKeyNotFound
func isKeyNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Direct comparison
	if errors.Is(err, ErrKeyNotFound) {
		return true
	}

	// Check if the error message contains the ErrKeyNotFound message
	// This handles cases like "some context: key not found"
	return strings.Contains(err.Error(), ErrKeyNotFound.Error())
}
