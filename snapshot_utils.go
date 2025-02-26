package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SnapshotState tracks the progress of a snapshot
type SnapshotState struct {
	BlockHeight      int64     `json:"block_height"`
	Timestamp        time.Time `json:"timestamp"`
	AccountsComplete bool      `json:"accounts_complete"`
	BalancesComplete bool      `json:"balances_complete"`
}

// Account represents a blockchain account
type Account struct {
	Address string `json:"address"`
	Balance int64  `json:"balance"`
}

// ensureSnapshotDir creates the snapshot directory if it doesn't exist
func ensureSnapshotDir(height int64) (string, error) {
	config := LoadConfig()
	// Create base snapshots directory
	if err := os.MkdirAll(config.SnapshotDir, 0755); err != nil {
		return "", err
	}

	// Create height-specific directory using the format "height_{HEIGHT}"
	snapshotPath := filepath.Join(config.SnapshotDir, fmt.Sprintf("height_%d", height))

	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return "", err
	}

	return snapshotPath, nil
}

// loadState loads the snapshot state or creates a new one
func loadState(snapshotDir string) (*SnapshotState, error) {
	statePath := filepath.Join(snapshotDir, "state.json")

	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		// Create new state
		state := &SnapshotState{
			Timestamp: time.Now(),
		}
		return state, nil
	}

	data, err := os.ReadFile(statePath)
	if err != nil {
		return nil, err
	}

	var state SnapshotState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

// saveState saves the current snapshot state
func saveState(state *SnapshotState, final bool, snapshotDir string) error {
	statePath := filepath.Join(snapshotDir, "state.json")

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(statePath, data, 0644)
}

// fetchAccountsParallel fetches all accounts in parallel
// This is a placeholder implementation
func fetchAccountsParallel() ([]Account, error) {
	// Simulate fetching accounts from a blockchain
	accounts := []Account{
		{Address: "cosmos1abc...", Balance: 1000000},
		{Address: "cosmos2def...", Balance: 2000000},
		// Add more sample accounts for testing
	}

	return accounts, nil
}

// updateBalances writes account balances to disk
// Returns total supply for the readme
func updateBalances(accounts []Account, height int64, snapshotDir string) (int64, error) {
	balancesPath := filepath.Join(snapshotDir, "balances.json")

	data, err := json.MarshalIndent(accounts, "", "  ")
	if err != nil {
		return 0, err
	}

	if err := os.WriteFile(balancesPath, data, 0644); err != nil {
		return 0, err
	}

	// Calculate total supply
	var totalSupply int64
	for _, acc := range accounts {
		totalSupply += acc.Balance
	}

	return totalSupply, nil
}

// updateReadme creates or updates the README.md file for the snapshot
func updateReadme(height int64, snapshotDir string, totalSupply int64) error {
	readmePath := filepath.Join(snapshotDir, "README.md")

	content := fmt.Sprintf(`# Unicorn Photos Snapshot

## Block Height: %d
## Timestamp: %s
## Total Supply: %d

This snapshot was created automatically by the Unicorn Photos snapshot tool.
`, height, time.Now().Format(time.RFC3339), totalSupply)

	return os.WriteFile(readmePath, []byte(content), 0644)
}
