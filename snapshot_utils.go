package main

import (
	"fmt"
	"os"
	"path/filepath"
	// "time"
	// "encoding/json"
)

// SnapshotState (Legacy or for partial snapshots, not currently used by full genesis generation)
// type SnapshotState struct {
// 	BlockHeight      int64     `json:"block_height"`
// 	Timestamp        time.Time `json:"timestamp"`
// 	AccountsComplete bool      `json:"accounts_complete"`
// 	BalancesComplete bool      `json:"balances_complete"`
// }

// ensureSnapshotDir creates the snapshot directory if it doesn't exist.
// The structure will be: <baseSnapshotDir>/<chainID>/height_<HEIGHT>/
func ensureSnapshotDir(height int64, chainSpecificBaseDir string) (string, error) {
	// chainSpecificBaseDir is like "snapshots/cosmoshub-4"
	// This base directory for the chain should already be created by the caller or checked.
	if err := os.MkdirAll(chainSpecificBaseDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create chain base snapshot directory %s: %w", chainSpecificBaseDir, err)
	}

	// Create height-specific directory using the format "height_{HEIGHT}"
	snapshotPath := filepath.Join(chainSpecificBaseDir, fmt.Sprintf("height_%d", height))

	if err := os.MkdirAll(snapshotPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create height-specific snapshot directory %s: %w", snapshotPath, err)
	}

	return snapshotPath, nil
}

// loadState, saveState, fetchAccountsParallel, updateBalances, updateReadme
// were part of a previous, more granular snapshot mechanism.
// With the full `genesis.json` generation approach in `chain_fetcher.go` and `snapshot_core.go`,
// these are largely superseded or handled differently.
// - `state.json` is not used per snapshot dir currently.
// - Account and balance fetching is part of `generateCosmosGenesisDoc`.
// - README per snapshot dir is created directly in `takeSnapshot`.
