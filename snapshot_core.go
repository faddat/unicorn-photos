package main

import (
	"fmt"
)

// takeSnapshotCore is the core implementation of taking a snapshot
func takeSnapshotCore(height int64) error {
	snapshotDir, err := ensureSnapshotDir(height)
	if err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	state, err := loadState(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to load state: %v", err)
	}

	// Fetch accounts and balances
	accounts, err := fetchAccountsParallel()
	if err != nil {
		return fmt.Errorf("failed to fetch accounts: %v", err)
	}

	balances, err := updateBalances(accounts, height, snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to update balances: %v", err)
	}

	// Save final state
	state.BlockHeight = height
	state.AccountsComplete = true
	state.BalancesComplete = true
	if err := saveState(state, true, snapshotDir); err != nil {
		return fmt.Errorf("failed to save final state: %v", err)
	}

	// Update README
	if err := updateReadme(height, snapshotDir, balances); err != nil {
		logger.Printf("Warning: failed to update README: %v", err)
	}

	return nil
}
