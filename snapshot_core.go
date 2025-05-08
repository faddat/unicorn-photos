package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// takeSnapshotRuntime creates a state dump snapshot at the specified height.
// It uses the chain_fetcher.go logic to get module states and saves them individually.
func takeSnapshotRuntime(height int64, chainConfig ChainRuntimeConfig, restURL string, snapshotDir string) error {
	logger.Printf("[%s / %s] Taking state dump snapshot at height %d using REST endpoint %s into %s",
		chainConfig.Name, chainConfig.ChainID, height, restURL, snapshotDir)

	// Fetch all module states using chain_fetcher
	fetchedState, err := fetchAllModuleStates(restURL, chainConfig.ChainID, height)
	if err != nil {
		// Even if fetching failed partially, try to save what was obtained
		// Log the error but continue to save partial data if available
		logger.Printf("[%s] Warning: fetchAllModuleStates returned error: %v. Attempting to save partial state.", chainConfig.ChainID, err)
		// return fmt.Errorf("failed to fetch module states for chain %s: %w", chainConfig.ChainID, err)
		if fetchedState == nil {
			return fmt.Errorf("failed to fetch any module states and fetcher returned nil state for chain %s: %w", chainConfig.ChainID, err)
		}
	}

	// --- Save individual module states to JSON files ---
	saveModuleState := func(moduleName string, data interface{}) {
		if data == nil {
			logger.Printf("[%s] No data fetched for module '%s', skipping save.", chainConfig.ChainID, moduleName)
			return
		}
		fileName := fmt.Sprintf("%s_state.json", moduleName)
		filePath := filepath.Join(snapshotDir, fileName)
		jsonData, marshalErr := json.MarshalIndent(data, "", "  ")
		if marshalErr != nil {
			logger.Printf("[%s] Error marshalling state for module %s: %v", chainConfig.ChainID, moduleName, marshalErr)
			// Optionally record this error in the main README or a status file
			return
		}
		if writeErr := os.WriteFile(filePath, jsonData, 0644); writeErr != nil {
			logger.Printf("[%s] Error writing state file %s: %v", chainConfig.ChainID, filePath, writeErr)
		} else {
			logger.Printf("[%s] Successfully wrote %s", chainConfig.ChainID, filePath)
		}
	}

	// Save state for each fetched module
	saveModuleState("auth", fetchedState.Auth)
	saveModuleState("bank", fetchedState.Bank)
	saveModuleState("staking", fetchedState.Staking)
	saveModuleState("gov", fetchedState.Gov)
	// saveModuleState("distribution", fetchedState.Distribution) // etc. for other modules

	// Optionally save the fetch errors as well
	if len(fetchedState.Errors) > 0 {
		saveModuleState("fetch_errors", fetchedState.Errors)
	}

	// --- Create Snapshot README ---
	snapshotReadmeContent := fmt.Sprintf(
		"# State Dump Snapshot for %s\n\n"+
			"Chain ID: %s\n"+
			"Target Block Height: %d\n"+
			"Snapshot Time (UTC): %s\n"+
			"REST Endpoint Used: %s\n\n"+
			"This directory contains a state dump snapshot captured from the chain's public API.\n"+
			"It includes JSON files representing the state of various modules at the target height.\n\n"+
			"**Included Files:**\n\n"+
			"- `auth_state.json`: Authentication parameters and accounts.\n"+
			"- `bank_state.json`: Bank parameters, supply, denom metadata, and balances.\n"+
			"- `staking_state.json`: Staking parameters and validators (potentially more).\n"+
			"- `gov_state.json`: Governance parameters and proposals (potentially more).\n"+
			// Add lines for other saved modules
			"%s"+ // Placeholder for fetch_errors file if present
			"\n**Note:** This is not a node data directory snapshot suitable for state sync. It's a structured dump of queryable state.\n",
		chainConfig.Name,
		chainConfig.ChainID,
		height,
		fetchedState.Timestamp.Format(time.RFC1123),
		restURL,
		func() string { // Add note about errors file if it exists
			if _, err := os.Stat(filepath.Join(snapshotDir, "fetch_errors_state.json")); err == nil {
				return "- `fetch_errors_state.json`: Lists errors encountered during state fetching.\n"
			}
			return ""
		}(),
	)
	readmePath := filepath.Join(snapshotDir, "README.md")
	if err := os.WriteFile(readmePath, []byte(snapshotReadmeContent), 0644); err != nil {
		logger.Printf("[%s] Warning: failed to write snapshot-specific README.md to %s: %v",
			chainConfig.ChainID, readmePath, err)
	}

	logger.Printf("[%s] Completed state dump snapshot processing for height %d.", chainConfig.ChainID, height)
	return nil // Return nil even if some module fetching failed, as partial data was saved
}
