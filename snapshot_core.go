package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ConsensusInfo captures essential consensus-related information
type ConsensusInfo struct {
	Height             int64             `json:"height"`
	ValidatorSet       json.RawMessage   `json:"validator_set,omitempty"`
	ConsensusParams    json.RawMessage   `json:"consensus_params,omitempty"`
	LastBlockTimestamp string            `json:"last_block_timestamp,omitempty"`
	ChainID            string            `json:"chain_id"`
	Errors             map[string]string `json:"errors,omitempty"`
}

// ExtendedFetchedState represents the collection of all fetched module states with dynamic module support
type ExtendedFetchedState struct {
	ChainID       string                     `json:"chain_id"`
	Height        int64                      `json:"height"`
	Timestamp     time.Time                  `json:"timestamp"`
	ConsensusInfo *ConsensusInfo             `json:"consensus_info,omitempty"`
	ModulesFound  []string                   `json:"modules_found,omitempty"`
	ModuleStates  map[string]json.RawMessage `json:"module_states,omitempty"`
	Errors        map[string]string          `json:"errors,omitempty"`
}

// takeSnapshotRuntime is the updated version that fetches more comprehensive state
func takeSnapshotRuntime(height int64, chainConfig ChainRuntimeConfig, restURL string, snapshotDir string) error {
	// Create ExtendedFetchedState that will hold all module data dynamically
	state, err := fetchCompleteChainState(restURL, chainConfig.ChainID, height)
	if err != nil {
		return fmt.Errorf("failed to fetch chain state: %w", err)
	}

	// Save the extended state to a single comprehensive file
	completeStatePath := filepath.Join(snapshotDir, "complete_state.json")
	completeStateJSON, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal complete state: %w", err)
	}
	if err := os.WriteFile(completeStatePath, completeStateJSON, 0644); err != nil {
		return fmt.Errorf("failed to write complete state file: %w", err)
	}

	// Also save individual module states to separate files for easier access
	for moduleName, moduleData := range state.ModuleStates {
		moduleFilePath := filepath.Join(snapshotDir, fmt.Sprintf("%s_state.json", moduleName))
		if err := os.WriteFile(moduleFilePath, moduleData, 0644); err != nil {
			logger.Printf("[%s] Warning: Failed to write %s module state file: %v",
				chainConfig.ChainID, moduleName, err)
		}
	}

	// Write consensus info to separate file for easy access
	if state.ConsensusInfo != nil {
		consensusFilePath := filepath.Join(snapshotDir, "consensus_info.json")
		consensusJSON, err := json.MarshalIndent(state.ConsensusInfo, "", "  ")
		if err != nil {
			logger.Printf("[%s] Warning: Failed to marshal consensus info: %v", chainConfig.ChainID, err)
		} else {
			if err := os.WriteFile(consensusFilePath, consensusJSON, 0644); err != nil {
				logger.Printf("[%s] Warning: Failed to write consensus info file: %v", chainConfig.ChainID, err)
			}
		}
	}

	// Create a metadata file with summary information
	metadata := struct {
		ChainID         string    `json:"chain_id"`
		Height          int64     `json:"height"`
		Timestamp       time.Time `json:"timestamp"`
		ModulesCaptured []string  `json:"modules_captured"`
		ErrorCount      int       `json:"error_count"`
	}{
		ChainID:         state.ChainID,
		Height:          state.Height,
		Timestamp:       state.Timestamp,
		ModulesCaptured: state.ModulesFound,
		ErrorCount:      len(state.Errors),
	}

	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		logger.Printf("[%s] Warning: Failed to marshal metadata: %v", chainConfig.ChainID, err)
	} else {
		metadataPath := filepath.Join(snapshotDir, "metadata.json")
		if err := os.WriteFile(metadataPath, metadataJSON, 0644); err != nil {
			logger.Printf("[%s] Warning: Failed to write metadata file: %v", chainConfig.ChainID, err)
		}
	}

	logger.Printf("[%s] Successfully saved state snapshot with %d modules at height %d",
		chainConfig.ChainID, len(state.ModulesFound), height)
	return nil
}

// fetchCompleteChainState fetches consensus info and all module states
func fetchCompleteChainState(restURL, chainID string, height int64) (*ExtendedFetchedState, error) {
	state := &ExtendedFetchedState{
		ChainID:      chainID,
		Height:       height,
		Timestamp:    time.Now().UTC(),
		ModuleStates: make(map[string]json.RawMessage),
		Errors:       make(map[string]string),
	}

	// 1. Fetch consensus info first
	consensusInfo, err := fetchConsensusInfo(restURL, chainID, height)
	if err != nil {
		state.Errors["consensus_info"] = err.Error()
		logger.Printf("[%s] Warning: Failed to fetch consensus info: %v", chainID, err)
	} else {
		state.ConsensusInfo = consensusInfo
	}

	// 2. Discover all modules
	modules, err := discoverModules(restURL)
	if err != nil {
		state.Errors["module_discovery"] = err.Error()
		logger.Printf("[%s] Warning: Failed to discover modules: %v", chainID, err)

		// If module discovery fails, fall back to known core modules
		modules = []string{"auth", "bank", "staking", "gov", "distribution", "slashing"}
		logger.Printf("[%s] Falling back to known core modules: %v", chainID, modules)
	}

	// 3. Fetch state for each module concurrently
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, moduleName := range modules {
		wg.Add(1)
		go func(module string) {
			defer wg.Done()

			moduleData, err := fetchModuleState(restURL, module)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				state.Errors[fmt.Sprintf("module_%s", module)] = err.Error()
				logger.Printf("[%s] Warning: Failed to fetch %s module state: %v", chainID, module, err)
			} else if moduleData != nil {
				state.ModuleStates[module] = moduleData
				// Only add to ModulesFound if we actually got data
				if !containsString(state.ModulesFound, module) {
					state.ModulesFound = append(state.ModulesFound, module)
				}
			}
		}(moduleName)
	}

	wg.Wait()

	logger.Printf("[%s] Completed state fetch for %d modules with %d errors",
		chainID, len(state.ModulesFound), len(state.Errors))

	return state, nil
}

// fetchConsensusInfo gathers essential consensus-related information
func fetchConsensusInfo(restURL, chainID string, height int64) (*ConsensusInfo, error) {
	info := &ConsensusInfo{
		Height:  height,
		ChainID: chainID,
		Errors:  make(map[string]string),
	}

	// 1. Get validator set
	validatorURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/validatorsets/latest", restURL)
	validatorData, err := HTTPGet(validatorURL)
	if err != nil {
		info.Errors["validator_set"] = err.Error()
	} else {
		info.ValidatorSet = validatorData
	}

	// 2. Get consensus parameters
	paramsURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/blocks/latest", restURL)
	paramsData, err := HTTPGet(paramsURL)
	if err != nil {
		info.Errors["consensus_params"] = err.Error()
	} else {
		var blockInfo struct {
			Block struct {
				Header struct {
					Height  string `json:"height"`
					Time    string `json:"time"`
					ChainID string `json:"chain_id"`
				} `json:"header"`
				LastCommit struct {
					Height string `json:"height"`
				} `json:"last_commit"`
			} `json:"block"`
		}

		if err := json.Unmarshal(paramsData, &blockInfo); err == nil {
			info.LastBlockTimestamp = blockInfo.Block.Header.Time
		}

		// Also get actual consensus parameters
		consensusParamsURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/params", restURL)
		consensusParamsData, err := HTTPGet(consensusParamsURL)
		if err != nil {
			info.Errors["consensus_params_detail"] = err.Error()
		} else {
			info.ConsensusParams = consensusParamsData
		}
	}

	return info, nil
}

// discoverModules uses the reflection API to discover all modules
func discoverModules(restURL string) ([]string, error) {
	moduleURL := fmt.Sprintf("%s/cosmos/base/reflection/v1beta1/modules", restURL)
	moduleData, err := HTTPGet(moduleURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch modules: %w", err)
	}

	var moduleResp struct {
		ModuleVersions []struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"module_versions"`
	}

	if err := json.Unmarshal(moduleData, &moduleResp); err != nil {
		return nil, fmt.Errorf("failed to parse module response: %w", err)
	}

	modules := make([]string, 0, len(moduleResp.ModuleVersions))
	for _, module := range moduleResp.ModuleVersions {
		modules = append(modules, module.Name)
	}

	return modules, nil
}

// fetchModuleState fetches state for a specific module
func fetchModuleState(restURL, moduleName string) (json.RawMessage, error) {
	// Map of common endpoints patterns for modules
	endpointPatterns := []string{
		"/cosmos/%s/v1beta1/params", // Common params endpoint
		"/cosmos/%s/v1beta1",        // Base module path
		"/%s/v1beta1",               // Some modules use their own base path
	}

	// Special case handlers for known complex modules
	specialModuleEndpoints := map[string][]string{
		"auth": {
			"/cosmos/auth/v1beta1/params",
			"/cosmos/auth/v1beta1/accounts",
		},
		"bank": {
			"/cosmos/bank/v1beta1/params",
			"/cosmos/bank/v1beta1/supply",
			"/cosmos/bank/v1beta1/denoms_metadata",
			// Note: balances are handled separately due to size
		},
		"staking": {
			"/cosmos/staking/v1beta1/params",
			"/cosmos/staking/v1beta1/validators",
			"/cosmos/staking/v1beta1/historical_info/latest",
		},
		"gov": {
			"/cosmos/gov/v1beta1/params/voting",
			"/cosmos/gov/v1beta1/params/tallying",
			"/cosmos/gov/v1beta1/params/deposit",
			"/cosmos/gov/v1beta1/proposals?proposal_status=2", // Active proposals
		},
		"ibc": {
			"/ibc/core/client/v1/params",
			"/ibc/core/connection/v1/connections",
			"/ibc/core/channel/v1/channels",
			"/ibc/applications/transfer/v1/params",
		},
	}

	// Collect data from multiple endpoints
	moduleData := make(map[string]json.RawMessage)

	// Check if we have special handling for this module
	if endpoints, ok := specialModuleEndpoints[moduleName]; ok {
		for _, endpoint := range endpoints {
			endpointData, err := safeHTTPGet(fmt.Sprintf("%s%s", restURL, endpoint))
			if err != nil {
				continue // Skip failed endpoints
			}

			parts := strings.Split(endpoint, "/")
			endpointName := parts[len(parts)-1]
			moduleData[endpointName] = endpointData
		}
	} else {
		// Try generic patterns for other modules
		foundAny := false
		for _, pattern := range endpointPatterns {
			endpoint := fmt.Sprintf(pattern, moduleName)
			endpointData, err := safeHTTPGet(fmt.Sprintf("%s%s", restURL, endpoint))
			if err != nil {
				continue // Skip failed endpoints
			}

			parts := strings.Split(endpoint, "/")
			endpointName := parts[len(parts)-1]
			moduleData[endpointName] = endpointData
			foundAny = true
		}

		if !foundAny {
			// If no endpoint matched, try a custom module scan for endpoints
			customEndpoints := scanForModuleEndpoints(restURL, moduleName)
			for endpoint, data := range customEndpoints {
				moduleData[endpoint] = data
			}
		}
	}

	if len(moduleData) == 0 {
		return nil, fmt.Errorf("no data found for module %s", moduleName)
	}

	// Marshal the collected data
	return json.Marshal(moduleData)
}

// safeHTTPGet is a wrapper that doesn't error on 404s
func safeHTTPGet(url string) (json.RawMessage, error) {
	data, err := HTTPGet(url)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// scanForModuleEndpoints attempts to discover endpoints by common patterns
func scanForModuleEndpoints(restURL, moduleName string) map[string]json.RawMessage {
	results := make(map[string]json.RawMessage)

	// Common Cosmos SDK module endpoint suffixes to try
	commonSuffixes := []string{
		"params",
		"supply",
		"list",
		"total",
		"info",
		"status",
		"metadata",
	}

	for _, suffix := range commonSuffixes {
		endpoint := fmt.Sprintf("/cosmos/%s/v1beta1/%s", moduleName, suffix)
		data, err := safeHTTPGet(fmt.Sprintf("%s%s", restURL, endpoint))
		if err == nil && len(data) > 0 {
			results[suffix] = data
		}
	}

	return results
}

// Helper function to check if a string is in a slice
func containsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
