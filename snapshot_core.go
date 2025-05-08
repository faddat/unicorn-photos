package main

import (
	"encoding/json"
	"fmt"
	"net/url"
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
			"/cosmos/auth/v1beta1/accounts", // All accounts
		},
		"bank": {
			"/cosmos/bank/v1beta1/params",
			"/cosmos/bank/v1beta1/supply",
			"/cosmos/bank/v1beta1/denoms_metadata",
			// Note: We'll handle balances separately with special logic
		},
		"staking": {
			"/cosmos/staking/v1beta1/params",
			"/cosmos/staking/v1beta1/validators",
			"/cosmos/staking/v1beta1/validators?status=BOND_STATUS_UNBONDED",  // Include unbonded validators
			"/cosmos/staking/v1beta1/validators?status=BOND_STATUS_UNBONDING", // Include unbonding validators
			"/cosmos/staking/v1beta1/historical_info/latest",
			"/cosmos/staking/v1beta1/pool", // Staking pool info
		},
		"gov": {
			"/cosmos/gov/v1beta1/params/voting",
			"/cosmos/gov/v1beta1/params/tallying",
			"/cosmos/gov/v1beta1/params/deposit",
			"/cosmos/gov/v1beta1/proposals?proposal_status=1", // Deposit period
			"/cosmos/gov/v1beta1/proposals?proposal_status=2", // Voting period
			"/cosmos/gov/v1beta1/proposals?proposal_status=3", // Passed
			"/cosmos/gov/v1beta1/proposals?proposal_status=4", // Rejected
		},
		"distribution": {
			"/cosmos/distribution/v1beta1/params",
			"/cosmos/distribution/v1beta1/community_pool",
			"/cosmos/distribution/v1beta1/validator_outstanding_rewards",
		},
		"slashing": {
			"/cosmos/slashing/v1beta1/params",
			"/cosmos/slashing/v1beta1/signing_infos",
		},
		"ibc": {
			"/ibc/core/client/v1/params",
			"/ibc/core/connection/v1/connections",
			"/ibc/core/channel/v1/channels",
			"/ibc/applications/transfer/v1/params",
			"/ibc/applications/transfer/v1/denom_traces",
		},
	}

	// Collect data from multiple endpoints
	moduleData := make(map[string]json.RawMessage)

	// Special handling for bank module to fetch complete state including balances
	if moduleName == "bank" {
		return fetchBankModuleCompleteState(restURL)
	}

	// Check if we have special handling for this module
	if endpoints, ok := specialModuleEndpoints[moduleName]; ok {
		for _, endpoint := range endpoints {
			endpointData, err := safeHTTPGet(fmt.Sprintf("%s%s", restURL, endpoint))
			if err != nil {
				continue // Skip failed endpoints
			}

			// Create endpoint name based on the path
			parts := strings.Split(endpoint, "/")
			endpointName := parts[len(parts)-1]
			// Handle query params in endpoint name
			if strings.Contains(endpointName, "?") {
				subParts := strings.Split(endpointName, "?")
				endpointName = subParts[0] + "_" + strings.ReplaceAll(subParts[1], "=", "_")
				endpointName = strings.ReplaceAll(endpointName, "&", "_")
			}
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

// fetchBankModuleCompleteState fetches complete bank module state including account balances
func fetchBankModuleCompleteState(restURL string) (json.RawMessage, error) {
	bankState := make(map[string]json.RawMessage)

	// 1. Fetch params
	paramsData, err := safeHTTPGet(fmt.Sprintf("%s%s", restURL, "/cosmos/bank/v1beta1/params"))
	if err == nil {
		bankState["params"] = paramsData
	}

	// 2. Fetch supply
	supplyData, err := fetchPaginatedResource(restURL, "/cosmos/bank/v1beta1/supply", "supply")
	if err == nil {
		bankState["supply"] = supplyData
	}

	// 3. Fetch denom metadata
	denomMetadataData, err := fetchPaginatedResource(restURL, "/cosmos/bank/v1beta1/denoms_metadata", "metadatas")
	if err == nil {
		bankState["denoms_metadata"] = denomMetadataData
	}

	// 4. Fetch account balances - this requires first getting all accounts from auth module
	// and then fetching balances for each account
	authAccountsData, err := fetchPaginatedResource(restURL, "/cosmos/auth/v1beta1/accounts", "accounts")
	if err == nil {
		// Parse accounts to get addresses
		var accounts []struct {
			Account struct {
				Address string `json:"address"`
			} `json:"account"`
		}

		if err := json.Unmarshal(authAccountsData, &accounts); err == nil {
			// Now fetch balances for each account
			var allBalances []struct {
				Address string `json:"address"`
				Coins   []struct {
					Denom  string `json:"denom"`
					Amount string `json:"amount"`
				} `json:"coins"`
			}

			// Use a limited number of concurrent requests
			semaphore := make(chan struct{}, 20)
			var wg sync.WaitGroup
			var mu sync.Mutex

			for _, acc := range accounts {
				if acc.Account.Address == "" {
					continue
				}

				wg.Add(1)
				go func(address string) {
					defer wg.Done()
					semaphore <- struct{}{}        // Acquire token
					defer func() { <-semaphore }() // Release token

					balanceURL := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", restURL, address)
					balanceData, err := safeHTTPGet(balanceURL)
					if err != nil {
						return
					}

					var balance struct {
						Balances []struct {
							Denom  string `json:"denom"`
							Amount string `json:"amount"`
						} `json:"balances"`
					}

					if err := json.Unmarshal(balanceData, &balance); err != nil {
						return
					}

					// Only include if there are balances
					if len(balance.Balances) > 0 {
						mu.Lock()
						allBalances = append(allBalances, struct {
							Address string `json:"address"`
							Coins   []struct {
								Denom  string `json:"denom"`
								Amount string `json:"amount"`
							} `json:"coins"`
						}{
							Address: address,
							Coins:   balance.Balances,
						})
						mu.Unlock()
					}
				}(acc.Account.Address)
			}

			wg.Wait()

			// Add the balances to bank state
			balancesJSON, err := json.Marshal(allBalances)
			if err == nil {
				bankState["balances"] = balancesJSON
			}
		}
	}

	// Return the complete bank state
	return json.Marshal(bankState)
}

// fetchPaginatedResource fetches a paginated resource and returns the combined result
func fetchPaginatedResource(restURL, endpoint, resultKey string) (json.RawMessage, error) {
	var allItems []json.RawMessage
	nextKey := ""

	for {
		urlPath := fmt.Sprintf("%s%s", restURL, endpoint)
		if nextKey != "" {
			if strings.Contains(urlPath, "?") {
				urlPath += "&pagination.key=" + url.QueryEscape(nextKey)
			} else {
				urlPath += "?pagination.key=" + url.QueryEscape(nextKey)
			}
		} else if !strings.Contains(urlPath, "?") {
			urlPath += "?pagination.limit=100" // Add pagination limit for first request
		}

		data, err := safeHTTPGet(urlPath)
		if err != nil {
			return nil, err
		}

		var result struct {
			Pagination struct {
				NextKey string `json:"next_key"`
			} `json:"pagination"`
		}

		if err := json.Unmarshal(data, &result); err != nil {
			return nil, err
		}

		// Extract the items array
		var resultData map[string]json.RawMessage
		if err := json.Unmarshal(data, &resultData); err != nil {
			return nil, err
		}

		itemsData, ok := resultData[resultKey]
		if ok {
			var items []json.RawMessage
			if err := json.Unmarshal(itemsData, &items); err != nil {
				return nil, err
			}
			allItems = append(allItems, items...)
		}

		// Check if there are more pages
		if result.Pagination.NextKey == "" {
			break
		}
		nextKey = result.Pagination.NextKey
	}

	// Combine all items into a single array
	return json.Marshal(allItems)
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
