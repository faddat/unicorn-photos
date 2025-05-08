package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// List of standard Cosmos SDK modules to capture
var standardModules = []string{
	"auth", "bank", "staking", "distribution",
	"gov", "slashing", "params", "evidence",
	"upgrade", "mint", "crisis", "ibc", "feegrant",
}

// Function to take a snapshot at a specific height
func takeSnapshotRuntime(height int64, chainConfig ChainRuntimeConfig, restEndpoint, snapshotDir string) error {
	logger.Printf("[%s] Starting comprehensive snapshot at height %d", chainConfig.ChainID, height)

	// Initialize progress tracker
	progress := SnapshotProgress{
		ChainID:          chainConfig.ChainID,
		Height:           height,
		StartTime:        time.Now(),
		TotalModules:     len(standardModules),
		CompletedModules: []string{},
		PercentComplete:  0,
	}

	// Update status tracker with initial progress
	statusTracker := GetStatusTracker()
	status := statusTracker.GetChainStatus(chainConfig.ChainID)
	status.Status = "active"
	status.LastProgress = &progress
	statusTracker.UpdateChainStatus(chainConfig.ChainID, status)

	// Create metadata file with chain info and timestamp
	metadata := map[string]interface{}{
		"chain_id":      chainConfig.ChainID,
		"height":        height,
		"timestamp":     time.Now().Format(time.RFC3339),
		"snapshot_tool": "unicorn-photos",
		"version":       "1.0.0",
	}
	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err == nil {
		if err := os.WriteFile(filepath.Join(snapshotDir, "metadata.json"), metadataJSON, 0644); err != nil {
			logger.Printf("[%s] Warning: Failed to write metadata file: %v", chainConfig.ChainID, err)
		}
	}

	// Create a context with timeout for the whole operation
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Channels for results and errors
	type moduleResult struct {
		name  string
		data  json.RawMessage
		error error
	}
	resultCh := make(chan moduleResult, len(standardModules))

	// Create a semaphore to limit concurrency
	const maxConcurrentFetches = 4
	sem := make(chan struct{}, maxConcurrentFetches)

	// Track how many modules we're processing
	var wg sync.WaitGroup
	var mu sync.Mutex // For synchronizing progress updates

	// Combine all fetched states
	completeState := make(map[string]json.RawMessage)
	var moduleErrors = make(map[string]string)
	var modulesFound []string

	// Prepare to fetch consensus info
	wg.Add(1)
	go func() {
		defer wg.Done()
		sem <- struct{}{}        // Acquire semaphore
		defer func() { <-sem }() // Release semaphore

		mu.Lock()
		progress.CurrentModule = "consensus"
		updateProgress(&progress, chainConfig.ChainID)
		mu.Unlock()

		// Fetch consensus info like validator set and params
		consensusInfo, err := fetchConsensusInfo(restEndpoint, chainConfig.ChainID, height)
		if err != nil {
			logger.Printf("[%s] Warning: Failed to fetch consensus info: %v", chainConfig.ChainID, err)
			moduleErrors["consensus"] = err.Error()
			resultCh <- moduleResult{name: "consensus", error: err}
			return
		}

		consensusJSON, err := json.MarshalIndent(consensusInfo, "", "  ")
		if err != nil {
			moduleErrors["consensus"] = fmt.Sprintf("failed to marshal consensus info: %v", err)
			resultCh <- moduleResult{name: "consensus", error: err}
			return
		}

		// Write consensus info to file
		err = os.WriteFile(filepath.Join(snapshotDir, "consensus_info.json"), consensusJSON, 0644)
		if err != nil {
			moduleErrors["consensus"] = fmt.Sprintf("failed to write consensus info file: %v", err)
			resultCh <- moduleResult{name: "consensus", error: err}
			return
		}

		mu.Lock()
		completeState["consensus_info"] = consensusJSON
		modulesFound = append(modulesFound, "consensus")
		progress.CompletedModules = append(progress.CompletedModules, "consensus")
		updateProgress(&progress, chainConfig.ChainID)
		mu.Unlock()

		resultCh <- moduleResult{name: "consensus", data: consensusJSON}
	}()

	// Launch goroutines to fetch each module's state
	for _, moduleName := range standardModules {
		wg.Add(1)
		go func(module string) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			// Update progress
			mu.Lock()
			progress.CurrentModule = module
			updateProgress(&progress, chainConfig.ChainID)
			mu.Unlock()

			// Fetch module state - use the context from parent
			logger.Printf("[%s] Fetching %s module state...", chainConfig.ChainID, module)
			moduleData, err := fetchModuleState(restEndpoint, module)

			if err != nil {
				logger.Printf("[%s] Warning: Failed to fetch %s module state: %v", chainConfig.ChainID, module, err)
				moduleErrors[module] = err.Error()
				resultCh <- moduleResult{name: module, error: err}
				return
			}

			if len(moduleData) == 0 || string(moduleData) == "null" || string(moduleData) == "{}" {
				logger.Printf("[%s] Module %s returned empty or null data", chainConfig.ChainID, module)
				resultCh <- moduleResult{name: module, error: fmt.Errorf("empty or null data")}
				return
			}

			// Write module state to file
			err = os.WriteFile(filepath.Join(snapshotDir, module+"_state.json"), moduleData, 0644)
			if err != nil {
				moduleErrors[module] = fmt.Sprintf("failed to write %s state file: %v", module, err)
				resultCh <- moduleResult{name: module, error: err}
				return
			}

			// Add to complete state and update progress
			mu.Lock()
			completeState[module] = moduleData
			modulesFound = append(modulesFound, module)
			progress.CompletedModules = append(progress.CompletedModules, module)
			updateProgress(&progress, chainConfig.ChainID)
			mu.Unlock()

			// Send result
			select {
			case resultCh <- moduleResult{name: module, data: moduleData}:
			case <-ctx.Done():
				// Context was canceled, log and return
				logger.Printf("[%s] Context canceled while fetching %s module", chainConfig.ChainID, module)
			}
		}(moduleName)
	}

	// Wait for all goroutines to finish in a separate goroutine
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Process results as they come in
	for result := range resultCh {
		select {
		case <-ctx.Done():
			logger.Printf("[%s] Snapshot operation canceled", chainConfig.ChainID)
			return ctx.Err()
		default:
			percentComplete := int((float32(len(progress.CompletedModules)) / float32(progress.TotalModules+1)) * 100)
			logger.Printf("[%s] Module %s fetched (%d%% complete)",
				chainConfig.ChainID, result.name, percentComplete)
		}
	}

	// Create a complete state file containing all fetched module states
	completeStateData := map[string]interface{}{
		"chain_id":       chainConfig.ChainID,
		"height":         height,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"consensus_info": completeState["consensus_info"],
		"modules_found":  modulesFound,
		"module_states":  completeState,
	}

	// Write the complete state file
	completeStateJSON, err := json.MarshalIndent(completeStateData, "", "  ")
	if err != nil {
		logger.Printf("[%s] Warning: Failed to marshal complete state: %v", chainConfig.ChainID, err)
	} else {
		if err := os.WriteFile(filepath.Join(snapshotDir, "complete_state.json"), completeStateJSON, 0644); err != nil {
			logger.Printf("[%s] Warning: Failed to write complete state file: %v", chainConfig.ChainID, err)
		}
	}

	// If there were errors, write them to a file
	if len(moduleErrors) > 0 {
		errorsJSON, err := json.MarshalIndent(moduleErrors, "", "  ")
		if err != nil {
			logger.Printf("[%s] Warning: Failed to marshal errors: %v", chainConfig.ChainID, err)
		} else {
			if err := os.WriteFile(filepath.Join(snapshotDir, "fetch_errors_state.json"), errorsJSON, 0644); err != nil {
				logger.Printf("[%s] Warning: Failed to write errors file: %v", chainConfig.ChainID, err)
			}
		}
	}

	// Final update to progress
	progress.PercentComplete = 100
	progress.CurrentModule = "complete"
	status.LastProgress = &progress
	statusTracker.UpdateChainStatus(chainConfig.ChainID, status)

	logger.Printf("[%s] Completed comprehensive snapshot at height %d with %d modules",
		chainConfig.ChainID, height, len(modulesFound))
	return nil
}

// updateProgress updates the progress in the status tracker
func updateProgress(progress *SnapshotProgress, chainID string) {
	progress.PercentComplete = int((float32(len(progress.CompletedModules)) / float32(progress.TotalModules+1)) * 100)

	// Update the status tracker
	statusTracker := GetStatusTracker()
	status := statusTracker.GetChainStatus(chainID)
	status.LastProgress = progress
	statusTracker.UpdateChainStatus(chainID, status)

	// Log progress
	logger.Printf("[%s] Progress: %d%% - Module: %s (Completed: %d/%d)",
		chainID, progress.PercentComplete, progress.CurrentModule,
		len(progress.CompletedModules), progress.TotalModules+1)
}

// fetchConsensusInfo retrieves key consensus-related information
func fetchConsensusInfo(restURL, chainID string, height int64) (*ConsensusInfo, error) {
	info := &ConsensusInfo{
		Height:  height,
		ChainID: chainID,
		Errors:  make(map[string]string),
	}

	// Fetch validator set
	validatorSetURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/validatorsets/%d", restURL, height)
	validatorSetData, err := safeHTTPGet(validatorSetURL)
	if err != nil {
		info.Errors["validator_set"] = fmt.Sprintf("failed to fetch validator set: %v", err)
	} else {
		info.ValidatorSet = validatorSetData
	}

	// Fetch consensus params
	consensusParamsURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/params", restURL)
	consensusParamsData, err := safeHTTPGet(consensusParamsURL)
	if err != nil {
		info.Errors["consensus_params_detail"] = fmt.Sprintf("request to %s failed with status %v: %s",
			consensusParamsURL, err, consensusParamsData)
	} else {
		info.ConsensusParams = consensusParamsData
	}

	// Fetch last block timestamp
	blockInfoURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/blocks/%d", restURL, height)
	blockData, err := safeHTTPGet(blockInfoURL)
	if err != nil {
		info.Errors["block_info"] = fmt.Sprintf("failed to fetch block info: %v", err)
	} else {
		// Extract timestamp from block data
		var blockInfo struct {
			Block struct {
				Header struct {
					Time string `json:"time"`
				} `json:"header"`
			} `json:"block"`
		}
		if err := json.Unmarshal(blockData, &blockInfo); err == nil {
			info.LastBlockTimestamp = blockInfo.Block.Header.Time
		} else {
			info.Errors["block_timestamp"] = fmt.Sprintf("failed to parse block timestamp: %v", err)
		}
	}

	return info, nil
}

// fetchModuleState now optimized for comprehensive state capture
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
			"/cosmos/bank/v1beta1/balances", // Special pagination handler for this
		},
		"staking": {
			"/cosmos/staking/v1beta1/params",
			"/cosmos/staking/v1beta1/pool",
			"/cosmos/staking/v1beta1/validators",
			"/cosmos/staking/v1beta1/delegations",                             // All delegations - might be large
			"/cosmos/staking/v1beta1/validators?status=BOND_STATUS_UNBONDED",  // Include unbonded
			"/cosmos/staking/v1beta1/validators?status=BOND_STATUS_UNBONDING", // Include unbonding
		},
		"distribution": {
			"/cosmos/distribution/v1beta1/params",
			"/cosmos/distribution/v1beta1/community_pool",
		},
		"gov": {
			"/cosmos/gov/v1beta1/params/voting",
			"/cosmos/gov/v1beta1/params/tallying",
			"/cosmos/gov/v1beta1/params/deposit",
			"/cosmos/gov/v1beta1/proposals",
		},
		"slashing": {
			"/cosmos/slashing/v1beta1/params",
			"/cosmos/slashing/v1beta1/signing_infos",
		},
		"ibc": {
			"/ibc/core/client/v1/params",
			"/ibc/core/connection/v1/connections",
			"/ibc/core/channel/v1/channels",
		},
	}

	// For special module handling with known complex structure
	if endpoints, ok := specialModuleEndpoints[moduleName]; ok {
		// Gather data from all endpoints for this module
		moduleData := make(map[string]json.RawMessage)
		hasData := false

		for _, endpoint := range endpoints {
			// Handle special case for paginated resources
			if strings.Contains(endpoint, "/balances") {
				// Use pagination for accounts with balances
				accountsWithBalances, err := fetchPaginatedResource(restURL, endpoint, "balances")
				if err == nil && len(accountsWithBalances) > 0 {
					moduleData["balances"] = accountsWithBalances
					hasData = true
				}
				continue
			}

			// Handle special case for delegations (large dataset)
			if strings.Contains(endpoint, "/delegations") {
				delegations, err := fetchPaginatedResource(restURL, endpoint, "delegation_responses")
				if err == nil && len(delegations) > 0 {
					moduleData["delegations"] = delegations
					hasData = true
				}
				continue
			}

			// Regular endpoint
			urlPath := fmt.Sprintf("%s%s", restURL, endpoint)
			data, err := safeHTTPGet(urlPath)
			if err != nil {
				logger.Printf("Warning: Error fetching from %s: %v", urlPath, err)
				continue
			}

			if len(data) > 0 && string(data) != "null" && string(data) != "{}" {
				// For params endpoints, extract the params object
				if strings.Contains(endpoint, "/params") {
					var paramsResp struct {
						Params json.RawMessage `json:"params"`
					}
					if json.Unmarshal(data, &paramsResp) == nil && len(paramsResp.Params) > 0 {
						moduleData[filepath.Base(endpoint)] = paramsResp.Params
					} else {
						moduleData[filepath.Base(endpoint)] = data
					}
				} else {
					moduleData[filepath.Base(endpoint)] = data
				}
				hasData = true
			}
		}

		// If we collected any data, marshal it to JSON and return
		if hasData {
			result, err := json.Marshal(moduleData)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal %s module data: %w", moduleName, err)
			}
			return result, nil
		}

		// If no special endpoints succeeded, fall back to general patterns
	}

	// Try common patterns for any module
	for _, pattern := range endpointPatterns {
		endpoint := fmt.Sprintf(pattern, moduleName)
		urlPath := fmt.Sprintf("%s%s", restURL, endpoint)
		data, err := safeHTTPGet(urlPath)
		if err == nil && len(data) > 0 && string(data) != "null" && string(data) != "{}" {
			return data, nil
		}
	}

	// Try to query params for the module (most common endpoint)
	paramsURL := fmt.Sprintf("%s/cosmos/%s/v1beta1/params", restURL, moduleName)
	data, err := safeHTTPGet(paramsURL)
	if err == nil && len(data) > 0 && string(data) != "null" && string(data) != "{}" {
		return data, nil
	}

	// If all attempts failed, return an error
	return nil, fmt.Errorf("failed to fetch %s module state from %s (no valid endpoints found)", moduleName, restURL)
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

		var rawResp struct {
			Pagination struct {
				NextKey string `json:"next_key"`
				Total   string `json:"total"`
			} `json:"pagination"`
			Results json.RawMessage `json:"-"` // Will match to the resultKey
		}

		// Parse the outer structure to get pagination info
		if err := json.Unmarshal(data, &rawResp); err != nil {
			return nil, fmt.Errorf("failed to parse pagination response: %w", err)
		}

		// Extract items using the resultKey
		var tempMap map[string]json.RawMessage
		if err := json.Unmarshal(data, &tempMap); err != nil {
			return nil, fmt.Errorf("failed to extract %s from response: %w", resultKey, err)
		}

		if items, ok := tempMap[resultKey]; ok && len(items) > 0 {
			var currentItems []json.RawMessage
			if err := json.Unmarshal(items, &currentItems); err != nil {
				return nil, fmt.Errorf("failed to parse %s items: %w", resultKey, err)
			}
			allItems = append(allItems, currentItems...)
		}

		// Check if we need to continue pagination
		if rawResp.Pagination.NextKey == "" {
			break
		}
		nextKey = rawResp.Pagination.NextKey
	}

	// Return the combined results
	result, err := json.Marshal(allItems)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal combined results: %w", err)
	}

	return result, nil
}

// safeHTTPGet performs an HTTP GET request with timeout and error handling
func safeHTTPGet(url string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for %s: %w", url, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request to %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body from %s: %w", url, err)
	}

	if resp.StatusCode >= 400 {
		return body, fmt.Errorf("request to %s failed with status %d: %s", url, resp.StatusCode, string(body))
	}

	return body, nil
}
