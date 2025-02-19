package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcutil/bech32"
)

// Constants
const RPC_URL = "https://rpc.unicorn.meme"

// State tracks progress
type State struct {
	BlockHeight          int64             `json:"block_height"`
	AccountsComplete     bool              `json:"accounts_complete"`
	BalancesComplete     bool              `json:"balances_complete"`
	ValidatorsComplete   bool              `json:"validators_complete"`
	LastCompleteHeight   int64             `json:"last_complete_height"`
	LastIncompleteHeight int64             `json:"last_incomplete_height"`
	InProgressFiles      map[string]string `json:"in_progress_files"` // tracks temp files
}

// Logger
var logger = log.New(os.Stdout, "[SNAPSHOT] ", log.Ldate|log.Ltime|log.Lmicroseconds)

// HTTP client
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	},
}

// Add this struct to track total balances
type TotalBalance struct {
	Address       string `json:"address"`
	Liquid        string `json:"liquid_amount"`
	Staked        string `json:"staked_amount"`
	Total         string `json:"total_amount"`
	LiquidUnicorn string `json:"liquid_unicorn"`
	StakedUnicorn string `json:"staked_unicorn"`
	TotalUnicorn  string `json:"total_amount"`
}

// loadState loads or initializes state
func loadState(snapshotDir string) (*State, error) {
	logger.Println("Loading state from state.json.tmp...")
	stateFile := filepath.Join(snapshotDir, "state.json.tmp")
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		logger.Println("Initializing new state.")
		return &State{}, nil
	}
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read state: %v", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state: %v", err)
	}
	logger.Printf("Loaded state with block height %d", state.BlockHeight)
	return &state, nil
}

// saveState saves state to file
func saveState(state *State, final bool, snapshotDir string) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}
	filename := filepath.Join(snapshotDir, "state.json.tmp")
	if final {
		filename = filepath.Join(snapshotDir, "state.json")
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return err
	}
	logger.Printf("State saved to %s", filename)
	return nil
}

// Update rpcQuery to add retries and better error handling
func rpcQuery(method string, params map[string]interface{}) (map[string]interface{}, error) {
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			logger.Printf("Retry %d/%d for %s...", retry, maxRetries, method)
			time.Sleep(time.Second * time.Duration(retry)) // Exponential backoff
		}

		payload := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  method,
			"params":  params,
		}

		body, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %v", err)
		}

		resp, err := httpClient.Post(RPC_URL, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = fmt.Errorf("RPC request failed: %v", err)
			continue
		}

		// Read full response for better error reporting
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("reading response body: %v", err)
			continue
		}

		// Try to decode as JSON
		var result map[string]interface{}
		if err := json.Unmarshal(respBody, &result); err != nil {
			lastErr = fmt.Errorf("decode response failed (%s): %v", string(respBody), err)
			continue
		}

		if errData, ok := result["error"]; ok {
			lastErr = fmt.Errorf("RPC error: %v", errData)
			continue
		}

		resultData, ok := result["result"].(map[string]interface{})
		if !ok {
			lastErr = fmt.Errorf("invalid result format: %v", result["result"])
			continue
		}

		return resultData, nil
	}

	return nil, fmt.Errorf("after %d retries: %v", maxRetries, lastErr)
}

// fetchRPC fetches data via RPC with pagination
func fetchRPC(path, cacheFile string, height int64, snapshotDir string) ([]map[string]interface{}, error) {
	fullPath := filepath.Join(snapshotDir, cacheFile)
	if data, err := os.ReadFile(fullPath); err == nil {
		var items []map[string]interface{}
		if err := json.Unmarshal(data, &items); err == nil {
			logger.Printf("Loaded %d items from %s", len(items), fullPath)
			return items, nil
		}
	}

	var allItems []map[string]interface{}
	nextKey := ""
	pageSize := 100

	for {
		params := map[string]interface{}{
			"path":   path,
			"height": fmt.Sprintf("%d", height),
			"prove":  false,
		}
		if nextKey != "" {
			params["pagination.key"] = nextKey
		}
		params["pagination.limit"] = fmt.Sprintf("%d", pageSize)

		rpcParams, err := rpcQuery("abci_query", params)
		if err != nil {
			logger.Printf("RPC failed for %s: %v", path, err)
			return nil, err
		}

		response, ok := rpcParams["response"].(map[string]interface{})
		if !ok || response["value"] == nil {
			logger.Printf("Invalid RPC response for %s", path)
			return nil, fmt.Errorf("invalid response")
		}

		value, err := base64.StdEncoding.DecodeString(response["value"].(string))
		if err != nil {
			logger.Printf("Decode failed for %s: %v", path, err)
			return nil, err
		}

		// Handle different response formats
		if strings.Contains(path, "auth.v1beta1/accounts") {
			var accountResp struct {
				Accounts   []map[string]interface{} `json:"accounts"`
				Pagination struct {
					NextKey string `json:"next_key"`
					Total   string `json:"total"`
				} `json:"pagination"`
			}
			if err := json.Unmarshal(value, &accountResp); err != nil {
				logger.Printf("Unmarshal failed for accounts: %v", err)
				return nil, err
			}
			allItems = append(allItems, accountResp.Accounts...)
			nextKey = accountResp.Pagination.NextKey
			total := "unknown"
			if accountResp.Pagination.Total != "" {
				total = accountResp.Pagination.Total
			}
			logger.Printf("Fetched %d accounts (total: %d, expected: %s) with next_key: %s",
				len(accountResp.Accounts), len(allItems), total, nextKey)

			// Save progress periodically
			if len(allItems) > 0 && len(allItems)%1000 == 0 {
				tempBytes, _ := json.Marshal(allItems)
				if err := os.WriteFile(filepath.Join(snapshotDir, "accounts.json.tmp"), tempBytes, 0644); err != nil {
					logger.Printf("Failed to save progress: %v", err)
				} else {
					logger.Printf("Saved progress: %d accounts", len(allItems))
				}
			}
		} else {
			var pageData struct {
				Items      []map[string]interface{} `json:"items"`
				Pagination struct {
					NextKey string `json:"next_key"`
				} `json:"pagination"`
			}
			if err := json.Unmarshal(value, &pageData); err != nil {
				logger.Printf("Unmarshal failed for %s: %v", path, err)
				return nil, err
			}
			allItems = append(allItems, pageData.Items...)
			nextKey = pageData.Pagination.NextKey
		}

		if nextKey == "" {
			break
		}
		logger.Printf("Fetching next page with key: %s", nextKey)
	}

	dataBytes, _ := json.Marshal(allItems)
	if err := os.WriteFile(fullPath, dataBytes, 0644); err != nil {
		logger.Printf("Failed to cache to %s: %v", fullPath, err)
	}
	logger.Printf("Fetched total %d items via RPC for %s", len(allItems), path)
	return allItems, nil
}

// fetchValidators fetches validators via RPC
func fetchValidators(height int64, snapshotDir string) ([]map[string]interface{}, error) {
	cacheFile := filepath.Join(snapshotDir, "validators.json.tmp")
	if data, err := os.ReadFile(cacheFile); err == nil {
		var validators []map[string]interface{}
		if err := json.Unmarshal(data, &validators); err == nil {
			logger.Printf("Loaded %d validators from %s", len(validators), cacheFile)
			return validators, nil
		}
	}

	rpcParams, err := rpcQuery("validators", map[string]interface{}{
		"height":   fmt.Sprintf("%d", height),
		"page":     "1",
		"per_page": "1000", // Adjust based on needs
	})
	if err != nil {
		logger.Printf("Validators RPC failed: %v", err)
		return nil, err
	}

	validators, ok := rpcParams["validators"].([]interface{})
	if !ok {
		logger.Println("Invalid validators response")
		return nil, fmt.Errorf("invalid validators")
	}

	var result []map[string]interface{}
	for _, v := range validators {
		result = append(result, v.(map[string]interface{}))
	}

	dataBytes, _ := json.Marshal(result)
	if err := os.WriteFile(cacheFile, dataBytes, 0644); err != nil {
		logger.Printf("Failed to cache validators: %v", err)
	}
	logger.Printf("Fetched %d validators via RPC", len(result))
	return result, nil
}

// fetchParamsRPC fetches parameters via RPC
func fetchParamsRPC(module string, height int64, snapshotDir string) (map[string]interface{}, error) {
	cacheFile := filepath.Join(snapshotDir, fmt.Sprintf("%s_params.json.tmp", module))
	if data, err := os.ReadFile(cacheFile); err == nil {
		var params map[string]interface{}
		if err := json.Unmarshal(data, &params); err == nil {
			logger.Printf("Loaded params from %s", cacheFile)
			return params, nil
		}
	}

	path := fmt.Sprintf("/cosmos.%s.v1beta1/params", module)
	rpcParams, err := rpcQuery("abci_query", map[string]interface{}{
		"path":   path,
		"data":   "",
		"height": fmt.Sprintf("%d", height),
	})
	if err != nil {
		return getDefaultParams(fmt.Sprintf("/cosmos/%s/v1beta1/params", module)), nil
	}

	response, ok := rpcParams["response"].(map[string]interface{})
	if !ok || response["value"] == nil {
		return getDefaultParams(fmt.Sprintf("/cosmos/%s/v1beta1/params", module)), nil
	}

	value, err := base64.StdEncoding.DecodeString(response["value"].(string))
	if err != nil {
		return getDefaultParams(fmt.Sprintf("/cosmos/%s/v1beta1/params", module)), nil
	}

	var params map[string]interface{}
	if err := json.Unmarshal(value, &params); err != nil {
		return getDefaultParams(fmt.Sprintf("/cosmos/%s/v1beta1/params", module)), nil
	}

	dataBytes, _ := json.Marshal(params)
	if err := os.WriteFile(cacheFile, dataBytes, 0644); err != nil {
		logger.Printf("Failed to cache params: %v", err)
	}
	logger.Printf("Fetched params for %s via RPC", module)
	return params, nil
}

// getDefaultParams provides fallback parameters
func getDefaultParams(endpoint string) map[string]interface{} {
	switch endpoint {
	case "/cosmos/auth/v1beta1/params":
		return map[string]interface{}{
			"max_memo_characters": "256",
			"tx_sig_limit":        "7",
		}
	case "/cosmos/bank/v1beta1/params":
		return map[string]interface{}{
			"send_enabled": true,
		}
	case "/cosmos/staking/v1beta1/params":
		return map[string]interface{}{
			"unbonding_time": "1814400s",
			"max_validators": 100,
			"bond_denom":     "umeme",
		}
	case "/cosmos/distribution/v1beta1/params":
		return map[string]interface{}{
			"community_tax": "0.020000000000000000",
		}
	case "/cosmos/gov/v1beta1/params":
		return map[string]interface{}{
			"voting_period": "172800s",
			"min_deposit":   []interface{}{map[string]interface{}{"denom": "umeme", "amount": "10000000"}},
		}
	default:
		return map[string]interface{}{}
	}
}

// Update getLatestBlockHeight to add REST fallback
func getLatestBlockHeight() (int64, error) {
	// Try RPC first
	rpcParams, err := rpcQuery("block", map[string]interface{}{})
	if err == nil {
		block := rpcParams["block"].(map[string]interface{})
		header := block["header"].(map[string]interface{})
		heightStr := header["height"].(string)
		height, err := strconv.ParseInt(heightStr, 10, 64)
		if err == nil {
			logger.Printf("Latest block height from RPC: %d", height)
			return height, nil
		}
	}

	// Fallback to REST API
	logger.Printf("RPC failed (%v), trying REST API...", err)
	resp, err := httpClient.Get("https://rest.unicorn.meme/cosmos/base/tendermint/v1beta1/blocks/latest")
	if err != nil {
		return 0, fmt.Errorf("both RPC and REST failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode REST response: %v", err)
	}

	height, err := strconv.ParseInt(result.Block.Header.Height, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse height: %v", err)
	}

	logger.Printf("Latest block height from REST: %d", height)
	return height, nil
}

// convertAddress converts Bech32 addresses
func convertAddress(addr, oldPrefix, newPrefix string) (string, error) {
	hrp, data, err := bech32.Decode(addr)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(hrp, oldPrefix) {
		return addr, nil
	}
	suffix := strings.TrimPrefix(hrp, oldPrefix)
	newHRP := newPrefix + suffix
	newAddr, err := bech32.Encode(newHRP, data)
	if err != nil {
		return "", err
	}
	return newAddr, nil
}

// convertAddresses updates Bech32 addresses in app state
func convertAddresses(appState map[string]interface{}, oldPrefix, newPrefix string) {
	convertField := func(m map[string]interface{}, key string) {
		if val, ok := m[key].(string); ok {
			if newVal, err := convertAddress(val, oldPrefix, newPrefix); err == nil {
				m[key] = newVal
			}
		}
	}

	if auth, ok := appState["auth"].(map[string]interface{}); ok {
		if accounts, ok := auth["accounts"].([]map[string]interface{}); ok {
			for _, acc := range accounts {
				convertField(acc, "address")
			}
		}
	}
	if bank, ok := appState["bank"].(map[string]interface{}); ok {
		if balances, ok := bank["balances"].([]map[string]interface{}); ok {
			for _, bal := range balances {
				convertField(bal, "address")
			}
		}
	}
	if staking, ok := appState["staking"].(map[string]interface{}); ok {
		if validators, ok := staking["validators"].([]map[string]interface{}); ok {
			for _, val := range validators {
				convertField(val, "operator_address")
			}
		}
		if delegations, ok := staking["delegations"].([]map[string]interface{}); ok {
			for _, del := range delegations {
				convertField(del, "delegator_address")
				convertField(del, "validator_address")
			}
		}
	}
}

// Add this function to manage snapshot directories
func ensureSnapshotDir(height int64) (string, error) {
	dirPath := fmt.Sprintf("snapshots/height_%d", height)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return "", fmt.Errorf("failed to create snapshot directory: %v", err)
	}
	logger.Printf("Using snapshot directory: %s", dirPath)
	return dirPath, nil
}

// Modify writeJSONFile to use snapshot directory
func writeJSONFile(filename string, data interface{}, snapshotDir string) error {
	fullPath := filepath.Join(snapshotDir, filename)

	// For accounts, write directly
	if filename == "accounts.json" {
		sortedJSON, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal failed: %v", err)
		}
		if err := os.WriteFile(fullPath, sortedJSON, 0644); err != nil {
			return fmt.Errorf("write failed: %v", err)
		}
		logger.Printf("Wrote %s", fullPath)
		return nil
	}

	// For other files, use temp file
	tempFile := fullPath + ".tmp"
	sortedJSON, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal failed: %v", err)
	}
	if err := os.WriteFile(tempFile, sortedJSON, 0644); err != nil {
		return fmt.Errorf("write failed: %v", err)
	}
	logger.Printf("Wrote temporary file %s", tempFile)
	return nil
}

// Add this function to fetch specific account numbers
func fetchMissingAccounts(missingNums []int64, height int64) ([]map[string]interface{}, error) {
	logger.Printf("Fetching %d missing accounts...", len(missingNums))
	var accounts []map[string]interface{}

	// Create channels for parallel processing
	workers := 50
	jobs := make(chan int64, workers*2)
	results := make(chan map[string]interface{}, workers*2)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for num := range jobs {
				url := fmt.Sprintf("https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts/%d", num)
				resp, err := httpClient.Get(url)
				if err != nil {
					logger.Printf("Failed to fetch account %d: %v", num, err)
					continue
				}

				var result struct {
					Account map[string]interface{} `json:"account"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					resp.Body.Close()
					logger.Printf("Failed to decode account %d: %v", num, err)
					continue
				}
				resp.Body.Close()

				if result.Account != nil {
					results <- result.Account
				}
			}
		}()
	}

	// Send jobs
	go func() {
		for _, num := range missingNums {
			jobs <- num
		}
		close(jobs)
	}()

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	for account := range results {
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// Modify processAccounts to be more resilient
func processAccounts(accounts []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(accounts) == 0 {
		return nil, fmt.Errorf("no accounts to process")
	}

	// Convert to a map keyed by account number
	accountMap := make(map[int64]map[string]interface{})
	var maxAccNum int64 = -1

	// First pass: collect valid accounts and find max account number
	for _, acc := range accounts {
		numStr, ok := acc["account_number"].(string)
		if !ok {
			logger.Printf("Warning: account missing account_number: %v", acc)
			continue
		}
		num, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			logger.Printf("Warning: invalid account_number: %s", numStr)
			continue
		}
		accountMap[num] = acc
		if num > maxAccNum {
			maxAccNum = num
		}
	}

	logger.Printf("Found %d valid accounts, highest account number: %d", len(accountMap), maxAccNum)

	// Find missing account numbers
	var missingNums []int64
	for i := int64(0); i <= maxAccNum; i++ {
		if _, exists := accountMap[i]; !exists {
			missingNums = append(missingNums, i)
		}
	}

	// Try to fetch missing accounts in batches
	if len(missingNums) > 0 {
		logger.Printf("Found %d missing accounts, attempting to fetch them...", len(missingNums))

		// Split missing numbers into batches of 1000
		batchSize := 1000
		for i := 0; i < len(missingNums); i += batchSize {
			end := i + batchSize
			if end > len(missingNums) {
				end = len(missingNums)
			}

			batch := missingNums[i:end]
			logger.Printf("Fetching batch %d-%d of missing accounts...", i, end-1)

			missingAccounts, err := fetchMissingAccounts(batch, 0)
			if err != nil {
				logger.Printf("Warning: failed to fetch batch of missing accounts: %v", err)
				continue
			}

			// Add successfully fetched accounts to the map
			for _, acc := range missingAccounts {
				if numStr, ok := acc["account_number"].(string); ok {
					if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
						accountMap[num] = acc
						logger.Printf("Retrieved missing account %d", num)
					}
				}
			}
		}
	}

	// Create final sorted slice, skipping any still-missing accounts
	var sorted []map[string]interface{}
	var stillMissing []int64
	for i := int64(0); i <= maxAccNum; i++ {
		if acc, exists := accountMap[i]; exists {
			sorted = append(sorted, acc)
		} else {
			stillMissing = append(stillMissing, i)
		}
	}

	if len(stillMissing) > 0 {
		logger.Printf("Warning: %d accounts still missing after fetching: %v",
			len(stillMissing), stillMissing[:min(10, len(stillMissing))])
		if len(stillMissing) > 10 {
			logger.Printf("... and %d more", len(stillMissing)-10)
		}
	}

	logger.Printf("Final account count: %d", len(sorted))
	return sorted, nil
}

// Helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Update the shouldUpdateBalances function to be more permissive
func shouldUpdateBalances(oldHeight, newHeight int64) bool {
	// Always update balances if they don't exist
	if _, err := os.Stat("balances.json"); os.IsNotExist(err) {
		logger.Println("No existing balances.json, will fetch balances")
		return true
	}

	// Update if height changed
	if oldHeight != newHeight {
		logger.Printf("Block height changed from %d to %d, will update balances",
			oldHeight, newHeight)
		return true
	}

	return false
}

// Modify updateBalances to track missing balances
func updateBalances(accounts []map[string]interface{}, height int64, snapshotDir string) ([]map[string]interface{}, error) {
	logger.Printf("Starting balance update for %d accounts at height %d", len(accounts), height)

	// Try to load existing balances first
	var balances []map[string]interface{}
	processedAddrs := make(map[string]bool)

	// Check all possible balance files in order of preference
	possibleFiles := []string{
		filepath.Join(snapshotDir, "balances.json.tmp.tmp"),
		filepath.Join(snapshotDir, "balances.json.tmp"),
		filepath.Join(snapshotDir, "balances.json"),
	}

	var loadedFile string
	for _, file := range possibleFiles {
		if data, err := os.ReadFile(file); err == nil {
			if err := json.Unmarshal(data, &balances); err == nil {
				loadedFile = file
				logger.Printf("Successfully loaded %d balances from %s", len(balances), file)
				// Track which addresses we've already processed
				for _, bal := range balances {
					if addr, ok := bal["address"].(string); ok {
						processedAddrs[addr] = true
					}
				}
				break
			} else {
				logger.Printf("Warning: Failed to parse %s: %v", file, err)
			}
		}
	}

	if loadedFile != "" {
		logger.Printf("Resuming from %d existing balances in %s", len(balances), loadedFile)
	}

	// Create a list of addresses that still need processing
	var remainingAddrs []string
	for _, acc := range accounts {
		if addr, ok := acc["address"].(string); ok {
			if !processedAddrs[addr] {
				remainingAddrs = append(remainingAddrs, addr)
			}
		}
	}
	logger.Printf("Found %d addresses remaining to process out of %d total", len(remainingAddrs), len(accounts))

	// Count remaining work
	remainingAddrsCount := len(remainingAddrs)
	logger.Printf("Found %d addresses remaining to process", remainingAddrsCount)

	// Create buffered channels
	workers := 100
	bufferSize := len(accounts)
	jobs := make(chan string, bufferSize)
	results := make(chan map[string]interface{}, bufferSize)

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for addr := range jobs {
				// Skip already processed addresses
				if processedAddrs[addr] {
					continue
				}
				url := fmt.Sprintf("https://rest.unicorn.meme/cosmos/bank/v1beta1/balances/%s", addr)
				resp, err := httpClient.Get(url)
				if err != nil {
					logger.Printf("Worker %d: Failed to get balance for %s: %v", workerID, addr, err)
					continue
				}

				var balanceResp struct {
					Balances []map[string]interface{} `json:"balances"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&balanceResp); err != nil {
					resp.Body.Close()
					continue
				}
				resp.Body.Close()

				results <- map[string]interface{}{
					"address": addr,
					"coins":   balanceResp.Balances,
				}
			}
		}(w)
	}

	// Send jobs
	go func() {
		for _, addr := range remainingAddrs {
			jobs <- addr
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	// Collect results with periodic saving
	lastSave := time.Now()
	processed := 0
	for result := range results {
		balances = append(balances, result)
		processed++

		// Save progress every minute or every 1000 balances
		if time.Since(lastSave) > time.Minute || processed%1000 == 0 {
			logger.Printf("Saving progress: %d/%d balances...", len(balances), len(accounts))
			if err := writeJSONFile("balances.json.tmp", balances, snapshotDir); err != nil {
				logger.Printf("Warning: Failed to save balance progress: %v", err)
			}
			lastSave = time.Now()
		}
	}

	// Final save
	if err := writeJSONFile("balances.json", balances, snapshotDir); err != nil {
		return nil, fmt.Errorf("failed to write final balances: %v", err)
	}

	logger.Printf("Balance update complete: %d total balances", len(balances))
	return balances, nil
}

// Update generateRichlist to use total balances
func generateRichlist(balances []TotalBalance, snapshotDir string) error {
	logger.Println("Generating richlist...")

	// Sort by total amount descending
	sort.Slice(balances, func(i, j int) bool {
		iTotal, _ := strconv.ParseInt(balances[i].Total, 10, 64)
		jTotal, _ := strconv.ParseInt(balances[j].Total, 10, 64)
		return iTotal > jTotal
	})

	// Add ranking
	type RichlistEntry struct {
		Rank          int    `json:"rank"`
		Address       string `json:"address"`
		LiquidAmount  string `json:"liquid_amount"`
		StakedAmount  string `json:"staked_amount"`
		TotalAmount   string `json:"total_amount"`
		LiquidUnicorn string `json:"liquid_unicorn"`
		StakedUnicorn string `json:"staked_unicorn"`
		TotalUnicorn  string `json:"total_amount"`
	}

	var richlist []RichlistEntry
	for i, balance := range balances {
		richlist = append(richlist, RichlistEntry{
			Rank:          i + 1,
			Address:       balance.Address,
			LiquidAmount:  balance.Liquid,
			StakedAmount:  balance.Staked,
			TotalAmount:   balance.Total,
			LiquidUnicorn: balance.LiquidUnicorn,
			StakedUnicorn: balance.StakedUnicorn,
			TotalUnicorn:  balance.TotalUnicorn,
		})
	}

	// Write richlist to file
	if err := writeJSONFile("richlist.json", richlist, snapshotDir); err != nil {
		return fmt.Errorf("failed to write richlist: %v", err)
	}

	logger.Printf("Generated richlist with %d entries", len(richlist))
	if len(richlist) > 0 {
		logger.Println("Top 10 total balances:")
		max := 10
		if len(richlist) < max {
			max = len(richlist)
		}
		for i := 0; i < max; i++ {
			logger.Printf("#%d: %s - %s UNICORN (Liquid: %s, Staked: %s)",
				richlist[i].Rank,
				richlist[i].Address,
				richlist[i].TotalUnicorn,
				richlist[i].LiquidUnicorn,
				richlist[i].StakedUnicorn)
		}
	}

	return nil
}

// Add this function to calculate total supply
func calculateSupply(balances []map[string]interface{}) (int64, float64) {
	var totalUwunicorn int64
	for _, bal := range balances {
		if coins, ok := bal["coins"].([]map[string]interface{}); ok {
			for _, coin := range coins {
				if denom, ok := coin["denom"].(string); ok && denom == "uwunicorn" {
					if amount, ok := coin["amount"].(string); ok {
						if amt, err := strconv.ParseInt(amount, 10, 64); err == nil {
							totalUwunicorn += amt
						}
					}
				}
			}
		}
	}
	totalUnicorn := float64(totalUwunicorn) / 1000000.0
	return totalUwunicorn, totalUnicorn
}

// Update the updateReadme function
func updateReadme(height int64, snapshotDir string, balances []map[string]interface{}) error {
	const readmeFile = "README.md"
	var content string

	// Read existing README if it exists
	if data, err := os.ReadFile(readmeFile); err == nil {
		content = string(data)
	}

	// Calculate supply
	totalUwunicorn, totalUnicorn := calculateSupply(balances)

	// Add new snapshot entry with supply information
	timestamp := time.Now().UTC().Format(time.RFC3339)
	newEntry := fmt.Sprintf(`
## Snapshot %d

- Height: %d
- Time: %s
- Path: %s
- Total Supply: %d uwunicorn (%.6f UNICORN)
`,
		height, height, timestamp, snapshotDir, totalUwunicorn, totalUnicorn)

	if !strings.Contains(content, fmt.Sprintf("## Snapshot %d", height)) {
		content = content + newEntry
		if err := os.WriteFile(readmeFile, []byte(content), 0644); err != nil {
			return fmt.Errorf("failed to update README: %v", err)
		}
		logger.Printf("Updated README.md with snapshot %d (Total Supply: %.6f UNICORN)", height, totalUnicorn)
	}

	return nil
}

// Add this function to check if a snapshot is complete
func isSnapshotComplete(snapshotDir string) bool {
	requiredFiles := []string{
		"accounts.json",
		"balances.json",
		"validators.json",
		"state.json",
	}

	for _, file := range requiredFiles {
		if _, err := os.Stat(filepath.Join(snapshotDir, file)); os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// Update main() to handle incomplete snapshots
func main() {
	logger.Println("=== Starting Unicorn Snapshot ===")
	logger.Printf("Block height check...")

	// Initialize variables
	data := make(map[string][]map[string]interface{})
	var accounts []map[string]interface{}
	var snapshotDir string
	var err error

	var newPrefix string
	flag.StringVar(&newPrefix, "prefix", "unicorn", "New Bech32 prefix (default: unicorn)")
	var force bool
	flag.BoolVar(&force, "force", false, "Force re-collection of data even if height is complete")
	flag.Parse()
	logger.Printf("Using Bech32 prefix: %s", newPrefix)

	// Get latest block height first
	latestHeight, err := getLatestBlockHeight()
	if err != nil {
		logger.Fatalf("Get block height failed: %v", err)
	}

	// Check for incomplete snapshots first
	if incompleteDir, incompleteHeight, err := findIncompleteSnapshot(); err != nil {
		logger.Printf("Warning: Error checking for incomplete snapshots: %v", err)
	} else if incompleteDir != "" {
		logger.Printf("Found incomplete snapshot at height %d, completing it first", incompleteHeight)
		snapshotDir = incompleteDir
		latestHeight = incompleteHeight // Use the incomplete snapshot's height
	} else {
		// Only create new snapshot if no incomplete ones exist
		snapshotDir, err = ensureSnapshotDir(latestHeight)
		if err != nil {
			logger.Fatalf("Failed to create snapshot directory: %v", err)
		}
	}

	// Load state from snapshot directory
	state, err := loadState(snapshotDir)
	if err != nil {
		logger.Fatalf("Load state failed: %v", err)
	}

	// Use the height from the incomplete snapshot
	if state.BlockHeight > 0 {
		latestHeight = state.BlockHeight
		logger.Printf("Using existing snapshot height: %d", latestHeight)
	}

	// Load root accounts first
	rootAccounts, err := loadRootAccounts()
	if err != nil {
		logger.Printf("Warning: Failed to load root accounts: %v", err)
	}

	// Use root accounts and fetch only new ones
	if rootAccounts != nil {
		accounts = rootAccounts
		missingNums, err := findMissingAndNewAccounts(accounts)
		if err != nil {
			logger.Printf("Warning: Error checking for missing accounts: %v", err)
		} else if len(missingNums) > 0 {
			logger.Printf("Fetching %d new accounts...", len(missingNums))
			newAccounts, err := fetchMissingAccounts(missingNums, latestHeight)
			if err != nil {
				logger.Printf("Warning: Failed to fetch new accounts: %v", err)
			} else {
				accounts = append(accounts, newAccounts...)
				if err := updateRootAccounts(accounts); err != nil {
					logger.Printf("Warning: %v", err)
				}
			}
		}
	} else {
		// No root accounts.json, fetch everything
		accounts, err = fetchAccountsParallel()
		if err != nil {
			logger.Fatalf("Failed to fetch accounts: %v", err)
		}
		if err := updateRootAccounts(accounts); err != nil {
			logger.Printf("Warning: %v", err)
		}
	}

	// Process accounts
	if accounts, err = processAccounts(accounts); err != nil {
		logger.Fatalf("Failed to process accounts: %v", err)
	}
	data["accounts"] = accounts

	// Update balances if needed
	if shouldUpdateBalances(state.BlockHeight, latestHeight) {
		data["balances"], err = updateBalances(accounts, latestHeight, snapshotDir)
		if err != nil {
			logger.Fatalf("Failed to update balances: %v", err)
		}
	} else {
		// Load existing balances
		var balances []map[string]interface{}
		balancesFile := filepath.Join(snapshotDir, "balances.json")
		if balanceData, err := os.ReadFile(balancesFile); err == nil {
			if err := json.Unmarshal(balanceData, &balances); err != nil {
				logger.Fatalf("Failed to parse balances.json: %v", err)
			}
			data["balances"] = balances
			logger.Printf("Using existing balances from height %d", state.BlockHeight)
		}
	}

	// Generate richlist
	if len(data["balances"]) > 0 {
		logger.Println("=== Generating Richlist ===")
		var totalBalances []TotalBalance
		balances := data["balances"]
		for _, bal := range balances {
			addr, ok := bal["address"].(string)
			if !ok {
				continue
			}
			coinsRaw, ok := bal["coins"].([]interface{})
			if !ok {
				continue
			}

			var liquidAmount, stakedAmount int64
			for _, c := range coinsRaw {
				coin, ok := c.(map[string]interface{})
				if !ok {
					continue
				}
				if denom, ok := coin["denom"].(string); ok && denom == "uwunicorn" {
					if amount, ok := coin["amount"].(string); ok {
						liquidAmount, _ = strconv.ParseInt(amount, 10, 64)
					}
				}
			}

			totalAmount := liquidAmount + stakedAmount
			if liquidAmount > 0 || stakedAmount > 0 {
				totalBalances = append(totalBalances, TotalBalance{
					Address:       addr,
					Liquid:        strconv.FormatInt(liquidAmount, 10),
					Staked:        strconv.FormatInt(stakedAmount, 10),
					Total:         strconv.FormatInt(totalAmount, 10),
					LiquidUnicorn: strconv.FormatFloat(float64(liquidAmount)/1000000, 'f', 6, 64),
					StakedUnicorn: strconv.FormatFloat(float64(stakedAmount)/1000000, 'f', 6, 64),
					TotalUnicorn:  strconv.FormatFloat(float64(totalAmount)/1000000, 'f', 6, 64),
				})
			}
		}

		if err := generateRichlist(totalBalances, snapshotDir); err != nil {
			logger.Printf("Warning: Failed to generate richlist: %v", err)
		}
	}

	logger.Println("=== Fetching Validators ===")
	data["validators"], err = fetchValidators(latestHeight, snapshotDir)
	if err != nil {
		logger.Printf("Warning: validator fetch failed: %v", err)
	}
	logger.Printf("Found %d validators", len(data["validators"]))

	logger.Println("=== Fetching Delegations ===")
	data["delegations"], err = fetchRPC("/cosmos.staking.v1beta1/delegations", "delegations.json.tmp", latestHeight, snapshotDir)
	if err != nil {
		logger.Printf("Warning: delegations fetch failed: %v", err)
	}
	logger.Printf("Found %d delegations", len(data["delegations"]))

	logger.Println("=== Fetching Denom Metadata ===")
	data["denom_metadata"], err = fetchRPC("/cosmos.bank.v1beta1/denoms_metadata", "metadatas.json.tmp", latestHeight, snapshotDir)
	if err != nil {
		logger.Printf("Warning: metadata fetch failed: %v", err)
	}
	logger.Printf("Found %d denom metadata entries", len(data["denom_metadata"]))

	// Fetch parameters
	logger.Println("=== Fetching Module Parameters ===")
	params := make(map[string]map[string]interface{})
	for _, module := range []string{"auth", "bank", "staking", "distribution", "gov"} {
		logger.Printf("Fetching %s parameters...", module)
		params[module], err = fetchParamsRPC(module, latestHeight, snapshotDir)
		if err != nil {
			logger.Printf("Warning: %s params fetch failed: %v", module, err)
		}
	}

	// Construct genesis
	logger.Println("=== Constructing Genesis File ===")
	logger.Printf("Creating genesis state at height %d", latestHeight)
	genesis := map[string]interface{}{
		"genesis_time":   time.Now().UTC().Format(time.RFC3339),
		"chain_id":       "unicorn-snapshot",
		"initial_height": "1",
		"app_state": map[string]interface{}{
			"auth": map[string]interface{}{
				"params":   params["auth"],
				"accounts": data["accounts"],
			},
			"bank": map[string]interface{}{
				"params":         params["bank"],
				"balances":       data["balances"],
				"denom_metadata": data["denom_metadata"],
			},
			"staking": map[string]interface{}{
				"params":      params["staking"],
				"validators":  data["validators"],
				"delegations": data["delegations"],
			},
			"distribution": map[string]interface{}{"params": params["distribution"]},
			"gov":          map[string]interface{}{"params": params["gov"]},
		},
	}

	// Convert Bech32 prefix
	oldPrefix := "cosmos"
	if len(data["accounts"]) > 0 {
		if addr, ok := data["accounts"][0]["address"].(string); ok {
			if hrp, _, err := bech32.Decode(addr); err == nil {
				oldPrefix = hrp
			}
		}
	}
	logger.Println("=== Converting Addresses ===")
	logger.Printf("Converting from prefix '%s' to '%s'", oldPrefix, newPrefix)
	convertAddresses(genesis["app_state"].(map[string]interface{}), oldPrefix, newPrefix)

	// Write genesis
	logger.Println("=== Writing Output Files ===")
	for _, key := range []string{"accounts", "balances", "validators", "delegations", "metadatas"} {
		if data[key] != nil {
			logger.Printf("Writing %s.json (%d entries)...", key, len(data[key]))
			if err := writeJSONFile(key+".json", data[key], snapshotDir); err != nil {
				logger.Printf("Warning: write %s failed: %v", key, err)
			}
		}
	}

	logger.Println("Writing parameter files...")
	for module, moduleParams := range params {
		logger.Printf("Writing %s_params.json...", module)
		if err := writeJSONFile(module+"_params.json", moduleParams, snapshotDir); err != nil {
			logger.Printf("Warning: write %s params failed: %v", module, err)
		}
	}

	// After fetching balances, save state again
	if err := saveState(state, true, snapshotDir); err != nil {
		logger.Printf("Failed to save final state: %v", err)
	}

	// Clean up at the end
	defer cleanupAllTempFiles(snapshotDir)

	logger.Println("=== Snapshot Complete ===")
	logger.Printf("Final block height: %d", latestHeight)
	logger.Printf("Total accounts: %d", len(data["accounts"]))
	logger.Printf("Total balances: %d", len(data["balances"]))
	logger.Printf("Total validators: %d", len(data["validators"]))
	logger.Printf("Total delegations: %d", len(data["delegations"]))
	logger.Printf("Total metadata entries: %d", len(data["denom_metadata"]))
	logger.Println("Snapshot completed!")
	fmt.Println("Snapshot completed!")

	// Update README after successful snapshot
	if err := updateReadme(latestHeight, snapshotDir, data["balances"]); err != nil {
		logger.Printf("Warning: %v", err)
	}
}

// Add this function that was referenced but missing
func fetchAccountsParallel() ([]map[string]interface{}, error) {
	logger.Println("Starting parallel account fetch...")

	// Create channels for parallel processing
	workers := 200
	jobs := make(chan int64, workers*2)
	results := make(chan map[string]interface{}, workers*2)

	// Start workers
	var wg sync.WaitGroup
	logger.Printf("Starting %d worker goroutines...", workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for page := range jobs {
				url := fmt.Sprintf("https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts?pagination.limit=1000&pagination.offset=%d000", page)
				resp, err := httpClient.Get(url)
				if err != nil {
					logger.Printf("Worker %d: Failed to fetch page %d: %v", workerID, page, err)
					continue
				}

				var result struct {
					Accounts []map[string]interface{} `json:"accounts"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					resp.Body.Close()
					logger.Printf("Worker %d: Failed to decode page %d: %v", workerID, page, err)
					continue
				}
				resp.Body.Close()

				for _, account := range result.Accounts {
					results <- account
				}
			}
		}(w)
	}

	// Send jobs
	go func() {
		for p := int64(0); p < 1000; p++ { // Adjust range as needed
			jobs <- p
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	// Collect results
	var accounts []map[string]interface{}
	for account := range results {
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// Add this function that was referenced but missing
func findMissingAndNewAccounts(existingAccounts []map[string]interface{}) ([]int64, error) {
	logger.Println("Analyzing account gaps and checking for new accounts...")

	// Create map of existing account numbers
	accountMap := make(map[int64]bool)
	var maxExisting int64 = -1

	for _, acc := range existingAccounts {
		if numStr, ok := acc["account_number"].(string); ok {
			if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
				accountMap[num] = true
				if num > maxExisting {
					maxExisting = num
				}
			}
		}
	}

	// Check for new accounts beyond our highest
	currentLast, err := checkForNewAccounts(maxExisting)
	if err != nil {
		logger.Printf("Warning: Failed to check for new accounts: %v", err)
		currentLast = maxExisting
	}

	// Find all missing numbers from 0 to currentLast
	var missingNums []int64
	for i := int64(0); i <= currentLast; i++ {
		if !accountMap[i] {
			missingNums = append(missingNums, i)
		}
	}

	return missingNums, nil
}

// Add this function that was missing
func checkForNewAccounts(lastKnownNum int64) (int64, error) {
	logger.Println("Checking for new accounts...")
	testURL := "https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts?pagination.limit=1&pagination.offset=999999"
	resp, err := httpClient.Get(testURL)
	if err != nil {
		return lastKnownNum, fmt.Errorf("failed to check for new accounts: %v", err)
	}

	var testResp struct {
		Accounts []map[string]interface{} `json:"accounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&testResp); err != nil {
		resp.Body.Close()
		return lastKnownNum, fmt.Errorf("failed to decode response: %v", err)
	}
	resp.Body.Close()

	if len(testResp.Accounts) > 0 {
		if numStr, ok := testResp.Accounts[0]["account_number"].(string); ok {
			if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
				if num > lastKnownNum {
					logger.Printf("Found new accounts: last known %d, current last %d", lastKnownNum, num)
					return num, nil
				}
			}
		}
	}

	return lastKnownNum, nil
}

// Update cleanupAllTempFiles to work in snapshot directory
func cleanupAllTempFiles(snapshotDir string) {
	logger.Printf("Cleaning up temporary files in %s", snapshotDir)
	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		logger.Printf("Warning: Failed to read directory for cleanup: %v", err)
		return
	}

	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".tmp") {
			fullPath := filepath.Join(snapshotDir, file.Name())
			if err := os.Remove(fullPath); err != nil {
				logger.Printf("Warning: Failed to remove %s: %v", fullPath, err)
			} else {
				logger.Printf("Removed temporary file: %s", fullPath)
			}
		}
	}
}

// Add this function to manage root accounts file
func loadRootAccounts() ([]map[string]interface{}, error) {
	if data, err := os.ReadFile("accounts.json"); err == nil {
		var accounts []map[string]interface{}
		if err := json.Unmarshal(data, &accounts); err != nil {
			return nil, fmt.Errorf("failed to parse root accounts.json: %v", err)
		}
		logger.Printf("Loaded %d accounts from root accounts.json", len(accounts))
		return accounts, nil
	}
	return nil, nil
}

// Add this function to update root accounts
func updateRootAccounts(accounts []map[string]interface{}) error {
	if err := writeJSONFile("accounts.json", accounts, "."); err != nil {
		return fmt.Errorf("failed to update root accounts.json: %v", err)
	}
	logger.Printf("Updated root accounts.json with %d accounts", len(accounts))
	return nil
}

// Update findIncompleteSnapshot to check completion status
func findIncompleteSnapshot() (string, int64, error) {
	entries, err := os.ReadDir("snapshots")
	if err != nil && !os.IsNotExist(err) {
		return "", 0, fmt.Errorf("failed to read snapshots directory: %v", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "height_") {
			continue
		}

		heightStr := strings.TrimPrefix(entry.Name(), "height_")
		height, err := strconv.ParseInt(heightStr, 10, 64)
		if err != nil {
			continue
		}

		snapshotDir := filepath.Join("snapshots", entry.Name())
		if !isSnapshotComplete(snapshotDir) {
			logger.Printf("Found incomplete snapshot at height %d", height)
			return snapshotDir, height, nil
		}
	}

	return "", 0, nil
}
