package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
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
	BlockHeight int64 `json:"block_height"`
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

// loadState loads or initializes state
func loadState() (*State, error) {
	logger.Println("Loading state from state.json.tmp...")
	if _, err := os.Stat("state.json.tmp"); os.IsNotExist(err) {
		logger.Println("Initializing new state.")
		return &State{}, nil
	}
	data, err := os.ReadFile("state.json.tmp")
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
func saveState(state *State, final bool) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}
	filename := "state.json.tmp"
	if final {
		filename = "state.json"
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return err
	}
	logger.Printf("State saved to %s", filename)
	return nil
}

// rpcQuery sends an RPC request
func rpcQuery(method string, params map[string]interface{}) (map[string]interface{}, error) {
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
		return nil, fmt.Errorf("RPC request: %v", err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %v", err)
	}
	if errData, ok := result["error"]; ok {
		return nil, fmt.Errorf("RPC error: %v", errData)
	}
	return result["result"].(map[string]interface{}), nil
}

// fetchRPC fetches data via RPC with pagination
func fetchRPC(path, cacheFile string, height int64) ([]map[string]interface{}, error) {
	if data, err := os.ReadFile(cacheFile); err == nil {
		var items []map[string]interface{}
		if err := json.Unmarshal(data, &items); err == nil {
			logger.Printf("Loaded %d items from %s", len(items), cacheFile)
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
				if err := os.WriteFile("accounts.json.tmp", tempBytes, 0644); err != nil {
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
	if err := os.WriteFile(cacheFile, dataBytes, 0644); err != nil {
		logger.Printf("Failed to cache to %s: %v", cacheFile, err)
	}
	logger.Printf("Fetched total %d items via RPC for %s", len(allItems), path)
	return allItems, nil
}

// fetchValidators fetches validators via RPC
func fetchValidators(height int64) ([]map[string]interface{}, error) {
	cacheFile := "validators.json.tmp"
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
func fetchParamsRPC(module string, height int64) (map[string]interface{}, error) {
	cacheFile := fmt.Sprintf("%s_params.json.tmp", module)
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

// getLatestBlockHeight fetches the latest block height
func getLatestBlockHeight() (int64, error) {
	rpcParams, err := rpcQuery("block", map[string]interface{}{})
	if err != nil {
		return 0, err
	}
	block := rpcParams["block"].(map[string]interface{})
	header := block["header"].(map[string]interface{})
	heightStr := header["height"].(string)
	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		return 0, err
	}
	logger.Printf("Latest block height: %d", height)
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

// writeJSONFile writes JSON data with sorted keys and consistent formatting
func writeJSONFile(filename string, data interface{}) error {
	// First marshal to get a map structure
	rawJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("initial marshal failed: %v", err)
	}

	// Unmarshal into interface{} to get a generic structure
	var v interface{}
	if err := json.Unmarshal(rawJSON, &v); err != nil {
		return fmt.Errorf("unmarshal failed: %v", err)
	}

	// Marshal again with sorted keys and indentation
	sortedJSON, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("final marshal failed: %v", err)
	}

	// Write to temp file first
	tempFile := filename + ".tmp"
	if err := os.WriteFile(tempFile, sortedJSON, 0644); err != nil {
		return fmt.Errorf("write failed: %v", err)
	}

	// Rename temp file to final file
	if err := os.Rename(tempFile, filename); err != nil {
		return fmt.Errorf("rename failed: %v", err)
	}

	logger.Printf("Wrote %s", filename)
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

// Update the updateBalances function to ensure it always writes the file
func updateBalances(accounts []map[string]interface{}, height int64) ([]map[string]interface{}, error) {
	logger.Printf("Updating balances for %d accounts at height %d", len(accounts), height)

	// Create channels for parallel processing
	workers := 50
	jobs := make(chan string, workers*2)
	results := make(chan map[string]interface{}, workers*2)

	// Start workers
	var wg sync.WaitGroup
	logger.Printf("Starting %d balance fetch workers...", workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for addr := range jobs {
				url := fmt.Sprintf("https://rest.unicorn.meme/cosmos/bank/v1beta1/balances/%s", addr)
				resp, err := fetchWithBackoff(url, 3)
				if err != nil {
					logger.Printf("Worker %d: Failed to get balance for %s: %v", workerID, addr, err)
					continue
				}

				var balanceResp struct {
					Balances []map[string]interface{} `json:"balances"`
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					logger.Printf("Worker %d: Failed to read balance for %s: %v", workerID, addr, err)
					continue
				}

				if err := json.Unmarshal(body, &balanceResp); err != nil {
					logger.Printf("Worker %d: Failed to parse balance for %s: %v", workerID, addr, err)
					continue
				}

				// Only send non-zero balances
				if len(balanceResp.Balances) > 0 {
					// Log interesting balances (only worker 0, and only if significant)
					if workerID == 0 {
						for _, bal := range balanceResp.Balances {
							if amount, ok := bal["amount"].(string); ok {
								if amt, err := strconv.ParseInt(amount, 10, 64); err == nil {
									if amt > 1000000 { // More than 1M uwunicorn
										logger.Printf("Found large balance: %s has %s %s",
											addr, amount, bal["denom"])
									}
								}
							}
						}
					}

					results <- map[string]interface{}{
						"address": addr,
						"coins":   balanceResp.Balances,
					}
				}
			}
			logger.Printf("Worker %d: Finished processing balances", workerID)
		}(w)
	}

	// Send jobs
	go func() {
		logger.Printf("Queueing %d addresses for balance fetch...", len(accounts))
		for _, acc := range accounts {
			if addr, ok := acc["address"].(string); ok {
				jobs <- addr
			}
		}
		close(jobs)

		// Wait for all workers to finish
		wg.Wait()
		close(results)
		logger.Println("All balance workers completed")
	}()

	// Collect results
	var balances []map[string]interface{}
	processed := 0
	lastSave := time.Now()

	logger.Println("Starting to collect balance results...")
	for balance := range results {
		balances = append(balances, balance)
		processed++

		// Save progress every minute
		if time.Since(lastSave) > time.Minute {
			logger.Printf("Saving intermediate balances (%d so far)...", len(balances))
			if err := writeJSONFile("balances.json", balances); err != nil {
				logger.Printf("Warning: Failed to save balance progress: %v", err)
			}
			lastSave = time.Now()
		}

		if processed%1000 == 0 {
			logger.Printf("Processed %d/%d balances (%.2f%%)",
				processed, len(accounts), float64(processed)/float64(len(accounts))*100)
		}
	}

	// Final save
	if err := writeJSONFile("balances.json", balances); err != nil {
		return nil, fmt.Errorf("failed to write final balances: %v", err)
	}

	logger.Printf("Completed balance update: %d total balances", len(balances))
	return balances, nil
}

// Add this function
func validateAccounts(filename string) (bool, int64) {
	logger.Printf("Validating %s...", filename)
	data, err := os.ReadFile(filename)
	if err != nil {
		logger.Printf("Could not read %s: %v", filename, err)
		return false, -1
	}

	var accounts []map[string]interface{}
	if err := json.Unmarshal(data, &accounts); err != nil {
		logger.Printf("Could not parse %s: %v", filename, err)
		return false, -1
	}

	if len(accounts) == 0 {
		logger.Printf("No accounts found in %s", filename)
		return false, -1
	}

	// Check account numbers are sequential
	lastNum := int64(-1)
	for i, acc := range accounts {
		numStr, ok := acc["account_number"].(string)
		if !ok {
			logger.Printf("Account %d missing account_number", i)
			return false, -1
		}
		num, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			logger.Printf("Invalid account_number at index %d: %s", i, numStr)
			return false, -1
		}
		if lastNum != -1 && num != lastNum+1 {
			logger.Printf("Non-sequential account numbers at index %d: %d -> %d", i, lastNum, num)
			return false, -1
		}
		lastNum = num
	}

	logger.Printf("Validated %d accounts, last account number: %d", len(accounts), lastNum)
	return true, lastNum
}

// Add this function for exponential backoff
func fetchWithBackoff(url string, maxAttempts int) (*http.Response, error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			logger.Printf("Backing off for %v before retry %d", backoff, attempt+1)
			time.Sleep(backoff)
		}

		resp, err := httpClient.Get(url)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		logger.Printf("Request failed (attempt %d/%d): %v", attempt+1, maxAttempts, err)
	}
	return nil, fmt.Errorf("all attempts failed: %v", lastErr)
}

// Add this function to safely append accounts
func appendToAccountsFile(accounts []map[string]interface{}) error {
	// First read existing accounts
	var existingAccounts []map[string]interface{}
	if data, err := os.ReadFile("accounts.json"); err == nil {
		if err := json.Unmarshal(data, &existingAccounts); err == nil {
			logger.Printf("Read %d existing accounts", len(existingAccounts))
		}
	}

	// Create map of existing accounts by account number
	accountMap := make(map[string]bool)
	for _, acc := range existingAccounts {
		if numStr, ok := acc["account_number"].(string); ok {
			accountMap[numStr] = true
		}
	}

	// Add new accounts that don't exist yet
	added := 0
	for _, acc := range accounts {
		if numStr, ok := acc["account_number"].(string); ok {
			if !accountMap[numStr] {
				existingAccounts = append(existingAccounts, acc)
				accountMap[numStr] = true
				added++
			}
		}
	}

	if added > 0 {
		logger.Printf("Adding %d new accounts", added)
		// Write all accounts back to file
		if err := writeJSONFile("accounts.json", existingAccounts); err != nil {
			return fmt.Errorf("failed to write accounts: %v", err)
		}
		logger.Printf("Successfully wrote %d total accounts", len(existingAccounts))
	}

	return nil
}

// Add this function to handle parallel account fetching
func fetchAccountsParallel() ([]map[string]interface{}, error) {
	logger.Println("Starting parallel account fetch...")

	// Try to get first page to determine total
	logger.Println("Getting initial page to determine total...")
	url := "https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts?pagination.limit=1&pagination.count_total=true"
	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial page: %v", err)
	}

	var initialResp struct {
		Accounts   []map[string]interface{} `json:"accounts"`
		Pagination struct {
			Total string `json:"total"`
		} `json:"pagination"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&initialResp); err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("failed to decode initial response: %v", err)
	}
	resp.Body.Close()

	// If no total provided, try to estimate from highest account number
	var total int64
	if initialResp.Pagination.Total == "" {
		logger.Println("No total provided, fetching last account to estimate...")
		// Try a large offset to find the last account
		testURL := "https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts?pagination.limit=1&pagination.offset=999999"
		resp, err := httpClient.Get(testURL)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate total: %v", err)
		}
		var testResp struct {
			Accounts []map[string]interface{} `json:"accounts"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&testResp); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("failed to decode test response: %v", err)
		}
		resp.Body.Close()

		if len(testResp.Accounts) > 0 {
			if numStr, ok := testResp.Accounts[0]["account_number"].(string); ok {
				if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
					total = num + 1
					logger.Printf("Estimated total accounts: %d", total)
				}
			}
		}
	} else {
		total, err = strconv.ParseInt(initialResp.Pagination.Total, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid total: %v", err)
		}
		logger.Printf("Got total accounts from API: %d", total)
	}

	if total == 0 {
		return nil, fmt.Errorf("could not determine total number of accounts")
	}

	// Create worker pool
	workers := 100
	pageSize := 1000
	pages := (total + int64(pageSize) - 1) / int64(pageSize)
	logger.Printf("Using %d workers to fetch %d pages with %d accounts per page", workers, pages, pageSize)

	type pageResult struct {
		accounts []map[string]interface{}
		page     int64
		err      error
	}

	jobs := make(chan int64, workers*2)
	results := make(chan pageResult, workers*2)

	// Start workers
	var wg sync.WaitGroup
	logger.Printf("Starting %d worker goroutines...", workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for page := range jobs {
				offset := page * int64(pageSize)
				url := fmt.Sprintf("https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts?pagination.limit=%d&pagination.offset=%d",
					pageSize, offset)

				var result pageResult
				resp, err := fetchWithBackoff(url, 5)
				if err != nil {
					result.err = err
					result.page = page
					results <- result
					continue
				}

				var pageData struct {
					Accounts []map[string]interface{} `json:"accounts"`
				}

				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					result.err = fmt.Errorf("failed to read response: %v", err)
					result.page = page
					results <- result
					continue
				}

				if err := json.Unmarshal(body, &pageData); err != nil {
					result.err = fmt.Errorf("failed to decode response: %v", err)
					result.page = page
					results <- result
					continue
				}

				result.accounts = pageData.Accounts
				result.page = page
				result.err = nil
				results <- result

				// Save progress every 10 pages per worker
				if page%10 == 0 {
					logger.Printf("Worker %d completed page %d", workerID, page)
				}
			}
		}(w)
	}

	// Send jobs and wait for completion
	go func() {
		// Send all jobs
		for p := int64(0); p < pages; p++ {
			jobs <- p
		}
		close(jobs)

		// Wait for all workers to finish
		wg.Wait()

		// Close results channel
		close(results)
		logger.Println("All workers completed, results channel closed")
	}()

	// Collect results
	processed := int64(0)
	lastLog := time.Now()
	successCount := int64(0)
	errorCount := int64(0)

	logger.Println("Starting to collect results...")
	for result := range results {
		processed++
		if result.err != nil {
			errorCount++
			logger.Printf("Error fetching page %d: %v (errors: %d/%d)",
				result.page, result.err, errorCount, processed)
		} else {
			successCount++
			// Write accounts immediately
			if err := appendToAccountsFile(result.accounts); err != nil {
				logger.Printf("Warning: Failed to append accounts from page %d: %v",
					result.page, err)
			}
			logger.Printf("Processed page %d with %d accounts (success: %d/%d)",
				result.page, len(result.accounts), successCount, processed)
		}

		// Log progress every 5 seconds
		if time.Since(lastLog) > 5*time.Second {
			logger.Printf("Progress: %d/%d pages (%.2f%%), Success: %d, Errors: %d",
				processed, pages, float64(processed)/float64(pages)*100,
				successCount, errorCount)
			lastLog = time.Now()
		}

		if processed >= pages {
			break
		}
	}

	logger.Printf("Account fetch complete: %d/%d pages successful (%.2f%% success rate)",
		successCount, pages, float64(successCount)/float64(pages)*100)

	// Read the final accounts file to return
	var finalAccounts []map[string]interface{}
	if data, err := os.ReadFile("accounts.json"); err == nil {
		if err := json.Unmarshal(data, &finalAccounts); err != nil {
			return nil, fmt.Errorf("failed to read final accounts: %v", err)
		}
		logger.Printf("Returning %d total accounts", len(finalAccounts))
		return finalAccounts, nil
	}

	return nil, fmt.Errorf("failed to read final accounts file")
}

// Add this function to check for new accounts
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

	if len(missingNums) > 0 {
		logger.Printf("Found %d missing account numbers", len(missingNums))
		if len(missingNums) < 10 {
			logger.Printf("Missing accounts: %v", missingNums)
		} else {
			logger.Printf("First 10 missing accounts: %v...", missingNums[:10])
		}
	}

	return missingNums, nil
}

// Add this new function to generate the richlist
func generateRichlist(balances []map[string]interface{}) error {
	logger.Println("Generating richlist...")

	type AccountBalance struct {
		Address string `json:"address"`
		Amount  int64  `json:"amount"`
	}

	var richlist []AccountBalance

	// Convert balances to sorted list
	for _, bal := range balances {
		addr, ok := bal["address"].(string)
		if !ok {
			continue
		}

		coins, ok := bal["coins"].([]interface{})
		if !ok {
			continue
		}

		// Find uwunicorn balance
		for _, coin := range coins {
			coinMap, ok := coin.(map[string]interface{})
			if !ok {
				continue
			}

			if denom, ok := coinMap["denom"].(string); ok && denom == "uwunicorn" {
				if amountStr, ok := coinMap["amount"].(string); ok {
					amount, err := strconv.ParseInt(amountStr, 10, 64)
					if err != nil {
						continue
					}
					if amount > 0 {
						richlist = append(richlist, AccountBalance{
							Address: addr,
							Amount:  amount,
						})
					}
				}
			}
		}
	}

	// Sort by amount descending
	sort.Slice(richlist, func(i, j int) bool {
		return richlist[i].Amount > richlist[j].Amount
	})

	// Add ranking and format for output
	type RichlistEntry struct {
		Rank    int    `json:"rank"`
		Address string `json:"address"`
		Amount  string `json:"amount"`
		// Convert uwunicorn to UNICORN for readability
		Unicorn string `json:"unicorn"`
	}

	var formattedList []RichlistEntry
	for i, entry := range richlist {
		formattedList = append(formattedList, RichlistEntry{
			Rank:    i + 1,
			Address: entry.Address,
			Amount:  strconv.FormatInt(entry.Amount, 10),
			Unicorn: strconv.FormatFloat(float64(entry.Amount)/1000000, 'f', 6, 64),
		})
	}

	// Write richlist to file
	if err := writeJSONFile("richlist.json", formattedList); err != nil {
		return fmt.Errorf("failed to write richlist: %v", err)
	}

	logger.Printf("Generated richlist with %d entries", len(formattedList))
	// Log top 10 for visibility
	if len(formattedList) > 0 {
		logger.Println("Top 10 balances:")
		max := 10
		if len(formattedList) < max {
			max = len(formattedList)
		}
		for i := 0; i < max; i++ {
			logger.Printf("#%d: %s - %s UNICORN",
				formattedList[i].Rank,
				formattedList[i].Address,
				formattedList[i].Unicorn)
		}
	}

	return nil
}

func main() {
	logger.Println("=== Starting Unicorn Snapshot ===")
	logger.Printf("Block height check...")

	var newPrefix string
	flag.StringVar(&newPrefix, "prefix", "unicorn", "New Bech32 prefix (default: unicorn)")
	flag.Parse()
	logger.Printf("Using Bech32 prefix: %s", newPrefix)

	// Load state
	state, err := loadState()
	if err != nil {
		logger.Fatalf("Load state failed: %v", err)
	}

	// Get latest block height
	latestHeight, err := getLatestBlockHeight()
	if err != nil {
		logger.Fatalf("Get block height failed: %v", err)
	}

	// Check if block height has changed
	if shouldUpdateBalances(state.BlockHeight, latestHeight) {
		logger.Println("=== Starting Balance Update ===")
		logger.Printf("Removing old balance files...")
		if err := os.Remove("balances.json"); err != nil && !os.IsNotExist(err) {
			logger.Printf("Warning: failed to remove balances.json: %v", err)
		}
		if err := os.Remove("balances.json.tmp"); err != nil && !os.IsNotExist(err) {
			logger.Printf("Warning: failed to remove balances.json.tmp: %v", err)
		}
	}

	state.BlockHeight = latestHeight
	if err := saveState(state, false); err != nil { // Save to state.json immediately
		logger.Printf("Failed to save state: %v", err)
	}

	// Fetch data
	data := make(map[string][]map[string]interface{})
	var accounts []map[string]interface{}

	// Fetch accounts via REST
	if fileData, err := os.ReadFile("accounts.json"); err == nil {
		// Load existing accounts
		if err := json.Unmarshal(fileData, &accounts); err != nil {
			logger.Fatalf("Failed to parse accounts.json: %v", err)
		}
		logger.Printf("Loaded %d existing accounts", len(accounts))

		// Find missing and new accounts
		missingNums, err := findMissingAndNewAccounts(accounts)
		if err != nil {
			logger.Printf("Warning: Error checking for missing accounts: %v", err)
		} else if len(missingNums) > 0 {
			logger.Printf("Fetching %d missing accounts...", len(missingNums))
			newAccounts, err := fetchMissingAccounts(missingNums, latestHeight)
			if err != nil {
				logger.Printf("Warning: Failed to fetch missing accounts: %v", err)
			} else {
				accounts = append(accounts, newAccounts...)
				logger.Printf("Added %d new/missing accounts", len(newAccounts))

				// Save updated accounts
				if err := writeJSONFile("accounts.json", accounts); err != nil {
					logger.Printf("Warning: Failed to save updated accounts: %v", err)
				}
			}
		} else {
			logger.Println("No missing or new accounts found")
		}
	} else {
		// No existing accounts.json, fetch everything
		logger.Println("No existing accounts.json, fetching all accounts...")
		accounts, err = fetchAccountsParallel()
		if err != nil {
			logger.Fatalf("Failed to fetch accounts: %v", err)
		}
	}

	// Process accounts
	if accounts, err = processAccounts(accounts); err != nil {
		logger.Fatalf("Failed to process accounts: %v", err)
	}
	data["accounts"] = accounts

	// Update balances if needed
	if shouldUpdateBalances(state.BlockHeight, latestHeight) {
		data["balances"], err = updateBalances(accounts, latestHeight)
		if err != nil {
			logger.Fatalf("Failed to update balances: %v", err)
		}
	} else {
		// Load existing balances
		var balances []map[string]interface{}
		if balanceData, err := os.ReadFile("balances.json"); err == nil {
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
		if err := generateRichlist(data["balances"]); err != nil {
			logger.Printf("Warning: Failed to generate richlist: %v", err)
		}
	}

	logger.Println("=== Fetching Validators ===")
	data["validators"], err = fetchValidators(latestHeight)
	if err != nil {
		logger.Printf("Warning: validator fetch failed: %v", err)
	}
	logger.Printf("Found %d validators", len(data["validators"]))

	logger.Println("=== Fetching Delegations ===")
	data["delegations"], err = fetchRPC("/cosmos.staking.v1beta1/delegations", "delegations.json.tmp", latestHeight)
	if err != nil {
		logger.Printf("Warning: delegations fetch failed: %v", err)
	}
	logger.Printf("Found %d delegations", len(data["delegations"]))

	logger.Println("=== Fetching Denom Metadata ===")
	data["denom_metadata"], err = fetchRPC("/cosmos.bank.v1beta1/denoms_metadata", "metadatas.json.tmp", latestHeight)
	if err != nil {
		logger.Printf("Warning: metadata fetch failed: %v", err)
	}
	logger.Printf("Found %d denom metadata entries", len(data["denom_metadata"]))

	// Fetch parameters
	logger.Println("=== Fetching Module Parameters ===")
	params := make(map[string]map[string]interface{})
	for _, module := range []string{"auth", "bank", "staking", "distribution", "gov"} {
		logger.Printf("Fetching %s parameters...", module)
		params[module], err = fetchParamsRPC(module, latestHeight)
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
			if err := writeJSONFile(key+".json", data[key]); err != nil {
				logger.Printf("Warning: write %s failed: %v", key, err)
			}
		}
	}

	logger.Println("Writing parameter files...")
	for module, moduleParams := range params {
		logger.Printf("Writing %s_params.json...", module)
		if err := writeJSONFile(module+"_params.json", moduleParams); err != nil {
			logger.Printf("Warning: write %s params failed: %v", module, err)
		}
	}

	// After fetching balances, save state again
	if err := saveState(state, true); err != nil {
		logger.Printf("Failed to save final state: %v", err)
	}

	logger.Println("=== Snapshot Complete ===")
	logger.Printf("Final block height: %d", latestHeight)
	logger.Printf("Total accounts: %d", len(data["accounts"]))
	logger.Printf("Total balances: %d", len(data["balances"]))
	logger.Printf("Total validators: %d", len(data["validators"]))
	logger.Printf("Total delegations: %d", len(data["delegations"]))
	logger.Printf("Total metadata entries: %d", len(data["denom_metadata"]))
	logger.Println("Snapshot completed!")
	fmt.Println("Snapshot completed!")
}
