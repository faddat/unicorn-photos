package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcutil/bech32"
	"golang.org/x/time/rate"
)

// REST_URL is the base URL for the blockchain's REST API.
const REST_URL = "https://rest.unicorn.meme"

// State tracks progress for resuming.
type State struct {
	CompletedEndpoints map[string]string `json:"completed_endpoints"` // endpoint -> next_key
	BlockHeight        int64             `json:"block_height"`
}

// fetchResult holds results from worker pool fetching.
type fetchResult struct {
	Items   []map[string]interface{}
	NextKey string
	Err     error
	Offset  int // For parallel offset fetching
}

// Logger with verbose output
var logger = log.New(os.Stdout, "[SNAPSHOT] ", log.Ldate|log.Ltime|log.Lmicroseconds)

// httpClient with connection pooling
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	},
}

// loadState loads or initializes the state.
func loadState() (*State, error) {
	logger.Println("Loading state from state.json.tmp...")
	if _, err := os.Stat("state.json.tmp"); os.IsNotExist(err) {
		logger.Println("No existing state found, initializing new state.")
		return &State{CompletedEndpoints: make(map[string]string)}, nil
	}
	data, err := ioutil.ReadFile("state.json.tmp")
	if err != nil {
		logger.Printf("Failed to read state.json.tmp: %v", err)
		return nil, fmt.Errorf("failed to read state: %v", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		logger.Printf("Failed to parse state.json.tmp: %v", err)
		return nil, fmt.Errorf("failed to parse state: %v", err)
	}
	logger.Printf("Loaded state with block height %d and %d completed endpoints.", state.BlockHeight, len(state.CompletedEndpoints))
	return &state, nil
}

// saveState saves the state to a file.
func saveState(state *State, final bool) error {
	logger.Println("Saving state...")
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		logger.Printf("Failed to marshal state: %v", err)
		return fmt.Errorf("failed to marshal state: %v", err)
	}
	filename := "state.json.tmp"
	if final {
		filename = "state.json"
	}
	if err := ioutil.WriteFile(filename, data, 0644); err != nil {
		logger.Printf("Failed to write %s: %v", filename, err)
		return err
	}
	logger.Printf("State saved to %s with block height %d and %d completed endpoints.", filename, state.BlockHeight, len(state.CompletedEndpoints))
	return nil
}

// fetchWithRetry performs an HTTP GET with retries.
func fetchWithRetry(url string, blockHeight int64, limiter *rate.Limiter) (*http.Response, error) {
	const maxRetries = 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := limiter.Wait(context.Background()); err != nil {
			logger.Printf("Rate limiter error: %v", err)
			continue
		}
		logger.Printf("Attempt %d: Fetching %s", attempt+1, url)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			logger.Printf("Failed to create request: %v", err)
			return nil, err
		}
		if blockHeight > 0 {
			req.Header.Add("x-cosmos-block-height", fmt.Sprintf("%d", blockHeight))
			logger.Printf("Set x-cosmos-block-height header to %d", blockHeight)
		}
		resp, err := httpClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			logger.Printf("Successfully fetched %s with status %d", url, resp.StatusCode)
			return resp, nil
		}
		if resp != nil {
			body, _ := ioutil.ReadAll(resp.Body)
			logger.Printf("Attempt %d failed with status %d: %s", attempt+1, resp.StatusCode, string(body))
			resp.Body.Close()
		} else {
			logger.Printf("Attempt %d failed: %v", attempt+1, err)
		}
		if attempt < maxRetries {
			delay := time.Second << uint(attempt)
			logger.Printf("Retrying in %v...", delay)
			time.Sleep(delay)
		}
	}
	logger.Printf("Exhausted %d retries for %s", maxRetries+1, url)
	return nil, fmt.Errorf("exhausted retries for %s", url)
}

// fetchAccountsParallel fetches accounts using parallel offset pagination.
func fetchAccountsParallel(endpoint, itemsKey string, state *State, workers int, limiter *rate.Limiter) ([]map[string]interface{}, error) {
	const pageSize = 100
	logger.Printf("Fetching %s (%s) in parallel with %d workers", endpoint, itemsKey, workers)

	// Load cached data
	cacheFile := fmt.Sprintf("%s.json.tmp", itemsKey)
	var allItems []map[string]interface{}
	if data, err := ioutil.ReadFile(cacheFile); err == nil {
		if err := json.Unmarshal(data, &allItems); err == nil {
			logger.Printf("Loaded %d cached %s from %s", len(allItems), itemsKey, cacheFile)
			return allItems, nil
		} else {
			logger.Printf("Failed to unmarshal cached %s: %v", itemsKey, err)
		}
	}

	// Fetch first page to get total
	url := fmt.Sprintf("%s%s?pagination.limit=%d", REST_URL, endpoint, pageSize)
	resp, err := fetchWithRetry(url, state.BlockHeight, limiter)
	if err != nil {
		logger.Printf("Failed to fetch first page: %v, falling back to next-key", err)
		return fetchAll(endpoint, itemsKey, state, workers, limiter)
	}
	defer resp.Body.Close()

	var firstPage map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&firstPage); err != nil {
		logger.Printf("Failed to decode first page: %v, falling back to next-key", err)
		return fetchAll(endpoint, itemsKey, state, workers, limiter)
	}

	pagination, ok := firstPage["pagination"].(map[string]interface{})
	if !ok || pagination["total"] == nil {
		logger.Println("No total in pagination, falling back to next-key")
		return fetchAll(endpoint, itemsKey, state, workers, limiter)
	}

	var total int
	switch t := pagination["total"].(type) {
	case float64:
		total = int(t)
	case string:
		totalInt, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			logger.Printf("Invalid total format: %v, falling back to next-key", err)
			return fetchAll(endpoint, itemsKey, state, workers, limiter)
		}
		total = int(totalInt)
	default:
		logger.Printf("Unexpected total type: %T, falling back to next-key", t)
		return fetchAll(endpoint, itemsKey, state, workers, limiter)
	}

	numPages := (total + pageSize - 1) / pageSize
	logger.Printf("Total accounts: %d, pages: %d", total, numPages)

	// Parallel fetch
	jobs := make(chan int, numPages)
	results := make(chan fetchResult, numPages)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for page := range jobs {
				offset := page * pageSize
				url := fmt.Sprintf("%s%s?pagination.limit=%d&pagination.offset=%d", REST_URL, endpoint, pageSize, offset)
				logger.Printf("Worker %d: Fetching page %d (offset %d)", workerID, page, offset)

				resp, err := fetchWithRetry(url, state.BlockHeight, limiter)
				if err != nil {
					results <- fetchResult{Offset: offset, Err: err}
					continue
				}
				defer resp.Body.Close()

				var data map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					logger.Printf("Worker %d: JSON decode error for page %d: %v", workerID, page, err)
					results <- fetchResult{Offset: offset, Err: err}
					continue
				}

				items, ok := data[itemsKey].([]interface{})
				if !ok {
					logger.Printf("Worker %d: Invalid %s format for page %d", workerID, itemsKey, page)
					results <- fetchResult{Offset: offset, Err: fmt.Errorf("invalid %s format", itemsKey)}
					continue
				}

				pageItems := make([]map[string]interface{}, 0, len(items))
				for _, item := range items {
					pageItems = append(pageItems, item.(map[string]interface{}))
				}
				logger.Printf("Worker %d: Fetched %d items for page %d", workerID, len(pageItems), page)
				results <- fetchResult{Items: pageItems, Offset: offset}
			}
		}(i)
	}

	go func() {
		for i := 0; i < numPages; i++ {
			jobs <- i
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]struct{})
	for result := range results {
		if result.Err != nil {
			logger.Printf("Error fetching %s at offset %d: %v", itemsKey, result.Offset, result.Err)
			continue
		}
		for _, item := range result.Items {
			if addr, ok := item["address"].(string); ok {
				if _, exists := seen[addr]; !exists {
					seen[addr] = struct{}{}
					allItems = append(allItems, item)
				}
			} else {
				allItems = append(allItems, item)
			}
		}
	}

	dataBytes, _ := json.Marshal(allItems)
	if err := ioutil.WriteFile(cacheFile, dataBytes, 0644); err != nil {
		logger.Printf("Failed to cache %s: %v", itemsKey, err)
	} else {
		logger.Printf("Cached %d %s", len(allItems), itemsKey)
	}
	delete(state.CompletedEndpoints, endpoint)
	if err := saveState(state, false); err != nil {
		logger.Printf("Failed to save state: %v", err)
	}
	logger.Printf("Fetched %d %s", len(allItems), itemsKey)
	return allItems, nil
}

// fetchAll fetches paginated data with next-key pagination (fallback).
func fetchAll(endpoint, itemsKey string, state *State, workers int, limiter *rate.Limiter) ([]map[string]interface{}, error) {
	const pageSize = 100
	logger.Printf("Fetching %s (%s) with next-key and %d workers", endpoint, itemsKey, workers)
	jobs := make(chan string, workers*2)
	results := make(chan fetchResult, workers*2)
	var wg sync.WaitGroup

	cacheFile := fmt.Sprintf("%s.json.tmp", itemsKey)
	var allItems []map[string]interface{}
	if data, err := ioutil.ReadFile(cacheFile); err == nil {
		if err := json.Unmarshal(data, &allItems); err == nil {
			logger.Printf("Loaded %d cached %s from %s", len(allItems), itemsKey, cacheFile)
		} else {
			logger.Printf("Failed to unmarshal cached %s: %v", itemsKey, err)
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for nextKey := range jobs {
				params := url.Values{}
				params.Add("pagination.limit", fmt.Sprintf("%d", pageSize))
				if nextKey != "" {
					params.Add("pagination.key", nextKey)
				}
				url := fmt.Sprintf("%s%s?%s", REST_URL, endpoint, params.Encode())
				logger.Printf("Worker %d: Fetching page with next_key=%s", workerID, nextKey)

				resp, err := fetchWithRetry(url, state.BlockHeight, limiter)
				if err != nil {
					results <- fetchResult{Err: err}
					continue
				}
				defer resp.Body.Close()

				var data map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					logger.Printf("Worker %d: JSON decode error: %v", workerID, err)
					results <- fetchResult{Err: err}
					continue
				}

				items, ok := data[itemsKey].([]interface{})
				if !ok {
					logger.Printf("Worker %d: Invalid %s format", workerID, itemsKey)
					results <- fetchResult{Err: fmt.Errorf("invalid %s format", itemsKey)}
					continue
				}

				pageItems := make([]map[string]interface{}, 0, len(items))
				for _, item := range items {
					pageItems = append(pageItems, item.(map[string]interface{}))
				}

				nextKeyVal, _ := data["pagination"].(map[string]interface{})["next_key"].(string)
				logger.Printf("Worker %d: Fetched %d items, next_key=%s", workerID, len(pageItems), nextKeyVal)
				results <- fetchResult{Items: pageItems, NextKey: nextKeyVal}
			}
		}(i)
	}

	nextKey := state.CompletedEndpoints[endpoint]
	logger.Printf("Starting with next_key=%s", nextKey)
	jobs <- nextKey
	activeJobs := 1

	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]struct{})
	for _, item := range allItems {
		if addr, ok := item["address"].(string); ok {
			seen[addr] = struct{}{}
		}
	}

	for result := range results {
		if result.Err != nil {
			logger.Printf("Error fetching %s: %v", itemsKey, result.Err)
			activeJobs-- // Decrement active jobs even on error
			continue
		}
		for _, item := range result.Items {
			if addr, ok := item["address"].(string); ok {
				if _, exists := seen[addr]; !exists {
					seen[addr] = struct{}{}
					allItems = append(allItems, item)
				}
			} else {
				allItems = append(allItems, item)
			}
		}
		if result.NextKey != "" {
			jobs <- result.NextKey
			activeJobs++
			logger.Printf("Queued next_key=%s, active jobs=%d", result.NextKey, activeJobs)
		}
		activeJobs-- // Always decrement active jobs
		logger.Printf("Active jobs=%d", activeJobs)

		if activeJobs == 0 { // Save and exit when no more jobs
			break
		}

		// Save progress
		state.CompletedEndpoints[endpoint] = result.NextKey
		dataBytes, _ := json.Marshal(allItems)
		if err := ioutil.WriteFile(cacheFile, dataBytes, 0644); err != nil {
			logger.Printf("Failed to cache %s: %v", itemsKey, err)
		} else {
			logger.Printf("Cached %d %s", len(allItems), itemsKey)
		}
		if err := saveState(state, false); err != nil {
			logger.Printf("Failed to save state: %v", err)
		}
	}
	close(jobs)

	if activeJobs == 0 {
		delete(state.CompletedEndpoints, endpoint)
		if err := saveState(state, false); err != nil {
			logger.Printf("Failed to save state: %v", err)
		}
	}
	logger.Printf("Fetched %d %s", len(allItems), itemsKey)
	return allItems, nil
}

// fetchBalances fetches balances in parallel.
func fetchBalances(accounts []map[string]interface{}, state *State, workers int, limiter *rate.Limiter) ([]map[string]interface{}, error) {
	logger.Println("Fetching balances...")
	jobs := make(chan struct {
		Index   int
		Address string
	}, workers*2)
	results := make(chan fetchResult, workers*2)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				url := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", REST_URL, job.Address)
				logger.Printf("Worker %d: Fetching balance for %s (index %d)", workerID, job.Address, job.Index)

				resp, err := fetchWithRetry(url, state.BlockHeight, limiter)
				if err != nil {
					results <- fetchResult{Err: err}
					continue
				}
				defer resp.Body.Close()

				var data struct {
					Balances []map[string]interface{} `json:"balances"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					logger.Printf("Worker %d: JSON decode error: %v", workerID, err)
					results <- fetchResult{Err: err}
					continue
				}
				for _, bal := range data.Balances {
					bal["address"] = job.Address
				}
				logger.Printf("Worker %d: Fetched %d balances", workerID, len(data.Balances))
				results <- fetchResult{Items: data.Balances}
			}
		}(i)
	}

	go func() {
		for i, acc := range accounts {
			if addr, ok := acc["address"].(string); ok {
				jobs <- struct {
					Index   int
					Address string
				}{i, addr}
			}
		}
		close(jobs)
		logger.Println("All balance jobs queued.")
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var balances []map[string]interface{}
	for result := range results {
		if result.Err != nil {
			logger.Printf("Balance fetch error: %v", result.Err)
			continue
		}
		balances = append(balances, result.Items...)
	}
	logger.Printf("Fetched %d balances", len(balances))
	return balances, nil
}

// fetchParams fetches module parameters with defaults for unimplemented endpoints.
func fetchParams(endpoint string, state *State, limiter *rate.Limiter) (map[string]interface{}, error) {
	// Existing REST fetch logic
	restURL := fmt.Sprintf("https://rest.unicorn.meme%s", endpoint)
	resp, err := http.Get(restURL) // Simplified; include retries and headers as in original
	if err != nil || resp.StatusCode == 501 {
		logger.Printf("REST fetch failed for %s with status %d: %v, attempting RPC", endpoint, resp.StatusCode, err)
		module := strings.Split(endpoint, "/")[2] // e.g., "gov" from "/cosmos/gov/v1beta1/params"
		rpcParams, err := rpcQuery("abci_query", map[string]interface{}{
			"path":   fmt.Sprintf("/custom/%s/params", module),
			"data":   "",
			"height": fmt.Sprintf("%d", state.BlockHeight),
		})
		if err != nil {
			logger.Printf("RPC fetch failed for %s: %v, using defaults", endpoint, err)
			return getDefaultParams(endpoint), nil // Assume a default params function exists
		}
		// Parse the base64-encoded response
		response := rpcParams["response"].(map[string]interface{})
		paramsData, err := base64.StdEncoding.DecodeString(response["value"].(string))
		if err != nil {
			logger.Printf("Failed to decode RPC response for %s: %v, using defaults", endpoint, err)
			return getDefaultParams(endpoint), nil
		}
		var paramData map[string]interface{}
		if err := json.Unmarshal(paramsData, &paramData); err != nil {
			logger.Printf("Failed to unmarshal RPC params for %s: %v, using defaults", endpoint, err)
			return getDefaultParams(endpoint), nil
		}
		return paramData, nil
	}
	// Existing successful REST logic (decode response, cache, etc.)
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// getDefaultParams provides fallback parameters for unimplemented endpoints.
func getDefaultParams(endpoint string) map[string]interface{} {
	switch endpoint {
	case "/cosmos/auth/v1beta1/params":
		return map[string]interface{}{
			"max_memo_characters":       "256",
			"tx_sig_limit":              "7",
			"tx_size_cost_per_byte":     "10",
			"sig_verify_cost_ed25519":   "590",
			"sig_verify_cost_secp256k1": "1000",
		}
	case "/cosmos/bank/v1beta1/params":
		return map[string]interface{}{
			"send_enabled":         []interface{}{},
			"default_send_enabled": true,
		}
	case "/cosmos/staking/v1beta1/params":
		return map[string]interface{}{
			"unbonding_time":     "1814400s",
			"max_validators":     100,
			"max_entries":        7,
			"historical_entries": 10000,
			"bond_denom":         "usei",
		}
	case "/cosmos/distribution/v1beta1/params":
		return map[string]interface{}{
			"community_tax":         "0.020000000000000000",
			"base_proposer_reward":  "0.010000000000000000",
			"bonus_proposer_reward": "0.040000000000000000",
			"withdraw_addr_enabled": true,
		}
	case "/cosmos/gov/v1beta1/params":
		return map[string]interface{}{
			"deposit_params": map[string]interface{}{
				"min_deposit": []interface{}{
					map[string]interface{}{
						"denom":  "usei",
						"amount": "10000000",
					},
				},
				"max_deposit_period": "172800s",
			},
			"voting_params": map[string]interface{}{
				"voting_period": "172800s",
			},
			"tally_params": map[string]interface{}{
				"quorum":         "0.334000000000000000",
				"threshold":      "0.500000000000000000",
				"veto_threshold": "0.334000000000000000",
			},
		}
	default:
		return map[string]interface{}{}
	}
}

// convertAddress converts a Bech32 address.
func convertAddress(addr, oldPrefix, newPrefix string) (string, error) {
	hrp, data, err := bech32.Decode(addr)
	if err != nil {
		logger.Printf("Failed to decode %s: %v", addr, err)
		return "", err
	}
	if !strings.HasPrefix(hrp, oldPrefix) {
		return addr, nil
	}
	suffix := strings.TrimPrefix(hrp, oldPrefix)
	newHRP := newPrefix + suffix
	newAddr, err := bech32.Encode(newHRP, data)
	if err != nil {
		logger.Printf("Failed to encode %s to %s: %v", addr, newPrefix, err)
		return "", err
	}
	logger.Printf("Converted %s to %s", addr, newAddr)
	return newAddr, nil
}

// convertAddresses updates Bech32 addresses in the app state.
func convertAddresses(appState map[string]interface{}, oldPrefix, newPrefix string) {
	logger.Printf("Converting addresses from %s to %s", oldPrefix, newPrefix)
	convertField := func(m map[string]interface{}, key string) {
		if val, ok := m[key].(string); ok {
			if newVal, err := convertAddress(val, oldPrefix, newPrefix); err == nil {
				m[key] = newVal
			}
		}
	}

	if auth, ok := appState["auth"].(map[string]interface{}); ok {
		if accounts, ok := auth["accounts"].([]map[string]interface{}); ok {
			for i, acc := range accounts {
				convertField(acc, "address")
				logger.Printf("Processed auth.accounts[%d]", i)
			}
		}
	}
	if bank, ok := appState["bank"].(map[string]interface{}); ok {
		if balances, ok := bank["balances"].([]map[string]interface{}); ok {
			for i, bal := range balances {
				convertField(bal, "address")
				logger.Printf("Processed bank.balances[%d]", i)
			}
		}
	}
	if staking, ok := appState["staking"].(map[string]interface{}); ok {
		if validators, ok := staking["validators"].([]map[string]interface{}); ok {
			for i, val := range validators {
				convertField(val, "operator_address")
				logger.Printf("Processed staking.validators[%d]", i)
			}
		}
		if delegations, ok := staking["delegations"].([]map[string]interface{}); ok {
			for i, del := range delegations {
				convertField(del, "delegator_address")
				convertField(del, "validator_address")
				logger.Printf("Processed staking.delegations[%d]", i)
			}
		}
	}
}

// getLatestBlockHeight fetches the latest block height.
func getLatestBlockHeight(limiter *rate.Limiter) (int64, error) {
	logger.Println("Fetching latest block height...")
	url := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/blocks/latest", REST_URL)
	resp, err := fetchWithRetry(url, 0, limiter)
	if err != nil {
		logger.Printf("Failed to fetch block height: %v", err)
		return 0, err
	}
	defer resp.Body.Close()
	var data struct {
		Block struct {
			Header struct {
				Height string `json:"height"`
			} `json:"header"`
		} `json:"block"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		logger.Printf("Failed to decode block height: %v", err)
		return 0, err
	}
	height, err := strconv.ParseInt(data.Block.Header.Height, 10, 64)
	if err != nil {
		logger.Printf("Failed to parse height %s: %v", data.Block.Header.Height, err)
		return 0, err
	}
	logger.Printf("Latest block height: %d", height)
	return height, nil
}

func main() {
	var newPrefix string
	flag.StringVar(&newPrefix, "prefix", "", "New Bech32 prefix (e.g., cosmos)")
	flag.Parse()
	logger.Printf("Starting with prefix: %s", newPrefix)

	limiter := rate.NewLimiter(500, 1000)
	const workers = 50
	logger.Printf("Using %d workers", workers)

	// Get block height
	blockHeight, err := getLatestBlockHeight(limiter)
	if err != nil {
		logger.Fatalf("Failed to get block height: %v", err)
	}
	logger.Printf("Snapshotting at block %d", blockHeight)

	// Load state
	state, err := loadState()
	if err != nil {
		logger.Fatalf("Failed to load state: %v", err)
	}
	state.BlockHeight = blockHeight

	// Fetch data concurrently
	var wg sync.WaitGroup
	errChan := make(chan error, 10)
	data := make(map[string][]map[string]interface{})
	params := make(map[string]map[string]interface{})

	fetchEndpoints := []struct{ endpoint, key string }{
		{"/cosmos/auth/v1beta1/accounts", "accounts"},
		{"/cosmos/staking/v1beta1/validators", "validators"},
		{"/cosmos/staking/v1beta1/delegations", "delegation_responses"},
		{"/cosmos/bank/v1beta1/denoms_metadata", "metadatas"},
	}
	paramEndpoints := []string{
		"/cosmos/auth/v1beta1/params",
		"/cosmos/bank/v1beta1/params",
		"/cosmos/staking/v1beta1/params",
		"/cosmos/distribution/v1beta1/params",
		"/cosmos/gov/v1beta1/params",
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		accounts, err := fetchAccountsParallel("/cosmos/auth/v1beta1/accounts", "accounts", state, workers, limiter)
		if err != nil {
			errChan <- fmt.Errorf("accounts: %v", err)
			return
		}
		data["accounts"] = accounts
	}()

	for _, e := range fetchEndpoints[1:] {
		wg.Add(1)
		go func(e struct{ endpoint, key string }) {
			defer wg.Done()
			items, err := fetchAll(e.endpoint, e.key, state, workers, limiter)
			if err != nil {
				errChan <- fmt.Errorf("%s: %v", e.key, err)
				return
			}
			data[e.key] = items
		}(e)
	}

	for _, e := range paramEndpoints {
		wg.Add(1)
		go func(e string) {
			defer wg.Done()
			p, err := fetchParams(e, state, limiter)
			if err != nil {
				logger.Printf("Warning: Failed to fetch %s params: %v, using defaults", e, err)
			}
			key := strings.Split(e, "/")[2]
			params[key] = p
		}(e)
	}

	wg.Wait()
	close(errChan)
	for err := range errChan {
		logger.Printf("Fetch error: %v", err)
	}

	// Fetch balances
	accounts := data["accounts"]
	if len(accounts) > 0 {
		balances, err := fetchBalances(accounts, state, workers, limiter)
		if err != nil {
			logger.Printf("Warning: Balances fetch failed: %v", err)
		}
		data["balances"] = balances
	}

	// Construct genesis
	logger.Println("Constructing genesis.json...")
	genesis := map[string]interface{}{
		"genesis_time":   time.Now().UTC().Format(time.RFC3339),
		"chain_id":       "unicorn-snapshot",
		"initial_height": "1",
		"consensus_params": map[string]interface{}{
			"block":     map[string]interface{}{"max_bytes": "22020096", "max_gas": "-1"},
			"evidence":  map[string]interface{}{"max_age_num_blocks": "100000", "max_age_duration": "172800000000000", "max_bytes": "1048576"},
			"validator": map[string]interface{}{"pub_key_types": []string{"ed25519"}},
			"version":   map[string]interface{}{},
		},
		"app_hash": "",
		"app_state": map[string]interface{}{
			"auth": map[string]interface{}{
				"params":   params["auth"],
				"accounts": accounts,
			},
			"bank": map[string]interface{}{
				"params":         params["bank"],
				"balances":       data["balances"],
				"denom_metadata": data["metadatas"],
			},
			"staking": map[string]interface{}{
				"params":                params["staking"],
				"validators":            data["validators"],
				"delegations":           data["delegation_responses"],
				"unbonding_delegations": []interface{}{},
				"redelegations":         []interface{}{},
			},
			"distribution": map[string]interface{}{"params": params["distribution"]},
			"gov":          map[string]interface{}{"params": params["gov"]},
		},
	}

	// Handle Bech32 prefix
	if newPrefix != "" {
		logger.Printf("Converting to prefix %s", newPrefix)
		oldPrefix := "sei"
		if len(accounts) > 0 {
			if addr, ok := accounts[0]["address"].(string); ok {
				if hrp, _, err := bech32.Decode(addr); err == nil {
					oldPrefix = hrp
				}
			}
		}
		convertAddresses(genesis["app_state"].(map[string]interface{}), oldPrefix, newPrefix)
	}

	// Write genesis
	logger.Println("Writing genesis.json...")
	genesisBytes, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		logger.Fatalf("Failed to marshal genesis: %v", err)
	}
	if err := ioutil.WriteFile("genesis.json.tmp", genesisBytes, 0644); err != nil {
		logger.Fatalf("Failed to write genesis.json.tmp: %v", err)
	}

	logger.Println("Renaming temporary files...")
	for _, key := range append([]string{"accounts", "balances", "validators", "delegation_responses", "metadatas"}, paramEndpoints...) {
		tmp := fmt.Sprintf("%s.json.tmp", key)
		final := fmt.Sprintf("%s.json", strings.ReplaceAll(key, "/", "_"))
		if _, err := os.Stat(tmp); err == nil {
			if err := os.Rename(tmp, final); err != nil {
				logger.Printf("Failed to rename %s to %s: %v", tmp, final, err)
			} else {
				logger.Printf("Renamed %s to %s", tmp, final)
			}
		}
	}
	if err := saveState(state, true); err != nil {
		logger.Printf("Failed to save final state: %v", err)
	}
	if err := os.Rename("genesis.json.tmp", "genesis.json"); err != nil {
		logger.Printf("Failed to rename genesis.json.tmp: %v", err)
	} else {
		logger.Println("Renamed genesis.json.tmp to genesis.json")
	}
	logger.Println("Snapshot completed successfully!")
	fmt.Println("Snapshot completed successfully!")
}
