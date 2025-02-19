package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
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
}

// Logger with buffering for performance
var logger = log.New(&bufferedWriter{Writer: os.Stdout}, "[SNAPSHOT] ", log.Ldate|log.Ltime|log.Lmicroseconds)

// bufferedWriter reduces I/O overhead by buffering log output
type bufferedWriter struct {
	sync.Mutex
	Writer io.Writer
	Buffer bytes.Buffer
}

func (bw *bufferedWriter) Write(p []byte) (n int, err error) {
	bw.Mutex.Lock()
	defer bw.Mutex.Unlock()
	bw.Buffer.Write(p)
	if bytes.Contains(p, []byte("\n")) {
		_, err = bw.Writer.Write(bw.Buffer.Bytes())
		bw.Buffer.Reset()
	}
	return len(p), err
}

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
	data, err := decompressFile("state.json.tmp")
	if err != nil {
		logger.Printf("Failed to read/decompress state.json.tmp: %v", err)
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

// saveState saves the state to a temporary compressed file.
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
	if err := compressAndWriteFile(filename, data); err != nil {
		logger.Printf("Failed to write %s: %v", filename, err)
		return err
	}
	logger.Printf("State saved to %s with block height %d and %d completed endpoints.", filename, state.BlockHeight, len(state.CompletedEndpoints))
	return nil
}

// fetchWithRetry performs an HTTP GET with retries and adaptive rate limiting.
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

// fetchAll fetches paginated data using next-key pagination with a worker pool.
func fetchAll(endpoint, itemsKey string, state *State, workers int, limiter *rate.Limiter) ([]map[string]interface{}, error) {
	const pageSize = 100
	logger.Printf("Starting fetchAll for %s (%s) with %d workers", endpoint, itemsKey, workers)
	jobs := make(chan string, workers*2)
	results := make(chan fetchResult, workers*2)
	var wg sync.WaitGroup

	// Load cached data
	cacheFile := fmt.Sprintf("%s.json.tmp", itemsKey)
	var allItems []map[string]interface{}
	if data, err := decompressFile(cacheFile); err == nil {
		if err := json.Unmarshal(data, &allItems); err == nil {
			logger.Printf("Loaded %d cached %s from %s", len(allItems), itemsKey, cacheFile)
		} else {
			logger.Printf("Failed to unmarshal cached %s: %v", itemsKey, err)
		}
	} else {
		logger.Printf("No cached data found for %s at %s", itemsKey, cacheFile)
	}

	// Start workers
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
					logger.Printf("Worker %d: Invalid %s format in response", workerID, itemsKey)
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

	// Start with initial key from state
	nextKey := state.CompletedEndpoints[endpoint]
	logger.Printf("Starting fetch with initial next_key=%s", nextKey)
	jobs <- nextKey
	activeJobs := 1

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]struct{}, len(allItems)*2) // Pre-allocate for efficiency
	for _, item := range allItems {
		if addr, ok := item["address"].(string); ok {
			seen[addr] = struct{}{}
		}
	}

	for result := range results {
		if result.Err != nil {
			logger.Printf("Error fetching %s: %v", itemsKey, result.Err)
			continue
		}
		for _, item := range result.Items {
			if addr, ok := item["address"].(string); ok {
				if _, exists := seen[addr]; !exists {
					seen[addr] = struct{}{}
					allItems = append(allItems, item)
				}
			} else {
				allItems = append(allItems, item) // Non-address items (e.g., metadata) added directly
			}
		}
		if result.NextKey != "" {
			jobs <- result.NextKey
			activeJobs++
			logger.Printf("Queued new job with next_key=%s, active jobs=%d", result.NextKey, activeJobs)
		} else {
			activeJobs--
			logger.Printf("No next_key, active jobs reduced to %d", activeJobs)
		}
		if activeJobs > 0 {
			state.CompletedEndpoints[endpoint] = result.NextKey
			dataBytes, err := json.Marshal(allItems) // No indent for speed
			if err != nil {
				logger.Printf("Failed to marshal %s for caching: %v", itemsKey, err)
			} else {
				if err := compressAndWriteFile(cacheFile, dataBytes); err != nil {
					logger.Printf("Failed to write %s cache: %v", cacheFile, err)
				} else {
					logger.Printf("Cached %d %s to %s", len(allItems), itemsKey, cacheFile)
				}
			}
			if err := saveState(state, false); err != nil {
				logger.Printf("Failed to save state: %v", err)
			}
		}
	}
	close(jobs)

	if activeJobs == 0 {
		delete(state.CompletedEndpoints, endpoint)
		logger.Printf("Completed fetching %s, removed from state", endpoint)
		if err := saveState(state, false); err != nil {
			logger.Printf("Failed to save state after completion: %v", err)
		}
	}
	logger.Printf("Total %s fetched: %d", itemsKey, len(allItems))
	return allItems, nil
}

// fetchBalances fetches balances for all accounts in parallel.
func fetchBalances(accounts []map[string]interface{}, state *State, workers int, limiter *rate.Limiter) ([]map[string]interface{}, error) {
	logger.Println("Starting parallel balance fetch...")
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
					logger.Printf("Worker %d: JSON decode error for %s: %v", workerID, job.Address, err)
					results <- fetchResult{Err: err}
					continue
				}
				for _, bal := range data.Balances {
					bal["address"] = job.Address
				}
				logger.Printf("Worker %d: Fetched %d balances for %s", workerID, len(data.Balances), job.Address)
				results <- fetchResult{Items: data.Balances}
			}
		}(i)
	}

	// Send jobs
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
		logger.Println("All balance fetch jobs queued.")
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	balances := make([]map[string]interface{}, 0, len(accounts))
	for result := range results {
		if result.Err != nil {
			logger.Printf("Balance fetch error: %v", result.Err)
			continue
		}
		balances = append(balances, result.Items...)
	}
	logger.Printf("Completed balance fetch, total balances: %d", len(balances))
	return balances, nil
}

// fetchParams fetches module parameters.
func fetchParams(endpoint string, state *State, limiter *rate.Limiter) (map[string]interface{}, error) {
	cacheFile := fmt.Sprintf("%s_params.json.tmp", endpoint[1:])
	logger.Printf("Fetching params for %s", endpoint)
	if data, err := decompressFile(cacheFile); err == nil {
		var params map[string]interface{}
		if err := json.Unmarshal(data, &params); err == nil {
			logger.Printf("Loaded cached params from %s", cacheFile)
			return params, nil
		}
		logger.Printf("Failed to unmarshal cached params: %v", err)
	}

	url := fmt.Sprintf("%s%s", REST_URL, endpoint)
	resp, err := fetchWithRetry(url, state.BlockHeight, limiter)
	if err != nil {
		logger.Printf("Failed to fetch %s params: %v", endpoint, err)
		return nil, err
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		logger.Printf("JSON decode error for %s: %v", endpoint, err)
		return nil, err
	}
	params, _ := data["params"].(map[string]interface{})
	dataBytes, err := json.Marshal(params)
	if err != nil {
		logger.Printf("Failed to marshal %s params: %v", endpoint, err)
	} else {
		if err := compressAndWriteFile(cacheFile, dataBytes); err != nil {
			logger.Printf("Failed to cache %s params: %v", endpoint, err)
		} else {
			logger.Printf("Cached %s params to %s", endpoint, cacheFile)
		}
	}
	logger.Printf("Successfully fetched %s params", endpoint)
	return params, nil
}

// compressAndWriteFile compresses data and writes it to a file.
func compressAndWriteFile(filename string, data []byte) error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	return ioutil.WriteFile(filename, buf.Bytes(), 0644)
}

// decompressFile reads and decompresses a file.
func decompressFile(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// convertAddress converts a Bech32 address to a new prefix.
func convertAddress(addr, oldPrefix, newPrefix string) (string, error) {
	hrp, data, err := bech32.Decode(addr)
	if err != nil {
		logger.Printf("Failed to decode Bech32 address %s: %v", addr, err)
		return "", err
	}
	if !strings.HasPrefix(hrp, oldPrefix) {
		logger.Printf("Address %s does not have prefix %s, skipping conversion", addr, oldPrefix)
		return addr, nil
	}
	suffix := strings.TrimPrefix(hrp, oldPrefix)
	newHRP := newPrefix + suffix
	newAddr, err := bech32.Encode(newHRP, data)
	if err != nil {
		logger.Printf("Failed to encode Bech32 address %s with prefix %s: %v", addr, newPrefix, err)
		return "", err
	}
	logger.Printf("Converted address %s to %s", addr, newAddr)
	return newAddr, nil
}

// convertAddresses updates all Bech32 addresses in the app state.
func convertAddresses(appState map[string]interface{}, oldPrefix, newPrefix string) {
	logger.Printf("Converting addresses from prefix %s to %s", oldPrefix, newPrefix)
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
	logger.Println("Address conversion completed.")
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
		logger.Printf("Failed to decode block height response: %v", err)
		return 0, err
	}
	height, err := strconv.ParseInt(data.Block.Header.Height, 10, 64)
	if err != nil {
		logger.Printf("Failed to parse block height %s: %v", data.Block.Header.Height, err)
		return 0, err
	}
	logger.Printf("Latest block height: %d", height)
	return height, nil
}

func main() {
	// Parse flags
	var newPrefix string
	flag.StringVar(&newPrefix, "prefix", "", "New Bech32 prefix for addresses (e.g., cosmos)")
	flag.Parse()
	logger.Printf("Starting with prefix argument: %s", newPrefix)

	// Adaptive rate limiter (starts at 500 req/s, adjusts based on runtime)
	limiter := rate.NewLimiter(500, 1000)
	go func() {
		for range time.Tick(10 * time.Second) {
			// Placeholder for dynamic adjustment (could use success rate or latency)
			logger.Printf("Current rate limit: %v req/s", limiter.Limit())
		}
	}()

	// Optimize worker count based on CPU cores with a cap
	workers := runtime.NumCPU() * 4
	if workers > 50 {
		workers = 50
	}
	logger.Printf("Using %d workers based on %d CPU cores", workers, runtime.NumCPU())

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

	// Fetch all data concurrently
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

	for _, e := range fetchEndpoints {
		wg.Add(1)
		go func(e struct{ endpoint, key string }) {
			defer wg.Done()
			logger.Printf("Starting fetch for %s", e.key)
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
			logger.Printf("Starting params fetch for %s", e)
			p, err := fetchParams(e, state, limiter)
			if err != nil {
				errChan <- fmt.Errorf("%s: %v", e, err)
				return
			}
			key := strings.Split(e, "/")[2]
			params[key] = p
		}(e)
	}

	// Wait for completion
	wg.Wait()
	close(errChan)
	for err := range errChan {
		logger.Printf("Fetch error: %v", err)
	}

	// Fetch balances after accounts
	accounts := data["accounts"]
	if len(accounts) > 0 {
		logger.Println("Fetching balances for accounts...")
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

	// Handle Bech32 prefix conversion
	if newPrefix != "" {
		logger.Printf("Applying Bech32 prefix conversion to %s", newPrefix)
		oldPrefix := "sei"
		if len(accounts) > 0 {
			if addr, ok := accounts[0]["address"].(string); ok {
				if hrp, _, err := bech32.Decode(addr); err == nil {
					oldPrefix = hrp
					logger.Printf("Detected old prefix: %s", oldPrefix)
				}
			}
		}
		convertAddresses(genesis["app_state"].(map[string]interface{}), oldPrefix, newPrefix)
	}

	// Write genesis file
	logger.Println("Writing genesis.json...")
	genesisBytes, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		logger.Fatalf("Failed to marshal genesis: %v", err)
	}
	if err := compressAndWriteFile("genesis.json.tmp", genesisBytes); err != nil {
		logger.Fatalf("Failed to write genesis.json.tmp: %v", err)
	}

	// Rename temporary files
	logger.Println("Renaming temporary files to final names...")
	for _, key := range append([]string{"accounts", "balances", "validators", "delegation_responses", "metadatas"}, paramEndpoints...) {
		tmp := fmt.Sprintf("%s.json.tmp", key)
		final := fmt.Sprintf("%s.json", key)
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
