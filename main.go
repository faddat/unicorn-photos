package main

import (
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

// loadState loads or initializes the state.
func loadState() (*State, error) {
	if _, err := os.Stat("state.json.tmp"); os.IsNotExist(err) {
		return &State{CompletedEndpoints: make(map[string]string)}, nil
	}
	data, err := ioutil.ReadFile("state.json.tmp")
	if err != nil {
		return nil, fmt.Errorf("failed to read state: %v", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state: %v", err)
	}
	return &state, nil
}

// saveState saves the state to a temporary file.
func saveState(state *State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}
	return ioutil.WriteFile("state.json.tmp", data, 0644)
}

// fetchWithRetry performs an HTTP GET with retries.
func fetchWithRetry(url string, blockHeight int64) (*http.Response, error) {
	const maxRetries = 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		client := &http.Client{Timeout: 10 * time.Second}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}
		if blockHeight > 0 {
			req.Header.Add("x-cosmos-block-height", fmt.Sprintf("%d", blockHeight))
		}
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		if attempt < maxRetries {
			time.Sleep(time.Second << uint(attempt)) // Exponential backoff
		}
	}
	return nil, fmt.Errorf("exhausted retries for %s", url)
}

// fetchAll fetches paginated data using next-key pagination with a worker pool.
func fetchAll(endpoint, itemsKey string, state *State, workers int) ([]map[string]interface{}, error) {
	const pageSize = 100
	rateLimit := time.NewTicker(time.Millisecond * 2) // 500 req/s
	defer rateLimit.Stop()

	jobs := make(chan string, workers*2)
	results := make(chan fetchResult, workers*2)
	var wg sync.WaitGroup

	// Load cached data
	cacheFile := fmt.Sprintf("%s.json.tmp", itemsKey)
	var allItems []map[string]interface{}
	if data, err := ioutil.ReadFile(cacheFile); err == nil {
		if err := json.Unmarshal(data, &allItems); err == nil {
			fmt.Printf("Loaded %d cached %s\n", len(allItems), itemsKey)
		}
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for nextKey := range jobs {
				<-rateLimit.C
				params := url.Values{}
				params.Add("pagination.limit", fmt.Sprintf("%d", pageSize))
				if nextKey != "" {
					params.Add("pagination.key", nextKey)
				}
				url := fmt.Sprintf("%s%s?%s", REST_URL, endpoint, params.Encode())

				resp, err := fetchWithRetry(url, state.BlockHeight)
				if err != nil {
					results <- fetchResult{Err: err}
					continue
				}
				defer resp.Body.Close()

				var data map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					results <- fetchResult{Err: err}
					continue
				}

				items, ok := data[itemsKey].([]interface{})
				if !ok {
					results <- fetchResult{Err: fmt.Errorf("invalid %s format", itemsKey)}
					continue
				}

				pageItems := make([]map[string]interface{}, len(items))
				for i, item := range items {
					pageItems[i] = item.(map[string]interface{})
				}

				nextKeyVal, _ := data["pagination"].(map[string]interface{})["next_key"].(string)
				results <- fetchResult{Items: pageItems, NextKey: nextKeyVal}
			}
		}()
	}

	// Start with initial key from state
	nextKey := state.CompletedEndpoints[endpoint]
	jobs <- nextKey
	activeJobs := 1

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	seen := make(map[string]bool)
	for _, item := range allItems {
		if addr, ok := item["address"].(string); ok {
			seen[addr] = true
		}
	}

	for result := range results {
		if result.Err != nil {
			log.Printf("Warning: Fetch %s failed: %v", itemsKey, result.Err)
			continue
		}
		for _, item := range result.Items {
			if addr, ok := item["address"].(string); ok && !seen[addr] {
				seen[addr] = true
				allItems = append(allItems, item)
			}
		}
		if result.NextKey != "" {
			jobs <- result.NextKey
			activeJobs++
		} else {
			activeJobs--
		}
		if activeJobs > 0 {
			state.CompletedEndpoints[endpoint] = result.NextKey
			dataBytes, _ := json.MarshalIndent(allItems, "", "  ")
			ioutil.WriteFile(cacheFile, dataBytes, 0644)
			saveState(state)
		}
	}
	close(jobs)

	if activeJobs == 0 {
		delete(state.CompletedEndpoints, endpoint)
		saveState(state)
	}
	fmt.Printf("Fetched %d %s\n", len(allItems), itemsKey)
	return allItems, nil
}

// fetchBalances fetches balances for all accounts in parallel.
func fetchBalances(accounts []map[string]interface{}, state *State, workers int) ([]map[string]interface{}, error) {
	jobs := make(chan struct {
		Index   int
		Address string
	}, workers*2)
	results := make(chan fetchResult, workers*2)
	var wg sync.WaitGroup
	rateLimit := time.NewTicker(time.Millisecond * 2)
	defer rateLimit.Stop()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				<-rateLimit.C
				url := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", REST_URL, job.Address)
				resp, err := fetchWithRetry(url, state.BlockHeight)
				if err != nil {
					results <- fetchResult{Err: err}
					continue
				}
				defer resp.Body.Close()

				var data struct {
					Balances []map[string]interface{} `json:"balances"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					results <- fetchResult{Err: err}
					continue
				}
				for _, bal := range data.Balances {
					bal["address"] = job.Address
				}
				results <- fetchResult{Items: data.Balances}
			}
		}()
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
	}()

	// Collect results
	go func() {
		wg.Wait()
		close(results)
	}()

	var balances []map[string]interface{}
	for result := range results {
		if result.Err != nil {
			log.Printf("Warning: Fetch balance failed: %v", result.Err)
			continue
		}
		balances = append(balances, result.Items...)
	}
	return balances, nil
}

// fetchParams fetches module parameters.
func fetchParams(endpoint string, state *State) (map[string]interface{}, error) {
	cacheFile := fmt.Sprintf("%s_params.json.tmp", endpoint[1:])
	if _, err := os.Stat(cacheFile); err == nil {
		data, err := ioutil.ReadFile(cacheFile)
		if err == nil {
			var params map[string]interface{}
			if json.Unmarshal(data, &params) == nil {
				fmt.Printf("Loaded %s params from cache\n", endpoint)
				return params, nil
			}
		}
	}

	url := fmt.Sprintf("%s%s", REST_URL, endpoint)
	resp, err := fetchWithRetry(url, state.BlockHeight)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	params, _ := data["params"].(map[string]interface{})
	dataBytes, _ := json.MarshalIndent(params, "", "  ")
	ioutil.WriteFile(cacheFile, dataBytes, 0644)
	return params, nil
}

// convertAddress converts a Bech32 address to a new prefix.
func convertAddress(addr, oldPrefix, newPrefix string) (string, error) {
	hrp, data, err := bech32.Decode(addr)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(hrp, oldPrefix) {
		return addr, nil // Skip if prefix doesn’t match
	}
	suffix := strings.TrimPrefix(hrp, oldPrefix)
	newHRP := newPrefix + suffix
	newAddr, err := bech32.Encode(newHRP, data)
	if err != nil {
		return "", err
	}
	return newAddr, nil
}

// convertAddresses updates all Bech32 addresses in the app state.
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

// getLatestBlockHeight fetches the latest block height.
func getLatestBlockHeight() (int64, error) {
	url := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/blocks/latest", REST_URL)
	resp, err := fetchWithRetry(url, 0)
	if err != nil {
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
		return 0, err
	}
	return strconv.ParseInt(data.Block.Header.Height, 10, 64)
}

func main() {
	// Parse flags
	var newPrefix string
	flag.StringVar(&newPrefix, "prefix", "", "New Bech32 prefix for addresses (e.g., cosmos)")
	flag.Parse()

	// Get block height
	blockHeight, err := getLatestBlockHeight()
	if err != nil {
		log.Fatalf("Failed to get block height: %v", err)
	}
	fmt.Printf("Snapshotting at block %d\n", blockHeight)

	// Load state
	state, err := loadState()
	if err != nil {
		log.Fatalf("Failed to load state: %v", err)
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
			items, err := fetchAll(e.endpoint, e.key, state, 50)
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
			p, err := fetchParams(e, state)
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
		log.Printf("Error: %v", err)
	}

	// Fetch balances after accounts
	accounts := data["accounts"]
	if len(accounts) > 0 {
		balances, err := fetchBalances(accounts, state, 50)
		if err != nil {
			log.Printf("Warning: Balances fetch failed: %v", err)
		}
		data["balances"] = balances
	}

	// Construct genesis
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
		oldPrefix := "sei" // Default for Sei-based chain
		if len(accounts) > 0 {
			if addr, ok := accounts[0]["address"].(string); ok {
				if hrp, _, err := bech32.Decode(addr); err == nil {
					oldPrefix = hrp
				}
			}
		}
		convertAddresses(genesis["app_state"].(map[string]interface{}), oldPrefix, newPrefix)
	}

	// Write genesis file
	genesisBytes, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal genesis: %v", err)
	}
	if err := ioutil.WriteFile("genesis.json.tmp", genesisBytes, 0644); err != nil {
		log.Fatalf("Failed to write genesis.json.tmp: %v", err)
	}

	// Rename temporary files to final names
	for _, key := range append([]string{"accounts", "balances", "validators", "delegation_responses", "metadatas"}, paramEndpoints...) {
		tmp := fmt.Sprintf("%s.json.tmp", key)
		final := fmt.Sprintf("%s.json", key)
		if _, err := os.Stat(tmp); err == nil {
			os.Rename(tmp, final)
		}
	}
	os.Rename("state.json.tmp", "state.json")
	os.Rename("genesis.json.tmp", "genesis.json")
	fmt.Println("Snapshot completed successfully!")
}
