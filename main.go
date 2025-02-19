package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// REST_URL is the base URL for the blockchain's REST API.
const REST_URL = "https://rest.unicorn.meme"

// State represents the script's progress for resuming.
type State struct {
	LastAccountPage    int      `json:"last_account_page"`
	AccountNextKey     string   `json:"account_next_key"`
	CompletedAccounts  bool     `json:"completed_accounts"`
	LastBalanceIndex   int      `json:"last_balance_index"`
	CompletedBalances  bool     `json:"completed_balances"`
	CompletedSnapshots []string `json:"completed_snapshots"`
	BlockHeight        int64    `json:"block_height"`
}

// loadState loads the current state or initializes a new one.
func loadState() (*State, error) {
	if _, err := os.Stat("state.json"); os.IsNotExist(err) {
		return &State{LastAccountPage: 0, LastBalanceIndex: -1}, nil
	}
	data, err := ioutil.ReadFile("state.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read state: %v", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state: %v", err)
	}
	return &state, nil
}

// saveState saves the current state to disk.
func saveState(state *State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %v", err)
	}
	return ioutil.WriteFile("state.json", data, 0644)
}

// fetchWithRetry performs an HTTP GET with retries on failure.
func fetchWithRetry(url string, blockHeight int64, maxRetries int) (*http.Response, error) {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		client := &http.Client{}
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
			fmt.Printf("Attempt %d failed: HTTP %d - %s\n", attempt+1, resp.StatusCode, url)
		} else {
			fmt.Printf("Attempt %d failed: %v - %s\n", attempt+1, err, url)
		}
		if attempt < maxRetries {
			delay := time.Duration(1<<uint(attempt)) * time.Second
			fmt.Printf("Retrying in %v...\n", delay)
			time.Sleep(delay)
		}
	}
	return nil, fmt.Errorf("exhausted retries for %s", url)
}

// fetchAll retrieves paginated data with resuming capability.
func fetchAll(endpoint, itemsKey string, state *State) ([]map[string]interface{}, error) {
	var existingItems []map[string]interface{}
	var lastAccountNum int64 = -1

	// Load existing accounts if any
	if _, err := os.Stat(itemsKey + ".json"); err == nil {
		data, err := os.ReadFile(itemsKey + ".json")
		if err == nil {
			if err := json.Unmarshal(data, &existingItems); err == nil {
				fmt.Printf("Loaded %d existing %s from cache.\n", len(existingItems), itemsKey)
				// Find highest account number
				for _, item := range existingItems {
					if itemsKey == "accounts" {
						if num, ok := item["account_number"].(string); ok {
							if n, err := strconv.ParseInt(num, 10, 64); err == nil && n > lastAccountNum {
								lastAccountNum = n
							}
						}
					}
				}
			}
		}
	}

	// If we're fetching accounts and have existing ones, modify the endpoint to start after the last account
	baseURL := fmt.Sprintf("%s%s", REST_URL, endpoint)
	var fullURL string
	if itemsKey == "accounts" && lastAccountNum >= 0 {
		// Remove the pagination parameters from subsequent requests since we're using account_number_gt
		baseURL = fmt.Sprintf("%s/cosmos/auth/v1beta1/accounts?account_number_gt=%d&pagination.limit=100", REST_URL, lastAccountNum)

		// Don't append additional pagination parameters for accounts
		fullURL = baseURL
	} else {
		params := url.Values{}
		params.Add("pagination.limit", "100")
		fullURL = fmt.Sprintf("%s?%s", baseURL, params.Encode())
	}

	resp, err := fetchWithRetry(fullURL, state.BlockHeight, 3)
	if err != nil {
		if itemsKey == "accounts" && len(existingItems) > 0 {
			fmt.Printf("Failed to fetch new accounts, using %d existing accounts\n", len(existingItems))
			return existingItems, nil
		}
		return nil, fmt.Errorf("failed to fetch first page: %v", err)
	}
	defer resp.Body.Close()

	var firstPage map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&firstPage); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	pagination, ok := firstPage["pagination"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid pagination format")
	}

	var total int
	switch t := pagination["total"].(type) {
	case float64:
		total = int(t)
	case string:
		totalInt, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid total format: %v", err)
		}
		total = int(totalInt)
	default:
		return nil, fmt.Errorf("unexpected total type: %T", pagination["total"])
	}

	if total == 0 {
		// No new items to fetch, return existing items
		fmt.Printf("No new %s to fetch\n", itemsKey)
		return existingItems, nil
	}

	pageSize := 100
	numPages := (total + pageSize - 1) / pageSize

	// Create worker pool for parallel fetching
	type pageResult struct {
		items []map[string]interface{}
		page  int
		err   error
	}

	workers := 50
	jobs := make(chan int, numPages)
	results := make(chan pageResult, numPages)
	rateLimit := time.NewTicker(time.Millisecond * 20)
	defer rateLimit.Stop()

	// Start workers
	for w := 0; w < workers; w++ {
		go func() {
			for page := range jobs {
				<-rateLimit.C
				var pageURL string
				if itemsKey == "accounts" {
					pageURL = fmt.Sprintf("%s/cosmos/auth/v1beta1/accounts?account_number_gt=%d&pagination.limit=%d",
						REST_URL, lastAccountNum+(int64(page)*int64(pageSize)), pageSize)
				} else {
					pageURL = fmt.Sprintf("%s?pagination.limit=%d&pagination.offset=%d",
						baseURL, pageSize, page*pageSize)
				}

				resp, err := fetchWithRetry(pageURL, state.BlockHeight, 3)
				if err != nil {
					results <- pageResult{page: page, err: err}
					continue
				}

				var data map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
					resp.Body.Close()
					results <- pageResult{page: page, err: err}
					continue
				}
				resp.Body.Close()

				items, ok := data[itemsKey].([]interface{})
				if !ok {
					results <- pageResult{page: page, err: fmt.Errorf("invalid response format")}
					continue
				}

				pageItems := make([]map[string]interface{}, len(items))
				for i, item := range items {
					pageItems[i] = item.(map[string]interface{})
				}
				results <- pageResult{items: pageItems, page: page}
			}
		}()
	}

	// Send jobs
	go func() {
		for i := 0; i < numPages; i++ {
			jobs <- i
		}
		close(jobs)
	}()

	// Collect results
	allItems := make([]map[string]interface{}, 0, total)
	processed := 0
	for result := range results {
		processed++
		if result.err != nil {
			fmt.Printf("Warning: Failed to fetch page %d: %v\n", result.page, result.err)
			continue
		}
		allItems = append(allItems, result.items...)

		if processed%10 == 0 || processed == numPages {
			fmt.Printf("Fetched %d/%d pages of %s\n", processed, numPages, itemsKey)
			// Cache intermediate results
			dataBytes, _ := json.MarshalIndent(allItems, "", "  ")
			os.WriteFile(itemsKey+".json", dataBytes, 0644)
		}

		if processed >= numPages {
			break
		}
	}

	// When collecting results, append to existing items
	allItems = append(existingItems, allItems...)

	state.CompletedAccounts = true
	saveState(state)

	fmt.Printf("Completed fetching %d %s.\n", len(allItems), itemsKey)
	return allItems, nil
}

// fetchParams fetches module parameters with retry logic.
func fetchParams(endpoint string, state *State) (map[string]interface{}, error) {
	if contains(state.CompletedSnapshots, endpoint) {
		data, err := ioutil.ReadFile(endpoint[1:] + "_params.json")
		if err == nil {
			var params map[string]interface{}
			if err := json.Unmarshal(data, &params); err == nil {
				fmt.Printf("Loaded parameters from %s from cache.\n", endpoint)
				return params, nil
			}
		}
	}

	fullURL := fmt.Sprintf("%s%s", REST_URL, endpoint)
	fmt.Printf("Fetching parameters from %s\n", fullURL)

	resp, err := fetchWithRetry(fullURL, state.BlockHeight, 3)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch params after retries: %v", err)
	}
	defer resp.Body.Close()

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	params, ok := data["params"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid params format")
	}

	// Cache and mark as completed
	dataBytes, _ := json.MarshalIndent(params, "", "  ")
	os.WriteFile(endpoint[1:]+"_params.json", dataBytes, 0644)
	state.CompletedSnapshots = append(state.CompletedSnapshots, endpoint)
	saveState(state)

	fmt.Printf("Successfully fetched parameters from %s\n", endpoint)
	return params, nil
}

// fetchBalances retrieves balances with resuming capability.
func fetchBalances(address string, index, total int, state *State) ([]map[string]string, error) {
	url := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", REST_URL, address)
	if index <= state.LastBalanceIndex {
		return nil, nil // Skip already processed accounts
	}

	fmt.Printf("Fetching balances for account %d/%d: %s\n", index+1, total, address)
	resp, err := fetchWithRetry(url, state.BlockHeight, 3)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch balances after retries: %v", err)
	}
	defer resp.Body.Close()

	var data struct {
		Balances []map[string]string `json:"balances"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}
	return data.Balances, nil
}

// contains checks if a slice contains a string.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// getLatestBlockHeight retrieves the latest block height from the blockchain.
func getLatestBlockHeight() (int64, error) {
	url := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/blocks/latest", REST_URL)
	resp, err := fetchWithRetry(url, 0, 3)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch latest block: %v", err)
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
		return 0, fmt.Errorf("failed to decode block response: %v", err)
	}

	return strconv.ParseInt(data.Block.Header.Height, 10, 64)
}

func main() {
	// Get latest block height
	blockHeight, err := getLatestBlockHeight()
	if err != nil {
		log.Fatalf("Failed to get latest block height: %v", err)
	}
	fmt.Printf("Starting snapshot process at block height %d...\n", blockHeight)

	state, err := loadState()
	if err != nil {
		log.Fatalf("Failed to load state: %v", err)
	}
	state.BlockHeight = blockHeight

	// Step 1: Fetch all accounts
	fmt.Println("Step 1: Fetching all accounts...")
	accounts, err := fetchAll("/cosmos/auth/v1beta1/accounts", "accounts", state)
	if err != nil {
		log.Fatalf("Failed to fetch accounts: %v", err)
	}
	for _, account := range accounts {
		if account["@type"] == "/cosmos.auth.v1beta1.BaseAccount" {
			account["account_number"] = "0"
			account["sequence"] = "0"
		}
	}

	// Step 2: Fetch balances and calculate supply
	fmt.Println("Step 2: Fetching account balances and calculating supply...")
	balances := []map[string]interface{}{}
	if _, err := os.Stat("balances.json"); err == nil && state.CompletedBalances {
		data, _ := ioutil.ReadFile("balances.json")
		json.Unmarshal(data, &balances)
		fmt.Printf("Loaded %d balances from cache.\n", len(balances))
	}

	type balanceResult struct {
		index   int
		address string
		coins   []map[string]string
		err     error
	}

	// Create channels for worker coordination
	numWorkers := 1600 // Increased from 10 to 50 workers
	jobs := make(chan int, len(accounts))
	results := make(chan balanceResult, len(accounts))
	var mu sync.Mutex

	// Create rate limiter to prevent overwhelming the API
	rateLimit := time.NewTicker(time.Microsecond * 5000) // 20 requests per second
	defer rateLimit.Stop()

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		go func() {
			for i := range jobs {
				if state.CompletedBalances {
					continue
				}
				<-rateLimit.C // Rate limit our requests

				account := accounts[i]
				address, ok := account["address"].(string)
				if !ok {
					results <- balanceResult{index: i, err: fmt.Errorf("no address")}
					continue
				}

				// Batch save every 100 records instead of every single one
				balResp, err := fetchBalances(address, i, len(accounts), state)
				results <- balanceResult{
					index:   i,
					address: address,
					coins:   balResp,
					err:     err,
				}
			}
		}()
	}

	// Send jobs to workers
	go func() {
		for i := range accounts {
			if i <= state.LastBalanceIndex {
				continue
			}
			jobs <- i
		}
		close(jobs)
	}()

	// Process results with batched writes
	const batchSize = 100
	batchedBalances := make([]map[string]interface{}, 0, batchSize)
	lastSave := time.Now()

	// Process results
	supply := make(map[string]*big.Int)
	processed := 0
	expected := len(accounts) - state.LastBalanceIndex - 1
	if expected > 0 {
		for result := range results {
			processed++
			if result.err != nil {
				fmt.Printf("Warning: Failed to fetch balances for account %d: %v\n", result.index+1, result.err)
				continue
			}
			if result.coins == nil {
				continue
			}

			mu.Lock()
			for _, coin := range result.coins {
				denom := coin["denom"]
				amountStr := coin["amount"]
				amount, ok := new(big.Int).SetString(amountStr, 10)
				if !ok {
					fmt.Printf("Warning: Invalid amount for %s: %s\n", denom, amountStr)
					continue
				}
				if _, exists := supply[denom]; !exists {
					supply[denom] = new(big.Int)
				}
				supply[denom].Add(supply[denom], amount)
			}

			batchedBalances = append(batchedBalances, map[string]interface{}{
				"address": result.address,
				"coins":   result.coins,
			})

			// Save state and balances every 100 records or after 5 seconds
			if len(batchedBalances) >= batchSize || time.Since(lastSave) > 5*time.Second {
				balances = append(balances, batchedBalances...)
				state.LastBalanceIndex = result.index
				saveState(state)
				dataBytes, _ := json.MarshalIndent(balances, "", "  ")
				os.WriteFile("balances.json", dataBytes, 0644)
				batchedBalances = batchedBalances[:0]
				lastSave = time.Now()
			}
			mu.Unlock()

			if processed >= expected {
				break
			}
		}

		// Save any remaining balances
		if len(batchedBalances) > 0 {
			mu.Lock()
			balances = append(balances, batchedBalances...)
			dataBytes, _ := json.MarshalIndent(balances, "", "  ")
			os.WriteFile("balances.json", dataBytes, 0644)
			mu.Unlock()
		}
	}

	state.CompletedBalances = true
	saveState(state)
	fmt.Printf("Collected balances for %d accounts.\n", len(balances))

	// Step 3: Fetch additional module data
	fmt.Println("Step 3: Fetching additional module data...")
	authParams, err := fetchParams("/cosmos/auth/v1beta1/params", state)
	if err != nil {
		fmt.Printf("Warning: Using default auth params: %v\n", err)
		authParams = map[string]interface{}{
			"max_memo_characters":       "256",
			"tx_sig_limit":              "7",
			"tx_size_cost_per_byte":     "10",
			"sig_verify_cost_ed25519":   "590",
			"sig_verify_cost_secp256k1": "1000",
		}
	}

	bankParams, err := fetchParams("/cosmos/bank/v1beta1/params", state)
	if err != nil {
		fmt.Printf("Warning: Using default bank params: %v\n", err)
		bankParams = map[string]interface{}{
			"send_enabled":         []interface{}{},
			"default_send_enabled": true,
		}
	}

	denomMetadata, err := fetchAll("/cosmos/bank/v1beta1/denoms_metadata", "metadatas", state)
	if err != nil {
		fmt.Printf("Warning: No denom metadata fetched: %v\n", err)
		denomMetadata = []map[string]interface{}{}
	}

	stakingParams, err := fetchParams("/cosmos/staking/v1beta1/params", state)
	if err != nil {
		fmt.Printf("Warning: Using default staking params: %v\n", err)
		stakingParams = map[string]interface{}{
			"unbonding_time":     "1814400s",
			"max_validators":     100,
			"max_entries":        7,
			"historical_entries": 10000,
			"bond_denom":         "usei",
		}
	}

	validators, err := fetchAll("/cosmos/staking/v1beta1/validators", "validators", state)
	if err != nil {
		fmt.Printf("Warning: No validators fetched: %v\n", err)
		validators = []map[string]interface{}{}
	}

	delegations, err := fetchAll("/cosmos/staking/v1beta1/delegations", "delegation_responses", state)
	if err != nil {
		fmt.Printf("Warning: No delegations fetched: %v\n", err)
		delegations = []map[string]interface{}{}
	}

	distParams, err := fetchParams("/cosmos/distribution/v1beta1/params", state)
	if err != nil {
		fmt.Printf("Warning: Using default distribution params: %v\n", err)
		distParams = map[string]interface{}{
			"community_tax":         "0.020000000000000000",
			"base_proposer_reward":  "0.010000000000000000",
			"bonus_proposer_reward": "0.040000000000000000",
			"withdraw_addr_enabled": true,
		}
	}

	govParams, err := fetchParams("/cosmos/gov/v1beta1/params", state)
	if err != nil {
		fmt.Printf("Warning: Using default governance params: %v\n", err)
		govParams = map[string]interface{}{
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
	}

	// Step 4: Construct genesis file
	fmt.Println("Step 4: Constructing genesis.json...")
	genesis := map[string]interface{}{
		"genesis_time":   time.Now().UTC().Format(time.RFC3339),
		"chain_id":       "unicorn-snapshot",
		"initial_height": "1",
		"consensus_params": map[string]interface{}{
			"block": map[string]interface{}{
				"max_bytes": "22020096",
				"max_gas":   "-1",
			},
			"evidence": map[string]interface{}{
				"max_age_num_blocks": "100000",
				"max_age_duration":   "172800000000000",
				"max_bytes":          "1048576",
			},
			"validator": map[string]interface{}{
				"pub_key_types": []string{"ed25519"},
			},
			"version": map[string]interface{}{},
		},
		"app_hash": "",
		"app_state": map[string]interface{}{
			"auth": map[string]interface{}{
				"params":   authParams,
				"accounts": accounts,
			},
			"bank": map[string]interface{}{
				"params":         bankParams,
				"balances":       balances,
				"supply":         []map[string]string{},
				"denom_metadata": denomMetadata,
			},
			"staking": map[string]interface{}{
				"params":                stakingParams,
				"last_total_power":      "0",
				"last_validator_powers": []interface{}{},
				"validators":            validators,
				"delegations":           delegations,
				"unbonding_delegations": []interface{}{},
				"redelegations":         []interface{}{},
				"exported":              false,
			},
			"distribution": map[string]interface{}{
				"params": distParams,
				"fee_pool": map[string]interface{}{
					"community_pool": []interface{}{},
				},
				"delegator_withdraw_infos":          []interface{}{},
				"previous_proposer":                 "",
				"outstanding_rewards":               []interface{}{},
				"validator_accumulated_commissions": []interface{}{},
				"validator_historical_rewards":      []interface{}{},
				"validator_current_rewards":         []interface{}{},
				"delegator_starting_infos":          []interface{}{},
				"validator_slash_events":            []interface{}{},
			},
			"gov": map[string]interface{}{
				"starting_proposal_id": "1",
				"deposits":             nil,
				"votes":                nil,
				"proposals":            nil,
				"deposit_params":       govParams["deposit_params"],
				"voting_params":        govParams["voting_params"],
				"tally_params":         govParams["tally_params"],
			},
		},
	}

	// Populate supply
	supplyList := []map[string]string{}
	for denom, amount := range supply {
		supplyList = append(supplyList, map[string]string{
			"denom":  denom,
			"amount": amount.String(),
		})
	}
	genesis["app_state"].(map[string]interface{})["bank"].(map[string]interface{})["supply"] = supplyList

	// Step 5: Save genesis file
	fmt.Println("Step 5: Saving to genesis.json...")
	genesisBytes, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal genesis: %v", err)
	}
	if err := os.WriteFile("genesis.json", genesisBytes, 0644); err != nil {
		log.Fatalf("Failed to write genesis.json: %v", err)
	}

	// Before exiting, update README.md
	readmeContent, err := os.ReadFile("README.md")
	if err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: Failed to read README.md: %v", err)
	}

	var newContent string
	if len(readmeContent) > 0 {
		// Find the last snapshot line and replace it
		lines := strings.Split(string(readmeContent), "\n")
		found := false
		for i, line := range lines {
			if strings.Contains(line, "Latest snapshot from block height") {
				lines[i] = fmt.Sprintf("Latest snapshot from block height: %d", blockHeight)
				found = true
				break
			}
		}
		if !found {
			lines = append(lines, "", fmt.Sprintf("Latest snapshot from block height: %d", blockHeight))
		}
		newContent = strings.Join(lines, "\n")
	} else {
		newContent = fmt.Sprintf("Latest snapshot from block height: %d\n", blockHeight)
	}

	if err := os.WriteFile("README.md", []byte(newContent), 0644); err != nil {
		log.Printf("Warning: Failed to update README.md: %v", err)
	}

	// Clean up state file on success
	os.Remove("state.json")
	fmt.Printf("Snapshot completed successfully at block height %d! Check genesis.json.\n", blockHeight)
}
