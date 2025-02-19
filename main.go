package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	Timeout: 10 * time.Second,
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

// Add this function to check if we need to update balances
func shouldUpdateBalances(oldHeight, newHeight int64) bool {
	return oldHeight != newHeight
}

func main() {
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
		logger.Printf("Block height changed from %d to %d, will update balances",
			state.BlockHeight, latestHeight)
		// Remove existing balance files to force refresh
		os.Remove("balances.json")
		os.Remove("balances.json.tmp")
	}

	state.BlockHeight = latestHeight
	if err := saveState(state, false); err != nil { // Save to state.json immediately
		logger.Printf("Failed to save state: %v", err)
	}

	// Fetch data
	data := make(map[string][]map[string]interface{})

	// Fetch accounts via REST
	accountsURL := "https://rest.unicorn.meme/cosmos/auth/v1beta1/accounts"
	var allAccounts []map[string]interface{}
	nextKey := ""
	pageSize := 500

	if valid, lastNum := validateAccounts("accounts.json"); valid {
		logger.Printf("Using existing accounts.json with %d accounts", lastNum+1)
		data, err := os.ReadFile("accounts.json")
		if err != nil {
			logger.Fatalf("Failed to read accounts.json: %v", err)
		}
		if err := json.Unmarshal(data, &allAccounts); err != nil {
			logger.Fatalf("Failed to parse accounts.json: %v", err)
		}
	} else {
		// Proceed with fetching accounts...
		for {
			url := accountsURL
			if nextKey != "" {
				url = fmt.Sprintf("%s?pagination.key=%s&pagination.limit=%d", accountsURL, nextKey, pageSize)
			} else {
				url = fmt.Sprintf("%s?pagination.limit=%d", accountsURL, pageSize)
			}

			resp, err := httpClient.Get(url)
			if err != nil {
				logger.Printf("Failed to fetch accounts: %v", err)
				break
			}

			var result struct {
				Accounts   []map[string]interface{} `json:"accounts"`
				Pagination struct {
					NextKey string `json:"next_key"`
					Total   string `json:"total"`
				} `json:"pagination"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				resp.Body.Close()
				logger.Printf("Failed to decode accounts: %v", err)
				break
			}
			resp.Body.Close()

			allAccounts = append(allAccounts, result.Accounts...)
			logger.Printf("Fetched %d accounts (total: %d, expected: %s)",
				len(result.Accounts), len(allAccounts), result.Pagination.Total)

			// Save progress periodically
			if len(allAccounts)%1000 == 0 {
				if err := writeJSONFile("accounts.json", allAccounts); err != nil {
					logger.Printf("Failed to save progress: %v", err)
				}
			}

			if result.Pagination.NextKey == "" {
				break
			}
			nextKey = result.Pagination.NextKey
		}
	}

	data["accounts"] = allAccounts
	data["balances"], _ = fetchRPC("/cosmos.bank.v1beta1/balances", "balances.json.tmp", latestHeight)
	data["validators"], _ = fetchValidators(latestHeight)
	data["delegations"], _ = fetchRPC("/cosmos.staking.v1beta1/delegations", "delegations.json.tmp", latestHeight)
	data["denom_metadata"], _ = fetchRPC("/cosmos.bank.v1beta1/denoms_metadata", "metadatas.json.tmp", latestHeight)

	// Fetch parameters
	params := make(map[string]map[string]interface{})
	for _, module := range []string{"auth", "bank", "staking", "distribution", "gov"} {
		params[module], _ = fetchParamsRPC(module, latestHeight)
	}

	// Construct genesis
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
	convertAddresses(genesis["app_state"].(map[string]interface{}), oldPrefix, newPrefix)

	// Write genesis
	if err := writeJSONFile("genesis.json", genesis); err != nil {
		logger.Fatalf("Write genesis failed: %v", err)
	}

	// Finalize files
	for _, key := range []string{"accounts", "balances", "validators", "delegations", "metadatas"} {
		if data[key] != nil {
			if err := writeJSONFile(key+".json", data[key]); err != nil {
				logger.Printf("Write %s failed: %v", key, err)
			}
		}
	}

	// Write params files
	for module, moduleParams := range params {
		if err := writeJSONFile(module+"_params.json", moduleParams); err != nil {
			logger.Printf("Write %s params failed: %v", module, err)
		}
	}

	// After fetching balances, save state again
	if err := saveState(state, true); err != nil {
		logger.Printf("Failed to save final state: %v", err)
	}

	logger.Println("Snapshot completed!")
	fmt.Println("Snapshot completed!")
}
