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
	"time"
)

// REST_URL is the base URL for the blockchain's REST API.
const REST_URL = "https://rest.unicorn.meme"

// fetchAll retrieves paginated data from an endpoint with progress indicators.
func fetchAll(endpoint, itemsKey string) ([]map[string]interface{}, error) {
	var allItems []map[string]interface{}
	nextKey := ""
	page := 1

	for {
		// Construct URL with proper query parameter encoding
		baseURL := fmt.Sprintf("%s%s", REST_URL, endpoint)
		params := url.Values{}
		params.Add("pagination.limit", "100") // Adjust limit as needed
		if nextKey != "" {
			params.Add("pagination.key", nextKey)
		}
		fullURL := fmt.Sprintf("%s?%s", baseURL, params.Encode())

		fmt.Printf("Fetching %s page %d: %s\n", itemsKey, page, fullURL)

		// Send HTTP request
		resp, err := http.Get(fullURL)
		if err != nil {
			return nil, fmt.Errorf("network error fetching %s: %v", itemsKey, err)
		}
		defer resp.Body.Close()

		// Check for HTTP errors and provide detailed feedback
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			return nil, fmt.Errorf("HTTP error %d fetching %s: %s", resp.StatusCode, itemsKey, string(body))
		}

		// Parse JSON response
		var data map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			return nil, fmt.Errorf("JSON decode error for %s: %v", itemsKey, err)
		}

		// Extract items from response
		items, ok := data[itemsKey].([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid response format for %s", itemsKey)
		}
		for _, item := range items {
			allItems = append(allItems, item.(map[string]interface{}))
		}

		// Handle pagination
		pagination, ok := data["pagination"].(map[string]interface{})
		if !ok || pagination["next_key"] == nil {
			break
		}
		nextKey = pagination["next_key"].(string)
		page++
	}
	fmt.Printf("Completed fetching %d %s.\n", len(allItems), itemsKey)
	return allItems, nil
}

// fetchParams fetches module parameters from an endpoint with progress feedback.
func fetchParams(endpoint string) (map[string]interface{}, error) {
	fullURL := fmt.Sprintf("%s%s", REST_URL, endpoint)
	fmt.Printf("Fetching parameters from %s\n", fullURL)

	resp, err := http.Get(fullURL)
	if err != nil {
		return nil, fmt.Errorf("network error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}

	params, ok := data["params"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid params format")
	}
	fmt.Printf("Successfully fetched parameters from %s\n", endpoint)
	return params, nil
}

// fetchBalances retrieves balances for an account with progress indicators.
func fetchBalances(address string, index, total int) ([]map[string]string, error) {
	url := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", REST_URL, address)
	fmt.Printf("Fetching balances for account %d/%d: %s\n", index+1, total, address)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("network error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	var data struct {
		Balances []map[string]string `json:"balances"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("JSON decode error: %v", err)
	}
	return data.Balances, nil
}

func main() {
	// Step 1: Fetch all accounts
	fmt.Println("Starting snapshot process...")
	fmt.Println("Step 1: Fetching all accounts...")
	accounts, err := fetchAll("/cosmos/auth/v1beta1/accounts", "accounts")
	if err != nil {
		log.Fatalf("Failed to fetch accounts: %v", err)
	}

	// Adjust BaseAccounts for genesis
	for _, account := range accounts {
		if account["@type"] == "/cosmos.auth.v1beta1.BaseAccount" {
			account["account_number"] = "0"
			account["sequence"] = "0"
		}
	}

	// Step 2: Fetch balances and calculate supply
	fmt.Println("Step 2: Fetching account balances and calculating supply...")
	balances := []map[string]interface{}{}
	supply := make(map[string]*big.Int)
	for i, account := range accounts {
		address, ok := account["address"].(string)
		if !ok {
			fmt.Printf("Warning: Skipping account %d due to missing address\n", i+1)
			continue
		}
		balResp, err := fetchBalances(address, i, len(accounts))
		if err != nil {
			fmt.Printf("Warning: Failed to fetch balances for %s: %v\n", address, err)
			continue
		}
		for _, coin := range balResp {
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
		balances = append(balances, map[string]interface{}{
			"address": address,
			"coins":   balResp,
		})
	}
	fmt.Printf("Collected balances for %d accounts.\n", len(balances))

	// Step 3: Fetch additional module data
	fmt.Println("Step 3: Fetching additional module data...")

	// Auth parameters
	authParams, err := fetchParams("/cosmos/auth/v1beta1/params")
	if err != nil {
		fmt.Printf("Warning: Using default auth params: %v\n", err)
		authParams = map[string]interface{}{
			"max_memo_characters":     "256",
			"tx_sig_limit":            "7",
			"tx_size_cost_per_byte":   "10",
			"sig_verify_cost_ed25519": "590",
			"sig_verify_cost_secp256k1": "1000",
		}
	}

	// Bank parameters
	bankParams, err := fetchParams("/cosmos/bank/v1beta1/params")
	if err != nil {
		fmt.Printf("Warning: Using default bank params: %v\n", err)
		bankParams = map[string]interface{}{
			"send_enabled":        []interface{}{},
			"default_send_enabled": true,
		}
	}

	// Denomination metadata
	denomMetadata, err := fetchAll("/cosmos/bank/v1beta1/denoms_metadata", "metadatas")
	if err != nil {
		fmt.Printf("Warning: No denom metadata fetched: %v\n", err)
		denomMetadata = []map[string]interface{}{}
	}

	// Staking parameters
	stakingParams, err := fetchParams("/cosmos/staking/v1beta1/params")
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

	// Validators
	validators, err := fetchAll("/cosmos/staking/v1beta1/validators", "validators")
	if err != nil {
		fmt.Printf("Warning: No validators fetched: %v\n", err)
		validators = []map[string]interface{}{}
	}

	// Delegations
	delegations, err := fetchAll("/cosmos/staking/v1beta1/delegations", "delegation_responses")
	if err != nil {
		fmt.Printf("Warning: No delegations fetched: %v\n", err)
		delegations = []map[string]interface{}{}
	}

	// Distribution parameters
	distParams, err := fetchParams("/cosmos/distribution/v1beta1/params")
	if err != nil {
		fmt.Printf("Warning: Using default distribution params: %v\n", err)
		distParams = map[string]interface{}{
			"community_tax":         "0.020000000000000000",
			"base_proposer_reward":  "0.010000000000000000",
			"bonus_proposer_reward": "0.040000000000000000",
			"withdraw_addr_enabled": true,
		}
	}

	// Governance parameters
	govParams, err := fetchParams("/cosmos/gov/v1beta1/params")
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

	// Step 4: Construct the genesis file
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

	// Populate supply in genesis
	supplyList := []map[string]string{}
	for denom, amount := range supply {
		supplyList = append(supplyList, map[string]string{
			"denom":  denom,
			"amount": amount.String(),
		})
	}
	genesis["app_state"].(map[string]interface{})["bank"].(map[string]interface{})["supply"] = supplyList

	// Step 5: Save to file
	fmt.Println("Step 5: Saving to genesis.json...")
	genesisBytes, err := json.MarshalIndent(genesis, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal genesis: %v", err)
	}
	if err := os.WriteFile("genesis.json", genesisBytes, 0644); err != nil {
		log.Fatalf("Failed to write genesis.json: %v", err)
	}

	fmt.Println("Snapshot completed successfully! Check genesis.json.")
}
