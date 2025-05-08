package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"
	// "path/filepath" // No longer used here
	// "os" // No longer used here
	// "strings"
)

// --- Re-usable Structs for Fetched Data ---
// (AuthParams, AuthAccount, BankParams, BalanceEntry, Coin, DenomMetadata, DenomUnit remain useful)

// Structs for different modules will be defined here.
type AuthParams struct {
	MaxMemoCharacters      string `json:"max_memo_characters"`
	TxSigLimit             string `json:"tx_sig_limit"`
	TxSizeCostPerByte      string `json:"tx_size_cost_per_byte"`
	SigVerifyCostEd25519   string `json:"sig_verify_cost_ed25519"`
	SigVerifyCostSecp256k1 string `json:"sig_verify_cost_secp256k1"`
}
type AuthAccount json.RawMessage

type BankParams struct {
	DefaultSendEnabled bool `json:"default_send_enabled"`
}
type BalanceEntry struct {
	Address string `json:"address"`
	Coins   []Coin `json:"coins"`
}
type Coin struct {
	Denom  string `json:"denom"`
	Amount string `json:"amount"`
}
type DenomMetadata struct {
	Description string      `json:"description"`
	DenomUnits  []DenomUnit `json:"denom_units"`
	Base        string      `json:"base"`
	Display     string      `json:"display"`
	Name        string      `json:"name"`
	Symbol      string      `json:"symbol"`
	URI         string      `json:"uri"`
	URIHash     string      `json:"uri_hash"`
}
type DenomUnit struct {
	Denom    string   `json:"denom"`
	Exponent uint32   `json:"exponent"`
	Aliases  []string `json:"aliases"`
}

// Consider adding structs for other module states if they have consistent structures
// e.g., Validator, Delegation, Proposal etc. Otherwise use json.RawMessage.
type Validator json.RawMessage
type Delegation json.RawMessage
type UnbondingDelegation json.RawMessage
type Proposal json.RawMessage

// --- Module State Containers ---
// These structs will hold the data fetched for each module.

type AuthModuleState struct {
	Params   *AuthParams   `json:"params,omitempty"`
	Accounts []AuthAccount `json:"accounts,omitempty"`
}

type BankModuleState struct {
	Params        *BankParams     `json:"params,omitempty"`
	Balances      []BalanceEntry  `json:"balances,omitempty"` // Balances for accounts with non-zero balance
	Supply        []Coin          `json:"supply,omitempty"`
	DenomMetadata []DenomMetadata `json:"denom_metadata,omitempty"`
}

type StakingModuleState struct {
	Params               json.RawMessage       `json:"params,omitempty"` // Use RawMessage for flexibility
	Validators           []Validator           `json:"validators,omitempty"`
	Delegations          []Delegation          `json:"delegations,omitempty"`           // Might be very large, consider fetching on demand?
	UnbondingDelegations []UnbondingDelegation `json:"unbonding_delegations,omitempty"` // Might be very large
	// Redelegations? Historical Entries?
}

type GovModuleState struct {
	Params    json.RawMessage `json:"params,omitempty"`
	Proposals []Proposal      `json:"proposals,omitempty"` // Fetch active or recent proposals? All?
	Deposits  json.RawMessage `json:"deposits,omitempty"`  // Requires proposal_id, maybe fetch per proposal?
	Votes     json.RawMessage `json:"votes,omitempty"`     // Requires proposal_id, maybe fetch per proposal?
}

// Slashing, Distribution, Mint etc. would have similar state structs.

// FetchedState represents the collection of all fetched module states.
type FetchedState struct {
	ChainID   string
	Height    int64
	Timestamp time.Time
	Auth      *AuthModuleState    `json:"auth,omitempty"`
	Bank      *BankModuleState    `json:"bank,omitempty"`
	Staking   *StakingModuleState `json:"staking,omitempty"`
	Gov       *GovModuleState     `json:"gov,omitempty"`
	// Distribution *DistrModuleState `json:"distribution,omitempty"`
	// Slashing *SlashingModuleState `json:"slashing,omitempty"`
	// Mint *MintModuleState `json:"mint,omitempty"`
	// Other modules... Add fields as needed
	Errors map[string]string `json:"errors,omitempty"` // Record errors fetching specific modules
}

// --- Fetching Logic ---

// fetchPaginatedData remains the same as before (utility function).
func fetchPaginatedData(restURL, path string, resultKey string, target interface{}) error {
	var allItems []json.RawMessage
	paginationKey := ""
	endpoint := fmt.Sprintf("%s%s", restURL, path)

	for {
		queryURL := endpoint
		params := url.Values{}
		params.Add("pagination.limit", "500")

		if paginationKey != "" {
			params.Add("pagination.key", paginationKey)
		}
		if len(params) > 0 {
			queryURL = fmt.Sprintf("%s?%s", endpoint, params.Encode())
		}

		body, err := HTTPGet(queryURL) // Using http_client.go
		if err != nil {
			return fmt.Errorf("failed to fetch from %s: %w", queryURL, err)
		}

		var rawResp map[string]json.RawMessage
		if err := json.Unmarshal(body, &rawResp); err != nil {
			return fmt.Errorf("failed to unmarshal paginated response base from %s: %w", queryURL, err)
		}

		var currentItems []json.RawMessage
		if itemsVal, ok := rawResp[resultKey]; ok {
			// Corrected line from previous error: passing pointer &currentItems
			if err := json.Unmarshal(itemsVal, &currentItems); err != nil {
				return fmt.Errorf("failed to unmarshal items for key '%s' from %s: %w", resultKey, queryURL, err)
			}
			allItems = append(allItems, currentItems...)
		} else {
			if _, pagOk := rawResp["pagination"]; !pagOk {
				// If no pagination key and no result key, maybe it wasn't paginated and resultKey was wrong?
				// Or it's just an empty response. We can't know for sure here.
				// Let's assume if resultKey is specified, it SHOULD exist on first page if there's data.
				// If it doesn't exist, maybe there are no items. Check if the target is a slice.
				// This part is tricky; requires knowing API behavior.
				// For now, return error if key is missing and no pagination.
				return fmt.Errorf("resultKey '%s' not found and no pagination info in response from %s", resultKey, queryURL)
			}
			// If pagination exists but no items key, it's likely just an empty page for this key.
		}

		var paginRaw json.RawMessage
		if pVal, ok := rawResp["pagination"]; ok {
			paginRaw = pVal
		}

		var nextKeyToUse string
		if paginRaw != nil {
			var paginData struct {
				NextKey string `json:"next_key"`
			}
			if err := json.Unmarshal(paginRaw, &paginData); err == nil {
				nextKeyToUse = paginData.NextKey
			}
		}

		if nextKeyToUse == "" {
			break
		}
		paginationKey = url.QueryEscape(nextKeyToUse)
	}

	// Combine all RawMessages into a single JSON array string then unmarshal to target
	finalJSON := []byte{'['}
	for i, item := range allItems {
		if i > 0 {
			finalJSON = append(finalJSON, ',')
		}
		finalJSON = append(finalJSON, item...)
	}
	finalJSON = append(finalJSON, ']')

	return json.Unmarshal(finalJSON, target)
}

// fetchAllModuleStates fetches state for key modules.
// It now returns a FetchedState struct containing data for each module.
func fetchAllModuleStates(restURL, chainIDStr string, currentBlockHeight int64) (*FetchedState, error) {
	logger.Printf("[%s] Fetching state dump data from REST endpoint: %s at height ~%d", chainIDStr, restURL, currentBlockHeight)

	state := &FetchedState{
		ChainID:   chainIDStr,
		Height:    currentBlockHeight,
		Timestamp: time.Now().UTC(),
		Errors:    make(map[string]string),
	}

	var wg sync.WaitGroup
	var mu sync.Mutex // Protects state struct and errors map

	// Channel to hold fetched accounts for bank balance fetching
	authAccountsChan := make(chan []AuthAccount, 1) // Buffered channel of size 1

	// --- Fetch Auth Module State ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Printf("[%s] Fetching auth module state...", chainIDStr)
		authModuleState := &AuthModuleState{}
		var fetchErr error // Use a single error variable for the goroutine

		// Fetch Params
		authParamsData, err := HTTPGet(fmt.Sprintf("%s/cosmos/auth/v1beta1/params", restURL))
		if err != nil {
			fetchErr = fmt.Errorf("auth_params: %w", err)
		} else {
			var paramsResp struct {
				Params AuthParams `json:"params"`
			}
			// Corrected line from previous error: pass pointer &paramsResp
			if err := json.Unmarshal(authParamsData, &paramsResp); err != nil {
				fetchErr = fmt.Errorf("auth_params_unmarshal: %w", err)
			} else {
				authModuleState.Params = &paramsResp.Params // Store pointer
			}
		}

		// Fetch Accounts
		var authAccounts []AuthAccount
		if err := fetchPaginatedData(restURL, "/cosmos/auth/v1beta1/accounts", "accounts", &authAccounts); err != nil {
			// Combine errors if params fetch also failed
			if fetchErr != nil {
				fetchErr = fmt.Errorf("%v; auth_accounts: %w", fetchErr, err)
			} else {
				fetchErr = fmt.Errorf("auth_accounts: %w", err)
			}
		} else {
			// Resetting sequence/number isn't needed for state dump, only for genesis
			authModuleState.Accounts = authAccounts
			logger.Printf("[%s] Fetched %d auth accounts.", chainIDStr, len(authAccounts))
		}

		mu.Lock()
		if fetchErr != nil {
			state.Errors["auth"] = fetchErr.Error()
			logger.Printf("[%s] Error fetching auth state: %v", chainIDStr, fetchErr)
			authAccountsChan <- nil // Send nil to signal error or no accounts
		} else {
			state.Auth = authModuleState
			authAccountsChan <- authAccounts // Send successfully fetched accounts
		}
		mu.Unlock()
		close(authAccountsChan) // Close channel once done
	}()

	// --- Fetch Bank Module State ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Printf("[%s] Fetching bank module state (params, supply, metadata)...", chainIDStr)
		bankModuleState := &BankModuleState{}
		var fetchErr error

		// Fetch Params
		bankParamsData, err := HTTPGet(fmt.Sprintf("%s/cosmos/bank/v1beta1/params", restURL))
		if err != nil {
			fetchErr = fmt.Errorf("bank_params: %w", err)
		} else {
			var paramsResp struct {
				Params BankParams `json:"params"`
			}
			// Corrected line from previous error: pass pointer &paramsResp
			if err := json.Unmarshal(bankParamsData, &paramsResp); err != nil {
				fetchErr = fmt.Errorf("bank_params_unmarshal: %w", err)
			} else {
				bankModuleState.Params = &paramsResp.Params // Store pointer
			}
		}

		// Fetch Supply
		var bankSupply []Coin
		if err := fetchPaginatedData(restURL, "/cosmos/bank/v1beta1/supply", "supply", &bankSupply); err != nil {
			if fetchErr != nil {
				fetchErr = fmt.Errorf("%v; bank_supply: %w", fetchErr, err)
			} else {
				fetchErr = fmt.Errorf("bank_supply: %w", err)
			}
		} else {
			bankModuleState.Supply = bankSupply
		}

		// Fetch Denom Metadata
		var bankDenomMetadata []DenomMetadata
		if err := fetchPaginatedData(restURL, "/cosmos/bank/v1beta1/denoms_metadata", "metadatas", &bankDenomMetadata); err != nil {
			if fetchErr != nil {
				fetchErr = fmt.Errorf("%v; bank_denom_metadata: %w", fetchErr, err)
			} else {
				fetchErr = fmt.Errorf("bank_denom_metadata: %w", err)
			}
		} else {
			bankModuleState.DenomMetadata = bankDenomMetadata
		}

		// Balance fetching depends on accounts and happens later sequentially

		mu.Lock()
		if fetchErr != nil {
			state.Errors["bank_core"] = fetchErr.Error() // Error for non-balance parts
			logger.Printf("[%s] Error fetching core bank state: %v", chainIDStr, fetchErr)
		}
		// Assign even if there were errors, partial data might be useful
		state.Bank = bankModuleState
		mu.Unlock()
	}()

	// --- Fetch Staking Module State (Example) ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Printf("[%s] Fetching staking module state...", chainIDStr)
		stakingModuleState := &StakingModuleState{}
		var fetchErr error

		// Fetch Params
		paramsData, err := HTTPGet(fmt.Sprintf("%s/cosmos/staking/v1beta1/params", restURL))
		if err != nil {
			fetchErr = fmt.Errorf("staking_params: %w", err)
		} else {
			// Attempt to unmarshal into a generic map first to ensure it's valid JSON
			var rawParams map[string]interface{}
			if errUnmarshal := json.Unmarshal(paramsData, &rawParams); errUnmarshal != nil {
				fetchErr = fmt.Errorf("staking_params_unmarshal: %w", errUnmarshal)
			} else {
				stakingModuleState.Params = json.RawMessage(paramsData) // Keep as raw
			}
		}

		// Fetch Validators (All statuses might be needed for a full dump)
		var validators []Validator
		// Query params might be needed e.g., status=BOND_STATUS_BONDED
		if err := fetchPaginatedData(restURL, "/cosmos/staking/v1beta1/validators", "validators", &validators); err != nil {
			if fetchErr != nil {
				fetchErr = fmt.Errorf("%v; staking_validators: %w", fetchErr, err)
			} else {
				fetchErr = fmt.Errorf("staking_validators: %w", err)
			}
		} else {
			stakingModuleState.Validators = validators
			logger.Printf("[%s] Fetched %d validators.", chainIDStr, len(validators))
		}
		// Fetching delegations/unbonding delegations can be very heavy. Omitted for brevity.
		// Consider adding flags or config options to enable/disable fetching these large datasets.

		mu.Lock()
		if fetchErr != nil {
			state.Errors["staking"] = fetchErr.Error()
			logger.Printf("[%s] Error fetching staking state: %v", chainIDStr, fetchErr)
		}
		state.Staking = stakingModuleState // Assign even if partial
		mu.Unlock()
	}()

	// --- Fetch Gov Module State (Example) ---
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Printf("[%s] Fetching gov module state...", chainIDStr)
		govModuleState := &GovModuleState{}
		var fetchErr error

		// Fetch Params (Gov params are split, fetch all needed)
		var allParamsRaw json.RawMessage
		paramsFetched := 0
		paramEndpoints := []string{"voting", "tallying", "deposit"}
		combinedParams := make(map[string]json.RawMessage)

		for _, ptype := range paramEndpoints {
			paramsData, err := HTTPGet(fmt.Sprintf("%s/cosmos/gov/v1beta1/params/%s", restURL, ptype))
			if err != nil {
				errCombined := fmt.Errorf("gov_params_%s: %w", ptype, err)
				if fetchErr != nil {
					fetchErr = fmt.Errorf("%v; %w", fetchErr, errCombined)
				} else {
					fetchErr = errCombined
				}
				continue
			}
			// Ensure valid JSON before adding
			var tempMap map[string]interface{}
			if errUnmarshal := json.Unmarshal(paramsData, &tempMap); errUnmarshal != nil {
				errCombined := fmt.Errorf("gov_params_%s_unmarshal: %w", ptype, errUnmarshal)
				if fetchErr != nil {
					fetchErr = fmt.Errorf("%v; %w", fetchErr, errCombined)
				} else {
					fetchErr = errCombined
				}
				continue
			}
			combinedParams[ptype+"_params"] = json.RawMessage(paramsData)
			paramsFetched++
		}
		if paramsFetched > 0 {
			allParamsRaw, _ = json.Marshal(combinedParams) // Marshal the map of params
			govModuleState.Params = allParamsRaw
		}

		// Fetch Proposals (Fetch all proposals, might be large)
		var proposals []Proposal
		if err := fetchPaginatedData(restURL, "/cosmos/gov/v1beta1/proposals", "proposals", &proposals); err != nil {
			if fetchErr != nil {
				fetchErr = fmt.Errorf("%v; gov_proposals: %w", fetchErr, err)
			} else {
				fetchErr = fmt.Errorf("gov_proposals: %w", err)
			}
		} else {
			govModuleState.Proposals = proposals
			logger.Printf("[%s] Fetched %d proposals.", chainIDStr, len(proposals))
			// Fetching Deposits/Votes per proposal would be very slow - omit for now
		}

		mu.Lock()
		if fetchErr != nil {
			state.Errors["gov"] = fetchErr.Error()
			logger.Printf("[%s] Error fetching gov state: %v", chainIDStr, fetchErr)
		}
		state.Gov = govModuleState // Assign even if partial
		mu.Unlock()
	}()

	// --- Add goroutines for other modules (Slashing, Distribution, Mint, IBC, WASM etc.) ---
	// Example Slashing: /cosmos/slashing/v1beta1/params, /cosmos/slashing/v1beta1/signing_infos
	// Example Distribution: /cosmos/distribution/v1beta1/params, /cosmos/distribution/v1beta1/community_pool, etc.

	wg.Wait() // Wait for all concurrent fetches to complete

	// --- Post-processing: Fetch Balances (Depends on Auth Accounts) ---
	// Retrieve accounts from the channel (waits if auth fetch isn't done)
	retrievedAuthAccounts := <-authAccountsChan

	if retrievedAuthAccounts != nil && state.Bank != nil { // Proceed only if accounts were fetched successfully
		logger.Printf("[%s] Fetching balances for %d fetched accounts...", chainIDStr, len(retrievedAuthAccounts))
		var bankBalances []BalanceEntry
		balanceFetchErrors := 0
		var balanceWg sync.WaitGroup
		balanceChan := make(chan BalanceEntry, len(retrievedAuthAccounts)) // Channel for results
		balanceErrChan := make(chan error, len(retrievedAuthAccounts))     // Channel for errors

		// Limit concurrency for balance fetching
		balanceSemaphore := make(chan struct{}, 20) // Limit to 20 concurrent balance fetches

		for _, accRaw := range retrievedAuthAccounts {
			var acc struct {
				Address string `json:"address"`
			}
			if err := json.Unmarshal(accRaw, &acc); err != nil || acc.Address == "" {
				continue
			}

			balanceWg.Add(1)
			go func(address string) {
				defer balanceWg.Done()
				balanceSemaphore <- struct{}{}        // Acquire semaphore
				defer func() { <-balanceSemaphore }() // Release semaphore

				balancesURL := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", restURL, address)
				var individualBalances struct {
					Balances []Coin `json:"balances"`
				}

				balanceData, err := HTTPGet(balancesURL)
				if err != nil {
					// Don't log every single error here, just count them
					balanceErrChan <- fmt.Errorf("addr %s: %w", address, err)
					return
				}
				if err := json.Unmarshal(balanceData, &individualBalances); err != nil {
					balanceErrChan <- fmt.Errorf("addr %s unmarshal: %w", address, err)
					return
				}
				if len(individualBalances.Balances) > 0 {
					balanceChan <- BalanceEntry{Address: address, Coins: individualBalances.Balances}
				}
			}(acc.Address)
		}

		go func() { // Closer goroutine
			balanceWg.Wait()
			close(balanceChan)
			close(balanceErrChan)
		}()

		// Collect results
		for entry := range balanceChan {
			bankBalances = append(bankBalances, entry)
		}
		for _ = range balanceErrChan { // Corrected: Use blank identifier for unused err
			// Log aggregated errors or a sample?
			// logger.Printf("[%s] Warning: balance fetch error: %v", chainIDStr, err)
			balanceFetchErrors++
		}

		state.Bank.Balances = bankBalances // Assign fetched balances
		logger.Printf("[%s] Fetched balances for %d accounts (encountered %d errors).", chainIDStr, len(bankBalances), balanceFetchErrors)
		if balanceFetchErrors > 0 {
			mu.Lock()
			state.Errors["bank_balances_fetch"] = fmt.Sprintf("%d accounts failed balance fetch", balanceFetchErrors)
			mu.Unlock()
		}
	} else {
		logger.Printf("[%s] Skipping balance fetch as auth accounts fetch failed or bank state is missing.", chainIDStr)
		mu.Lock()
		if state.Bank != nil { // Add error only if bank state struct exists but accounts were missing
			state.Errors["bank_balances_skipped"] = "Auth accounts unavailable or fetch failed"
		}
		mu.Unlock()
	}

	if len(state.Errors) > 0 {
		logger.Printf("[%s] State dump fetch completed with %d module errors.", chainIDStr, len(state.Errors))
	} else {
		logger.Printf("[%s] State dump fetch completed successfully.", chainIDStr)
	}

	// Return the state struct, the caller (takeSnapshot) will handle saving
	// Return nil error even if there were partial fetch errors, as we want to save what we got.
	// The errors map inside FetchedState indicates issues.
	return state, nil
}
