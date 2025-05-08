package main

import (
	"encoding/json"
	"fmt"
	"net/url"

	// "path/filepath" // Not used directly in this version
	// "os" // Not used directly in this version
	"time"
	// "strings"
)

// Structs for different modules will be defined here.
// Example:
type AuthParams struct {
	MaxMemoCharacters      string `json:"max_memo_characters"`
	TxSigLimit             string `json:"tx_sig_limit"`
	TxSizeCostPerByte      string `json:"tx_size_cost_per_byte"`
	SigVerifyCostEd25519   string `json:"sig_verify_cost_ed25519"`
	SigVerifyCostSecp256k1 string `json:"sig_verify_cost_secp256k1"`
}
type AuthAccount json.RawMessage // Using RawMessage to keep accounts flexible

type BankParams struct {
	// SendEnabled        []SendEnabledEntry `json:"send_enabled"` // Older structure
	DefaultSendEnabled bool `json:"default_send_enabled"`
}

// type SendEnabledEntry struct { // Older structure
// Denom   string `json:"denom"`
// Enabled bool   `json:"enabled"`
// }
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
	URI         string      `json:"uri"`      // For token images, etc.
	URIHash     string      `json:"uri_hash"` // Hash of the URI content
}
type DenomUnit struct {
	Denom    string   `json:"denom"`
	Exponent uint32   `json:"exponent"`
	Aliases  []string `json:"aliases"`
}

// AppState structures
type AuthAppState struct {
	Params   AuthParams    `json:"params"`
	Accounts []AuthAccount `json:"accounts"`
}
type BankAppState struct {
	Params        BankParams      `json:"params"`
	Balances      []BalanceEntry  `json:"balances"`
	Supply        []Coin          `json:"supply"`
	DenomMetadata []DenomMetadata `json:"denom_metadata"`
	// SendEnabled is often part of params in newer SDKs, or implicitly all if not specified
}

// ... Add more app state structs for staking, gov, etc. as needed

type GenesisAppState struct {
	Auth *AuthAppState `json:"auth,omitempty"`
	Bank *BankAppState `json:"bank,omitempty"`
	// Staking      *StakingAppState      `json:"staking,omitempty"`
	// Gov          *GovAppState          `json:"gov,omitempty"`
	// Distribution *DistributionAppState `json:"distribution,omitempty"`
	// Slashing     *SlashingAppState     `json:"slashing,omitempty"`
	// Mint         *MintAppState         `json:"mint,omitempty"`
	// IBC, other modules...
}

type FullGenesisDoc struct {
	GenesisTime     string           `json:"genesis_time"`
	ChainID         string           `json:"chain_id"`
	InitialHeight   string           `json:"initial_height"` // Usually "1" for snapshots
	ConsensusParams *ConsensusParams `json:"consensus_params,omitempty"`
	AppHash         string           `json:"app_hash"` // Typically empty for a new genesis from snapshot
	AppState        GenesisAppState  `json:"app_state"`
}

type ConsensusParams struct { // Simplified based on common defaults
	Block     map[string]string   `json:"block"`
	Evidence  map[string]string   `json:"evidence"`
	Validator map[string][]string `json:"validator"`
	Version   map[string]string   `json:"version,omitempty"` // Often empty or has "app": "0"
}

func fetchPaginatedData(restURL, path string, resultKey string, target interface{}) error {
	var allItems []json.RawMessage
	paginationKey := ""
	endpoint := fmt.Sprintf("%s%s", restURL, path)

	for {
		queryURL := endpoint
		params := url.Values{}
		params.Add("pagination.limit", "500") // Request more items per page

		if paginationKey != "" {
			params.Add("pagination.key", paginationKey)
		}
		if len(params) > 0 {
			queryURL = fmt.Sprintf("%s?%s", endpoint, params.Encode())
		}

		body, err := HTTPGet(queryURL)
		if err != nil {
			return fmt.Errorf("failed to fetch from %s: %w", queryURL, err)
		}

		var rawResp map[string]json.RawMessage
		if err := json.Unmarshal(body, &rawResp); err != nil {
			return fmt.Errorf("failed to unmarshal paginated response base from %s: %w", queryURL, err)
		}

		var currentItems []json.RawMessage // Declare currentItems here
		if itemsVal, ok := rawResp[resultKey]; ok {
			if err := json.Unmarshal(itemsVal, &currentItems); err != nil { // Corrected: currentItems
				return fmt.Errorf("failed to unmarshal items for key '%s' from %s: %w", resultKey, queryURL, err)
			}
			allItems = append(allItems, currentItems...)
		} else {
			if _, pagOk := rawResp["pagination"]; !pagOk {
				return fmt.Errorf("resultKey '%s' not found and no distinct items array or pagination in response from %s", resultKey, queryURL)
			}
			// If resultKey is not found but pagination exists, it might be an empty page for that key.
			// Or the key name itself might be dynamic/absent in some responses.
			// logger.Printf("Warning: resultKey '%s' not found in response from %s, but pagination key exists.", resultKey, queryURL)
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

func generateCosmosGenesisDoc(restURL, chainIDStr string, currentBlockHeight int64, snapshotDir string) (*FullGenesisDoc, error) {
	logger.Printf("[%s] Generating genesis document from REST endpoint: %s at height ~%d", chainIDStr, restURL, currentBlockHeight)

	doc := &FullGenesisDoc{
		GenesisTime:   time.Now().UTC().Format(time.RFC3339Nano), // Use RFC3339Nano for more precision
		ChainID:       chainIDStr,
		InitialHeight: "1", // Snapshots typically start a new chain at height 1
		AppHash:       "",  // App hash is usually empty for a new genesis from snapshot
		ConsensusParams: &ConsensusParams{ // Common default consensus params
			Block:     map[string]string{"max_bytes": "22020096", "max_gas": "-1"},
			Evidence:  map[string]string{"max_age_num_blocks": "100000", "max_age_duration": "172800000000000", "max_bytes": "1048576"},
			Validator: map[string][]string{"pub_key_types": {"ed25519"}},
			Version:   map[string]string{}, // Often empty or {"app": "0"}
		},
		AppState: GenesisAppState{}, // Initialize AppState
	}

	// --- Fetch Auth Module State ---
	logger.Printf("[%s] Fetching auth params...", chainIDStr)
	authParamsData, err := HTTPGet(fmt.Sprintf("%s/cosmos/auth/v1beta1/params", restURL))
	var authParamsResp struct {
		Params AuthParams `json:"params"`
	}
	if err != nil {
		logger.Printf("[%s] Warning: failed to fetch auth params: %v. Using defaults.", chainIDStr, err)
		// Use some sane defaults if fetch fails
		authParamsResp.Params = AuthParams{MaxMemoCharacters: "256", TxSigLimit: "7", TxSizeCostPerByte: "10", SigVerifyCostEd25519: "590", SigVerifyCostSecp256k1: "1000"}
	} else {
		if err := json.Unmarshal(authParamsData, &authParamsResp); err != nil {
			return nil, fmt.Errorf("[%s] failed to unmarshal auth params: %w. Body: %s", chainIDStr, err, string(authParamsData))
		}
	}

	logger.Printf("[%s] Fetching all auth accounts...", chainIDStr)
	var authAccounts []AuthAccount
	if err := fetchPaginatedData(restURL, "/cosmos/auth/v1beta1/accounts", "accounts", &authAccounts); err != nil {
		return nil, fmt.Errorf("[%s] failed to fetch auth accounts: %w", chainIDStr, err)
	}
	// Modify accounts as per snapshot.py (reset sequence and account number for BaseAccount)
	for i, accRaw := range authAccounts {
		var accMap map[string]interface{}
		if err := json.Unmarshal(accRaw, &accMap); err == nil {
			if accType, ok := accMap["@type"].(string); ok && accType == "/cosmos.auth.v1beta1.BaseAccount" {
				accMap["account_number"] = "0" // Stored as string in some genesis
				accMap["sequence"] = "0"       // Stored as string
				modifiedAccRaw, _ := json.Marshal(accMap)
				authAccounts[i] = modifiedAccRaw
			}
		}
	}
	doc.AppState.Auth = &AuthAppState{Params: authParamsResp.Params, Accounts: authAccounts}
	logger.Printf("[%s] Fetched %d auth accounts.", chainIDStr, len(authAccounts))

	// --- Fetch Bank Module State ---
	logger.Printf("[%s] Fetching bank params...", chainIDStr)
	bankParamsData, err := HTTPGet(fmt.Sprintf("%s/cosmos/bank/v1beta1/params", restURL))
	var bankParamsResp struct {
		Params BankParams `json:"params"`
	}
	if err != nil {
		logger.Printf("[%s] Warning: failed to fetch bank params: %v. Using defaults.", chainIDStr, err)
		bankParamsResp.Params = BankParams{DefaultSendEnabled: true}
	} else {
		if err := json.Unmarshal(bankParamsData, &bankParamsResp); err != nil {
			return nil, fmt.Errorf("[%s] failed to unmarshal bank params: %w. Body: %s", chainIDStr, err, string(bankParamsData))
		}
	}

	logger.Printf("[%s] Fetching all account balances...", chainIDStr)
	var bankBalances []BalanceEntry
	for _, accRaw := range authAccounts { // Iterate over already fetched accounts
		var acc struct {
			Address string `json:"address"`
		} // Minimal unmarshal to get address
		if err := json.Unmarshal(accRaw, &acc); err != nil {
			logger.Printf("[%s] Warning: could not parse address from account data: %v", chainIDStr, err)
			continue
		}
		if acc.Address == "" {
			continue
		}

		balancesURL := fmt.Sprintf("%s/cosmos/bank/v1beta1/balances/%s", restURL, acc.Address)
		// This balance endpoint can be paginated too for accounts with many token types
		var individualBalances struct {
			Balances   []Coin          `json:"balances"`
			Pagination json.RawMessage `json:"pagination"`
		} // handle potential pagination

		// Simple GET for now, assuming not too many denoms per account to require balance pagination handling
		balanceData, err := HTTPGet(balancesURL)
		if err != nil {
			logger.Printf("[%s] Warning: failed to fetch balance for account %s: %v", chainIDStr, acc.Address, err)
			continue // Skip this account's balance on error
		}
		if err := json.Unmarshal(balanceData, &individualBalances); err != nil {
			logger.Printf("[%s] Warning: failed to unmarshal balance for account %s: %v. Body: %s", chainIDStr, acc.Address, err, string(balanceData))
			continue
		}
		if len(individualBalances.Balances) > 0 {
			bankBalances = append(bankBalances, BalanceEntry{Address: acc.Address, Coins: individualBalances.Balances})
		}
	}
	logger.Printf("[%s] Fetched balances for %d accounts.", chainIDStr, len(bankBalances))

	logger.Printf("[%s] Fetching bank supply...", chainIDStr)
	var bankSupply []Coin
	// Supply can be paginated if there are many denoms
	if err := fetchPaginatedData(restURL, "/cosmos/bank/v1beta1/supply", "supply", &bankSupply); err != nil {
		return nil, fmt.Errorf("[%s] failed to fetch bank supply: %w", chainIDStr, err)
	}

	logger.Printf("[%s] Fetching bank denom metadata...", chainIDStr)
	var bankDenomMetadata []DenomMetadata
	if err := fetchPaginatedData(restURL, "/cosmos/bank/v1beta1/denoms_metadata", "metadatas", &bankDenomMetadata); err != nil {
		return nil, fmt.Errorf("[%s] failed to fetch bank denoms_metadata: %w", chainIDStr, err)
	}
	doc.AppState.Bank = &BankAppState{
		Params:        bankParamsResp.Params,
		Balances:      bankBalances,
		Supply:        bankSupply,
		DenomMetadata: bankDenomMetadata,
	}

	// --- Placeholder for other modules (staking, gov, distribution, slashing, mint, ibc etc.) ---
	// Each module would require similar fetching for its params and state.
	// Example for Staking:
	// - /cosmos/staking/v1beta1/params
	// - /cosmos/staking/v1beta1/validators (paginated, with status filters)
	// - /cosmos/staking/v1beta1/delegations/{delegator_addr} (or all delegations, paginated)
	// - /cosmos/staking/v1beta1/unbonding_delegations/{delegator_addr} (or all, paginated)
	// - /cosmos/staking/v1beta1/redelegations (paginated, with src/dst validator filters)
	// This part is extensive and specific to each module's structure.

	logger.Printf("[%s] Genesis document generation complete.", chainIDStr) // Removed "Saving to file..."

	// Save the generated genesis to a file for inspection or direct use.
	// This is done by takeSnapshot now.

	return doc, nil
}
