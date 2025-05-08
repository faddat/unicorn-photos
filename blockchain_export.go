package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Export blockchain module parameters and state
func exportBlockchainState(height int64, snapshotDir string) error {
	logger.Printf("Exporting blockchain state at height %d", height)

	// Export various module parameters
	if err := exportAuthParams(snapshotDir); err != nil {
		return fmt.Errorf("failed to export auth parameters: %w", err)
	}

	if err := exportBankParams(snapshotDir); err != nil {
		return fmt.Errorf("failed to export bank parameters: %w", err)
	}

	if err := exportStakingParams(snapshotDir); err != nil {
		return fmt.Errorf("failed to export staking parameters: %w", err)
	}

	if err := exportGovernanceParams(snapshotDir); err != nil {
		return fmt.Errorf("failed to export governance parameters: %w", err)
	}

	// Export genesis-like state file
	if err := exportGenesisState(height, snapshotDir); err != nil {
		return fmt.Errorf("failed to export genesis state: %w", err)
	}

	return nil
}

// AuthParams represents authentication module parameters
type AuthParams struct {
	MaxMemoCharacters string `json:"max_memo_characters"`
	TxSigLimit        string `json:"tx_sig_limit"`
}

// Export auth module parameters
func exportAuthParams(snapshotDir string) error {
	// In production, this would query the blockchain
	// For demonstration, we'll use placeholder values
	authParams := AuthParams{
		MaxMemoCharacters: "256",
		TxSigLimit:        "7",
	}

	filePath := filepath.Join(snapshotDir, "auth_params.json")
	data, err := json.MarshalIndent(authParams, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// BankParams represents bank module parameters
type BankParams struct {
	SendEnabled        bool `json:"send_enabled"`
	DefaultSendEnabled bool `json:"default_send_enabled"`
}

// Export bank module parameters
func exportBankParams(snapshotDir string) error {
	bankParams := BankParams{
		SendEnabled:        true,
		DefaultSendEnabled: true,
	}

	filePath := filepath.Join(snapshotDir, "bank_params.json")
	data, err := json.MarshalIndent(bankParams, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// StakingParams represents staking module parameters
type StakingParams struct {
	UnbondingTime     string `json:"unbonding_time"`
	MaxValidators     int    `json:"max_validators"`
	MaxEntries        int    `json:"max_entries"`
	BondDenom         string `json:"bond_denom"`
	HistoricalEntries int    `json:"historical_entries"`
}

// Export staking module parameters
func exportStakingParams(snapshotDir string) error {
	stakingParams := StakingParams{
		UnbondingTime:     "1814400s",
		MaxValidators:     100,
		MaxEntries:        7,
		BondDenom:         "umeme",
		HistoricalEntries: 10000,
	}

	filePath := filepath.Join(snapshotDir, "staking_params.json")
	data, err := json.MarshalIndent(stakingParams, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// GovParams represents governance module parameters
type GovParams struct {
	VotingPeriod        string `json:"voting_period"`
	DepositPeriod       string `json:"deposit_period"`
	MinDeposit          string `json:"min_deposit"`
	QuorumPercentage    string `json:"quorum_percentage"`
	ThresholdPercentage string `json:"threshold_percentage"`
}

// Export governance module parameters
func exportGovernanceParams(snapshotDir string) error {
	govParams := GovParams{
		VotingPeriod:        "1209600s",
		DepositPeriod:       "604800s",
		MinDeposit:          "10000000umeme",
		QuorumPercentage:    "0.334",
		ThresholdPercentage: "0.5",
	}

	filePath := filepath.Join(snapshotDir, "gov_params.json")
	data, err := json.MarshalIndent(govParams, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// Export a full genesis-like state
func exportGenesisState(height int64, snapshotDir string) error {
	// This would be a complete genesis state extraction in production
	// For now we'll use a simplified placeholder
	genesisState := map[string]interface{}{
		"chain_id": "unicorn-1",
		"height":   height,
		"time":     "2023-10-12T00:00:00Z",
		"app_hash": "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF",
		"modules": []string{
			"auth",
			"bank",
			"staking",
			"slashing",
			"distribution",
			"gov",
		},
	}

	filePath := filepath.Join(snapshotDir, "genesis.json")
	data, err := json.MarshalIndent(genesisState, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}
