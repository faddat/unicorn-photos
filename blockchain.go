package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// BlockchainRPCResponse represents a response from a blockchain RPC endpoint
type BlockchainRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  struct {
		LastHeight string `json:"last_height"`
	} `json:"result"`
	Error interface{} `json:"error"`
}

// getLatestBlockHeight queries the blockchain RPC endpoint to get the latest block height
func getLatestBlockHeight() (int64, error) {
	// For testing without an actual blockchain, return a fixed value
	if os.Getenv("UNICORN_PHOTOS_TEST_MODE") == "true" {
		logger.Printf("Running in test mode, returning mock block height")
		return 12345, nil
	}

	// In production, we'd query the actual blockchain
	client := &http.Client{Timeout: 10 * time.Second}

	// This URL would be configured in a real implementation
	rpcURL := "https://rpc.unicorn.photos/status"
	if url := os.Getenv("UNICORN_PHOTOS_RPC_URL"); url != "" {
		rpcURL = url
	}

	resp, err := client.Get(rpcURL)
	if err != nil {
		return 0, fmt.Errorf("failed to query blockchain status: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %w", err)
	}

	var rpcResp BlockchainRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return 0, fmt.Errorf("failed to parse RPC response: %w", err)
	}

	if rpcResp.Error != nil {
		return 0, fmt.Errorf("RPC error: %v", rpcResp.Error)
	}

	height, err := strconv.ParseInt(rpcResp.Result.LastHeight, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse block height: %w", err)
	}

	return height, nil
}
