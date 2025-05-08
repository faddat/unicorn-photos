package main

import (
	"context"
	// "encoding/json" // No longer directly used for this function
	"fmt"
	// "io" // No longer directly used
	// "net/http" // Switching to cometbft client
	"os"
	// "strconv" // No longer directly used
	"time"

	comethttp "github.com/cometbft/cometbft/rpc/client/http"
)

// BlockchainRPCResponse is no longer used by getLatestBlockHeight with cometbft client
// type BlockchainRPCResponse struct { ... }

// getLatestBlockHeight queries a specific blockchain RPC endpoint to get the latest block height.
func getLatestBlockHeight(rpcURL string) (int64, error) { // Added rpcURL parameter
	if os.Getenv("UNICORN_PHOTOS_TEST_MODE") == "true" {
		logger.Printf("Running in test mode, returning mock block height 12345")
		return 12345, nil
	}

	client, err := comethttp.New(rpcURL)
	if err != nil {
		return 0, fmt.Errorf("failed to create CometBFT client for %s: %w", rpcURL, err)
	}
	// Setting timeout on the client itself if desired, or per call via context
	// client.SetTimeout(20 * time.Second) // Example client-wide timeout

	statusCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // Per-call timeout
	defer cancel()

	status, err := client.Status(statusCtx)
	if err != nil {
		return 0, fmt.Errorf("failed to query /status from %s: %w", rpcURL, err)
	}

	if status == nil || status.SyncInfo.LatestBlockHeight == 0 {
		// Log the actual status if it's not nil, for debugging
		// if status != nil {
		// 	logger.Printf("[%s] Warning: Invalid status response or zero block height. Status: %+v", rpcURL, status.SyncInfo)
		// }
		return 0, fmt.Errorf("invalid status response or zero block height from %s", rpcURL)
	}

	return status.SyncInfo.LatestBlockHeight, nil
}
