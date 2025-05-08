package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Struct to hold runtime state for endpoints
type ChainEndpointsState struct {
	RPCEndpoints  []DiscoveredEndpoint
	RESTEndpoints []DiscoveredEndpoint
	lastDiscovery time.Time
	mu            sync.Mutex
}

var chainEndpoints = make(map[string]*ChainEndpointsState) // chainID -> state
var chainEndpointsMu sync.Mutex

func getChainEndpointsState(chainID string) *ChainEndpointsState {
	chainEndpointsMu.Lock()
	defer chainEndpointsMu.Unlock()
	if _, ok := chainEndpoints[chainID]; !ok {
		chainEndpoints[chainID] = &ChainEndpointsState{}
	}
	return chainEndpoints[chainID]
}

func runDaemon(ctx context.Context) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Initialize logger with configured level (implementation detail for logger.go)
	// SetupLogger(config.LogLevel)

	logger.Printf("Starting Unicorn Photos IPFS snapshot daemon for multiple chains")
	logger.Printf("Global snapshot interval: %s", config.GlobalSnapshotInterval)
	logger.Printf("Max pinned size: %d bytes (%d GB)", config.MaxPinnedSizeBytes, config.MaxPinnedSizeGB)
	logger.Printf("Pruning threshold: %d bytes (%d GB)", config.PruningThresholdBytes, config.PruningThresholdGB)

	ipfs, err := NewIPFSNode(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to initialize IPFS node: %v", err)
	}
	defer ipfs.Close()
	logger.Printf("IPFS node initialized")

	var wg sync.WaitGroup
	for i := range config.Chains {
		chainCfg := config.Chains[i] // Create a copy for the goroutine
		if chainCfg.Enabled {
			wg.Add(1)
			go func(cChainCfg ChainConfig) {
				defer wg.Done()
				runChainSnapshotter(ctx, &cChainCfg, config, ipfs) // Pass pointer to allow state updates
			}(chainCfg)

			// Start per-chain pruning goroutine
			wg.Add(1)
			go func(cChainCfg ChainConfig) {
				defer wg.Done()
				runPerChainPruning(ctx, cChainCfg, config, ipfs)
			}(chainCfg)
		}
	}

	// Global pruning (could be for shared resources or as a fallback)
	// For now, per-chain pruning is primary. This can be re-enabled if needed.
	// go manageGlobalPruning(ctx, config, ipfs)

	// Mutual pinning can run globally
	go manageMutualPinning(ctx, ipfs, config)

	<-ctx.Done() // Wait for context cancellation (e.g., SIGINT)
	logger.Printf("Daemon shutting down...")
	wg.Wait() // Wait for all chain snapshotters to finish
	logger.Printf("All chain processors stopped.")
	return ctx.Err()
}

func runChainSnapshotter(ctx context.Context, chainConfig *ChainConfig, globalConfig *Config, ipfs *IPFSNode) {
	logger.Printf("[%s] Starting snapshotter. Configured Interval: %s", chainConfig.ChainID, chainConfig.SnapshotIntervalRaw)

	isContinuous := chainConfig.SnapshotIntervalRaw == "0s" || chainConfig.SnapshotInterval == 0
	var ticker *time.Ticker
	if !isContinuous {
		ticker = time.NewTicker(chainConfig.SnapshotInterval)
		defer ticker.Stop()
	} else {
		logger.Printf("[%s] Running in continuous mode (as frequent as possible).", chainConfig.ChainID)
	}

	// Initial snapshot for this chain
	logger.Printf("[%s] Attempting initial snapshot...", chainConfig.ChainID)
	if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
		logger.Printf("[%s] Error during initial snapshot: %v", chainConfig.ChainID, err)
	}

	for {
		if isContinuous {
			// In continuous mode, run immediately, then short pause to prevent tight loop on errors
			if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
				logger.Printf("[%s] Error in continuous snapshot: %v. Retrying after 1 minute.", chainConfig.ChainID, err)
				select {
				case <-time.After(1 * time.Minute):
				case <-ctx.Done():
					logger.Printf("[%s] Stopping continuous snapshotter.", chainConfig.ChainID)
					return
				}
			} else {
				// Optional: Add a very short delay even on success for continuous mode
				// to yield resources, e.g., time.Sleep(1 * time.Second)
			}
		} else {
			// Timed mode
			select {
			case <-ctx.Done():
				logger.Printf("[%s] Stopping snapshotter.", chainConfig.ChainID)
				return
			case <-ticker.C:
				if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
					logger.Printf("[%s] Error taking scheduled snapshot: %v", chainConfig.ChainID, err)
				}
			}
		}
		// Check context after loop iteration for continuous mode
		if isContinuous {
			select {
			case <-ctx.Done():
				logger.Printf("[%s] Stopping continuous snapshotter.", chainConfig.ChainID)
				return
			default: // Non-blocking check
			}
		}
	}
}

// getHealthyEndpoint tries configured endpoints, then peer-discovered ones.
func getHealthyEndpoint(ctx context.Context, chainConfig *ChainConfig, endpointType string) (string, error) {
	state := getChainEndpointsState(chainConfig.ChainID)
	state.mu.Lock()
	defer state.mu.Unlock()

	var candidates []DiscoveredEndpoint
	var existingEndpoints []DiscoveredEndpoint

	if endpointType == "rpc" {
		existingEndpoints = state.RPCEndpoints
		for _, url := range chainConfig.RPCEndpoints {
			candidates = append(candidates, DiscoveredEndpoint{Address: url, Type: "rpc", Source: "config"})
		}
	} else { // rest
		existingEndpoints = state.RESTEndpoints
		for _, url := range chainConfig.RESTEndpoints {
			candidates = append(candidates, DiscoveredEndpoint{Address: url, Type: "rest", Source: "config"})
		}
	}

	// Add previously discovered (and working) endpoints
	for _, ep := range existingEndpoints {
		// Avoid duplicates if they were also in config
		isDup := false
		for _, cfgEp := range candidates {
			if cfgEp.Address == ep.Address {
				isDup = true
				break
			}
		}
		if !isDup {
			candidates = append(candidates, ep)
		}
	}

	// Try candidates (config first, then previously discovered)
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	for _, ep := range candidates {
		var err error
		if endpointType == "rpc" {
			err = checkRPC(ctx, ep.Address, chainConfig.ChainID)
		} else {
			err = checkREST(ctx, ep.Address, chainConfig.ChainID)
		}
		if err == nil {
			logger.Printf("[%s] Using healthy %s endpoint: %s (source: %s)", chainConfig.ChainID, endpointType, ep.Address, ep.Source)
			return ep.Address, nil
		}
		logger.Printf("[%s] Endpoint %s (%s) failed health check: %v", chainConfig.ChainID, ep.Address, endpointType, err)
	}

	// If all fail and discovery is enabled, try peer discovery
	if chainConfig.EnablePeerDiscoveryFallback && time.Since(state.lastDiscovery) > 15*time.Minute { // Rate limit discovery
		logger.Printf("[%s] Configured and cached %s endpoints failed. Attempting peer discovery.", chainConfig.ChainID, endpointType)
		// Use configured RPCs as seeds for discovery, or P2P seeds
		var seedRPCsForDiscovery []string
		if len(chainConfig.RPCEndpoints) > 0 {
			seedRPCsForDiscovery = append(seedRPCsForDiscovery, chainConfig.RPCEndpoints...)
		} else if len(state.RPCEndpoints) > 0 { // Use previously good RPCs as seeds
			for _, ep := range state.RPCEndpoints {
				seedRPCsForDiscovery = append(seedRPCsForDiscovery, ep.Address)
			}
		}

		discovered, err := DiscoverEndpoints(ctx, chainConfig.ChainID, seedRPCsForDiscovery, chainConfig.SeedNodesP2P)
		state.lastDiscovery = time.Now()
		if err != nil {
			logger.Printf("[%s] Peer discovery for %s failed: %v", chainConfig.ChainID, endpointType, err)
		} else {
			newlyFoundForType := false
			for _, discEp := range discovered {
				if discEp.Type == endpointType {
					newlyFoundForType = true
					// Add to global cache and try immediately
					if endpointType == "rpc" {
						state.RPCEndpoints = append(state.RPCEndpoints, discEp) // Could add logic to limit cache size
					} else {
						state.RESTEndpoints = append(state.RESTEndpoints, discEp)
					}

					var checkErr error
					if endpointType == "rpc" {
						checkErr = checkRPC(ctx, discEp.Address, chainConfig.ChainID)
					} else {
						checkErr = checkREST(ctx, discEp.Address, chainConfig.ChainID)
					}
					if checkErr == nil {
						logger.Printf("[%s] Using newly discovered healthy %s endpoint: %s", chainConfig.ChainID, endpointType, discEp.Address)
						return discEp.Address, nil
					}
					logger.Printf("[%s] Discovered endpoint %s (%s) failed immediate health check: %v", chainConfig.ChainID, discEp.Address, endpointType, checkErr)
				}
			}
			if !newlyFoundForType {
				logger.Printf("[%s] Peer discovery ran, but no new usable %s endpoints found.", chainConfig.ChainID, endpointType)
			}
		}
	}
	return "", fmt.Errorf("[%s] no healthy %s endpoint found after all attempts", chainConfig.ChainID, endpointType)
}

func takeAndProcessSnapshotForChain(ctx context.Context, chainConfig *ChainConfig, globalConfig *Config, ipfs *IPFSNode) error {
	logger.Printf("[%s] Checking for new blocks to snapshot...", chainConfig.ChainID)

	rpcURL, err := getHealthyEndpoint(ctx, chainConfig, "rpc")
	if err != nil {
		return fmt.Errorf("[%s] no healthy RPC endpoint found: %w", chainConfig.ChainID, err)
	}

	height, err := getLatestBlockHeight(rpcURL) // getLatestBlockHeight uses this specific RPC
	if err != nil {
		return fmt.Errorf("[%s] failed to get latest block height from %s: %w", chainConfig.ChainID, rpcURL, err)
	}

	if height <= chainConfig.LastSuccessfulSnapshotHeight && chainConfig.LastSuccessfulSnapshotHeight > 0 {
		logger.Printf("[%s] No new blocks since last successful snapshot (current: %d, last: %d). Skipping.",
			chainConfig.ChainID, height, chainConfig.LastSuccessfulSnapshotHeight)
		return nil
	}
	if height == chainConfig.LastAttemptedSnapshotHeight && chainConfig.LastAttemptedSnapshotHeight > 0 {
		logger.Printf("[%s] Current height %d was already attempted. Skipping to avoid retry loops on unchanged height.",
			chainConfig.ChainID, height)
		return nil
	}

	logger.Printf("[%s] Latest block height from %s: %d", chainConfig.ChainID, rpcURL, height)
	chainConfig.LastAttemptedSnapshotHeight = height // Mark as attempted

	chainSnapshotBaseDir := filepath.Join(globalConfig.SnapshotBaseDir, chainConfig.ChainID)
	snapshotDir, err := ensureSnapshotDir(height, chainSnapshotBaseDir)
	if err != nil {
		return fmt.Errorf("[%s] failed to ensure snapshot directory: %w", chainConfig.ChainID, err)
	}

	restURL, err := getHealthyEndpoint(ctx, chainConfig, "rest")
	if err != nil {
		return fmt.Errorf("[%s] no healthy REST endpoint found: %w", chainConfig.ChainID, err)
	}
	logger.Printf("[%s] Using REST endpoint: %s", chainConfig.ChainID, restURL)

	if err := takeSnapshot(height, *chainConfig, restURL, snapshotDir); err != nil {
		return fmt.Errorf("[%s] failed to take snapshot: %w", chainConfig.ChainID, err)
	}

	if err := addSnapshotToIndex(ipfs, globalConfig, chainConfig.ChainID, height, snapshotDir); err != nil {
		logger.Printf("[%s] Warning: failed to add snapshot to index and IPFS: %v. Snapshot data may exist locally but not be pinned or indexed.", chainConfig.ChainID, err)
		// Don't return error here, as snapshot was taken. Indexing can be retried or done manually.
	} else {
		// Update main README (index.json itself is published by addSnapshotToIndex)
		index := loadSnapshotIndex(globalConfig)
		if err := updateReadmeWithIPFS(index, globalConfig, ipfs); err != nil {
			logger.Printf("[%s] Warning: failed to update main README.md: %v", chainConfig.ChainID, err)
		}
	}

	chainConfig.LastSuccessfulSnapshotHeight = height
	logger.Printf("[%s] Successfully processed snapshot for height %d.", chainConfig.ChainID, height)
	return nil
}

func runPerChainPruning(ctx context.Context, chainConfig ChainConfig, globalConfig *Config, ipfs *IPFSNode) {
	logger.Printf("[%s] Starting per-chain pruning service. Interval: %s, Keep: %d",
		chainConfig.ChainID, chainConfig.PruneInterval, chainConfig.MaxSnapshotsToKeepPerChain)

	ticker := time.NewTicker(chainConfig.PruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Printf("[%s] Stopping per-chain pruning service.", chainConfig.ChainID)
			return
		case <-ticker.C:
			logger.Printf("[%s] Running per-chain snapshot pruning...", chainConfig.ChainID)
			err := pruneOldSnapshotsForChain(ipfs, globalConfig, &chainConfig)
			if err != nil {
				logger.Printf("[%s] Error during per-chain pruning: %v", chainConfig.ChainID, err)
			}
		}
	}
}

func pruneOldSnapshotsForChain(ipfs *IPFSNode, config *Config, chainConfig *ChainConfig) error {
	logger.Printf("[%s] Checking if snapshots need pruning (max to keep: %d)...",
		chainConfig.ChainID, chainConfig.MaxSnapshotsToKeepPerChain)

	chainSnapshotBaseDir := filepath.Join(config.SnapshotBaseDir, chainConfig.ChainID)
	entries, err := os.ReadDir(chainSnapshotBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Printf("[%s] Snapshot directory %s does not exist, nothing to prune.", chainConfig.ChainID, chainSnapshotBaseDir)
			return nil
		}
		return fmt.Errorf("failed to read snapshot directory %s: %w", chainSnapshotBaseDir, err)
	}

	var snapshots []struct {
		path    string
		height  int64
		modTime time.Time
	}

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "height_") {
			continue
		}
		var height int64
		if _, errS := fmt.Sscanf(entry.Name(), "height_%d", &height); errS != nil {
			logger.Printf("[%s] Warning: couldn't parse height from directory name %s", chainConfig.ChainID, entry.Name())
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			logger.Printf("[%s] Warning: couldn't get info for directory %s: %v", chainConfig.ChainID, entry.Name(), errInfo)
			continue
		}
		snapshots = append(snapshots, struct {
			path    string
			height  int64
			modTime time.Time
		}{
			path:    filepath.Join(chainSnapshotBaseDir, entry.Name()),
			height:  height,
			modTime: info.ModTime(),
		})
	}

	if len(snapshots) <= chainConfig.MaxSnapshotsToKeepPerChain {
		logger.Printf("[%s] Found %d snapshots, which is within the limit of %d. No pruning needed.",
			chainConfig.ChainID, len(snapshots), chainConfig.MaxSnapshotsToKeepPerChain)
		return nil
	}

	// Sort snapshots by modification time (oldest first) to prune
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].modTime.Before(snapshots[j].modTime)
	})

	numToPrune := len(snapshots) - chainConfig.MaxSnapshotsToKeepPerChain
	logger.Printf("[%s] Need to prune %d old snapshots.", chainConfig.ChainID, numToPrune)

	snapshotIndex := loadSnapshotIndex(config) // Load once

	for i := 0; i < numToPrune; i++ {
		snapshot := snapshots[i]
		logger.Printf("[%s] Removing snapshot at height %d (path: %s)",
			chainConfig.ChainID, snapshot.height, snapshot.path)

		// Find CID in index to unpin
		var cidToUnpin string
		foundInIndex := false
		for _, indexedSnap := range snapshotIndex.Snapshots {
			// Compare just the directory name "height_XXXX"
			if indexedSnap.ChainID == chainConfig.ChainID && filepath.Base(indexedSnap.Path) == filepath.Base(snapshot.path) {
				cidToUnpin = indexedSnap.IPFSCID
				foundInIndex = true
				break
			}
		}

		if foundInIndex && cidToUnpin != "" {
			if err := ipfs.UnpinCID(cidToUnpin); err != nil {
				logger.Printf("[%s] Warning: failed to unpin CID %s for snapshot %s: %v",
					chainConfig.ChainID, cidToUnpin, snapshot.path, err)
				// Continue with local deletion even if unpin fails
			} else {
				logger.Printf("[%s] Successfully unpinned CID %s for snapshot %s", chainConfig.ChainID, cidToUnpin, snapshot.path)
			}
		} else {
			logger.Printf("[%s] Warning: Snapshot %s (height %d) not found in index or CID is empty, cannot unpin from IPFS. It might have been manually removed or not indexed.",
				chainConfig.ChainID, snapshot.path, snapshot.height)
		}

		if err := os.RemoveAll(snapshot.path); err != nil {
			logger.Printf("[%s] Warning: failed to remove snapshot directory %s: %v",
				chainConfig.ChainID, snapshot.path, err)
			continue // Skip to next if removal fails
		}

		// Remove from the loaded index struct (so subsequent unpins for the same pruning session are faster)
		// This doesn't save the index; saving happens when new snapshots are added.
		// This is an optimization to prevent re-searching the full index.
		updatedSnapshots := []IPFSSnapshot{}
		for _, indexedSnap := range snapshotIndex.Snapshots {
			if !(indexedSnap.ChainID == chainConfig.ChainID && filepath.Base(indexedSnap.Path) == filepath.Base(snapshot.path)) {
				updatedSnapshots = append(updatedSnapshots, indexedSnap)
			}
		}
		snapshotIndex.Snapshots = updatedSnapshots
	}

	// After pruning, the index file (index.json) itself should be updated and re-pinned
	// This is important if pruning removed entries that were part of the last published index.
	// The `addSnapshotToIndex` function handles publishing the index, so this will be updated
	// automatically when the next snapshot is added. For immediate update, we could call:
	// SaveAndPublishIndex(snapshotIndex, config, ipfs)
	// However, to avoid too frequent index updates, let's rely on the next snapshot addition.

	return nil
}

// Helper function to calculate directory size (can be moved to snapshot_utils.go if not already there)
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func manageMutualPinning(ctx context.Context, node *IPFSNode, config *Config) {
	// Check more frequently if interval is shorter, e.g. once per global snapshot interval
	pinInterval := config.GlobalSnapshotInterval
	if pinInterval < 1*time.Hour { // Ensure it's not too frequent
		pinInterval = 1 * time.Hour
	}
	if pinInterval > 6*time.Hour { // Ensure it's not too infrequent
		pinInterval = 6 * time.Hour
	}

	ticker := time.NewTicker(pinInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Printf("Stopping mutual pinning service.")
			return
		case <-ticker.C:
			logger.Printf("Running mutual pinning check...")
			index := loadSnapshotIndex(config) // Load the latest index
			if len(index.Snapshots) == 0 {
				logger.Printf("No snapshots in index to mutually pin.")
				continue
			}
			pinnedCount := 0
			failedCount := 0
			// Pin a subset or all? For now, let's try all known CIDs from our index.
			// In a real distributed scenario, this would involve discovering CIDs from other trusted nodes.
			for _, snapshot := range index.Snapshots {
				if snapshot.IPFSCID != "" {
					// PinCID already checks MaxPinnedSize limit
					if err := node.PinCID(snapshot.IPFSCID, snapshot.Size); err != nil { // Pass size to PinCID
						// Log as debug or warning if it's an expected error (like already pinned or size limit)
						// logger.Printf("Failed to pin %s (size %d): %v", snapshot.IPFSCID, snapshot.Size, err)
						failedCount++
					} else {
						// logger.Printf("Successfully pinned/verified %s (size %d)", snapshot.IPFSCID, snapshot.Size)
						pinnedCount++
					}
				}
			}
			logger.Printf("Mutual pinning check complete. Verified/Pinned: %d, Failed/Skipped: %d", pinnedCount, failedCount)
		}
	}
}
