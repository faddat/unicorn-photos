package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	// "sync/atomic" // No longer needed
)

// Struct to hold runtime state for endpoints
type ChainEndpointsState struct {
	RPCEndpoints  []DiscoveredEndpoint
	RESTEndpoints []DiscoveredEndpoint
	lastDiscovery time.Time
	mu            sync.Mutex
}

// ChainEndpointsState remains the same

var chainEndpoints = make(map[string]*ChainEndpointsState)
var chainEndpointsMu sync.Mutex

func getChainEndpointsState(chainID string) *ChainEndpointsState {
	chainEndpointsMu.Lock()
	defer chainEndpointsMu.Unlock()
	if _, ok := chainEndpoints[chainID]; !ok {
		chainEndpoints[chainID] = &ChainEndpointsState{}
	}
	return chainEndpoints[chainID]
}

// runGitCommand executes a git command with context and timeout
func runGitCommand(ctx context.Context, timeout time.Duration, dir string, args ...string) error {
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmdArgs := []string{}
	if dir != "" {
		// Check if dir exists before trying to run command in it
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			// If the command isn't 'clone', this is likely an error
			if len(args) > 0 && args[0] != "clone" {
				return fmt.Errorf("git directory %s does not exist", dir)
			}
			// For clone, dir is the target, so it might not exist yet, which is fine.
		} else {
			cmdArgs = append(cmdArgs, "-C", dir)
		}
	}
	cmdArgs = append(cmdArgs, args...)

	// If the command is 'clone', the target dir is the last argument
	if len(args) > 0 && args[0] == "clone" && dir != "" {
		// Ensure clone command includes the target directory if -C wasn't used (dir didn't exist)
		hasTargetDirArg := false
		if len(cmdArgs) > 0 {
			lastArg := cmdArgs[len(cmdArgs)-1]
			// A simple check, might need refinement if repo URL could look like a path
			if !strings.HasPrefix(lastArg, "-") && !strings.Contains(lastArg, "://") {
				hasTargetDirArg = true
			}
		}
		if !hasTargetDirArg {
			cmdArgs = append(cmdArgs, dir) // Add target dir explicitly
		}
		// Remove -C flag if present, as clone target is now explicit
		filteredArgs := []string{}
		for i := 0; i < len(cmdArgs); i++ {
			if cmdArgs[i] == "-C" {
				i++ // Skip the directory path as well
				continue
			}
			filteredArgs = append(filteredArgs, cmdArgs[i])
		}
		cmdArgs = filteredArgs
	}

	cmd := exec.CommandContext(cmdCtx, "git", cmdArgs...)
	logger.Printf("Running git command: %s", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git command failed: %w\nOutput:\n%s", err, string(output))
	}
	logger.Printf("Git command successful. Output:\n%s", string(output))
	return nil
}

// isGitRepo checks if a path is a git repository (.git exists)
func isGitRepo(path string) bool {
	gitPath := filepath.Join(path, ".git")
	_, err := os.Stat(gitPath)
	return err == nil // If .git exists (no error), it's likely a git repo
}

func runDaemon(ctx context.Context) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	logger.Printf("Starting Unicorn Photos IPFS snapshot daemon")

	// Override config to set all_chains to true
	config.AllChains = true
	logger.Printf("Setting all_chains=true to process all chains in the registry")

	// --- Determine and Manage Chain Registry Path ---
	userHome, err := os.UserHomeDir()
	if err != nil {
		logger.Printf("Warning: Could not determine user home directory: %v", err)
		// Proceed, but ~ expansion and default path will fail if needed
	}

	registryPathInput := config.ChainRegistryPath // Default is "~/.chain-registry"
	effectiveRegistryPath := registryPathInput
	useManagedDefault := false
	defaultManagedPath := ""

	if userHome != "" {
		defaultManagedPath = filepath.Join(userHome, ".chain-registry")
		if strings.HasPrefix(registryPathInput, "~/") {
			effectiveRegistryPath = filepath.Join(userHome, registryPathInput[2:])
		}
	}
	// Clean the path
	effectiveRegistryPath = filepath.Clean(effectiveRegistryPath)

	// Check if the effective path matches the default managed path
	if effectiveRegistryPath == defaultManagedPath || strings.Contains(effectiveRegistryPath, ".chain-registry") {
		useManagedDefault = true
	}

	registryPathToLoad := effectiveRegistryPath // This path will be passed to LoadRegistryChains

	if useManagedDefault {
		logger.Printf("Managing chain registry at location: %s", effectiveRegistryPath)
		registryPathToLoad = effectiveRegistryPath // Ensure we use the resolved path
		_, err := os.Stat(effectiveRegistryPath)
		if os.IsNotExist(err) {
			// Create parent directory if needed
			if err := os.MkdirAll(filepath.Dir(effectiveRegistryPath), 0755); err != nil {
				logger.Printf("ERROR: Failed to create parent directory for chain registry: %v", err)
			}

			logger.Printf("Cloning cosmos/chain-registry to %s...", effectiveRegistryPath)
			gitErr := runGitCommand(ctx, 2*time.Minute, "", "clone", "https://github.com/cosmos/chain-registry", effectiveRegistryPath)
			if gitErr != nil {
				logger.Printf("ERROR: Failed to clone chain registry: %v. Registry data may be unavailable.", gitErr)
				// Proceed, LoadRegistryChains should handle missing dir
			} else {
				logger.Printf("Chain registry successfully cloned.")
			}
		} else if err == nil { // Directory exists
			if isGitRepo(effectiveRegistryPath) {
				logger.Printf("Updating chain registry at %s...", effectiveRegistryPath)
				gitErr := runGitCommand(ctx, 1*time.Minute, effectiveRegistryPath, "fetch", "origin")
				if gitErr == nil {
					gitErr = runGitCommand(ctx, 1*time.Minute, effectiveRegistryPath, "reset", "--hard", "origin/main")
				}
				if gitErr != nil {
					logger.Printf("Warning: Failed to update chain registry git repo at %s: %v. Using existing local data.", effectiveRegistryPath, gitErr)
				} else {
					logger.Printf("Chain registry successfully updated.")
				}
			} else {
				logger.Printf("Warning: Path %s exists but is not a git repository. Using as is, updates disabled.", effectiveRegistryPath)
			}
		} else { // Other error stating the directory
			logger.Printf("Error checking chain registry path %s: %v. Trying to proceed.", effectiveRegistryPath, err)
		}
	} else {
		logger.Printf("Using user-specified chain registry path: %s", effectiveRegistryPath)
	}

	logger.Printf("Snapshot Base Directory: %s", config.SnapshotBaseDir)
	logger.Printf("Chain Registry Path: %s", effectiveRegistryPath)
	logger.Printf("Snapshot All Chains: %t", config.AllChains)
	if !config.AllChains {
		logger.Printf("Specific Chains to Snapshot: %v", config.ChainsToSnapshot)
	}

	// --- Resolve IPFS repo path ---
	// Ensure the path is absolute with ~ expanded
	resolvedIPFSPath := config.IPFSRepoPath
	if strings.HasPrefix(resolvedIPFSPath, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to resolve home directory for IPFS path: %w", err)
		}
		resolvedIPFSPath = filepath.Join(home, resolvedIPFSPath[2:])
	}
	resolvedIPFSPath = filepath.Clean(resolvedIPFSPath)
	logger.Printf("Using IPFS repository at: %s", resolvedIPFSPath)

	// Update the config with the resolved path
	config.IPFSRepoPath = resolvedIPFSPath

	ipfs, err := NewIPFSNode(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to initialize IPFS node: %v", err)
	}
	defer func() {
		if err := ipfs.Close(); err != nil {
			logger.Printf("Error closing IPFS node: %v", err)
		}
	}()
	logger.Printf("IPFS node initialized")

	// --- Determine Chains to Process ---
	chainsToProcess := make(map[string]*ChainRuntimeConfig) // Use map to handle overrides easily (key: registry name)
	configOverrides := make(map[string]*ChainOverrideConfig)
	for i := range config.ChainOverrides {
		configOverrides[config.ChainOverrides[i].Name] = &config.ChainOverrides[i]
	}

	// Load from registry if needed, now using registryPathToLoad
	var registryChains map[string]*BasicChainInfo
	if config.AllChains || len(config.ChainsToSnapshot) > 0 {
		registryChains, err = LoadRegistryChains(registryPathToLoad, "mainnet") // Use the determined path
		if err != nil {
			logger.Printf("Error loading chain registry from %s: %v. Only chains defined explicitly in config [[chains]] will be processed.", registryPathToLoad, err)
		}
	}

	if config.AllChains {
		logger.Printf("Processing all mainnet chains from registry...")
		for name, basicInfo := range registryChains {
			runtimeConf := createRuntimeConfig(basicInfo, config, configOverrides[name])
			chainsToProcess[name] = runtimeConf
		}
		// Also add any explicit [[chains]] that weren't overrides (e.g., testnets defined only in config)
		for name, override := range configOverrides {
			if _, exists := chainsToProcess[name]; !exists {
				// This chain was only in config, not registry mainnet list
				runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
				if runtimeConf != nil { // Check if minimal info was present
					chainsToProcess[name] = runtimeConf
				}
			}
		}

	} else if len(config.ChainsToSnapshot) > 0 {
		logger.Printf("Processing specific chains from 'chains_to_snapshot' list...")
		if registryChains != nil {
			for _, name := range config.ChainsToSnapshot {
				if basicInfo, ok := registryChains[name]; ok {
					runtimeConf := createRuntimeConfig(basicInfo, config, configOverrides[name])
					chainsToProcess[name] = runtimeConf
				} else {
					logger.Printf("Warning: Chain '%s' from 'chains_to_snapshot' not found in registry.", name)
					// Check if it exists as an override-only entry
					if override, ok := configOverrides[name]; ok {
						runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
						if runtimeConf != nil {
							chainsToProcess[name] = runtimeConf
						}
					}
				}
			}
		} else { // Registry failed, but we have a specific list - try overrides only
			for _, name := range config.ChainsToSnapshot {
				if override, ok := configOverrides[name]; ok {
					runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
					if runtimeConf != nil {
						chainsToProcess[name] = runtimeConf
					}
				} else {
					logger.Printf("Warning: Cannot process chain '%s' - not found in registry (or registry failed) and no override found.", name)
				}
			}
		}
		// Also add override-only chains not in the snapshot list
		for name, override := range configOverrides {
			if _, exists := chainsToProcess[name]; !exists {
				runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
				if runtimeConf != nil {
					chainsToProcess[name] = runtimeConf
				}
			}
		}
	} else {
		logger.Printf("Processing only chains explicitly defined in config [[chains]] blocks...")
		// Only process chains from the [[chains]] overrides section
		for name, override := range configOverrides {
			runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
			// Try lookup in registry if seeds/chainid missing
			if runtimeConf != nil {
				if len(runtimeConf.SeedNodesP2P) == 0 || runtimeConf.ChainID == "" {
					logger.Printf("Explicit chain '%s' missing ChainID or Seeds, attempting registry lookup...", name)
					if registryChains != nil { // Check if registry loaded
						if basicInfo, ok := registryChains[name]; ok {
							if runtimeConf.ChainID == "" {
								runtimeConf.ChainID = basicInfo.ChainID
							}
							if len(runtimeConf.SeedNodesP2P) == 0 {
								runtimeConf.SeedNodesP2P = getP2PAddresses(basicInfo.Peers.Seeds)
							}
							if runtimeConf.Name == "" {
								runtimeConf.Name = basicInfo.PrettyName
							}
						} else {
							logger.Printf("Warning: Cannot find registry info for explicitly defined chain '%s' to fill missing details.", name)
						}
					} else {
						logger.Printf("Warning: Registry not loaded, cannot fill missing details for '%s'.", name)
					}
				}
				// Final check if essential info is present
				if runtimeConf.ChainID != "" { // SeedNodes might be empty if only direct endpoints are given
					chainsToProcess[name] = runtimeConf
				} else {
					logger.Printf("Warning: Skipping explicitly defined chain '%s' due to missing ChainID.", name)
				}
			}
		}
	}

	// --- Launch Goroutines ---
	var wg sync.WaitGroup
	finalChainCount := 0
	for _, runtimeConf := range chainsToProcess {
		if runtimeConf.Enabled {
			finalChainCount++
			// Create copies for goroutines
			rtConfCopy := *runtimeConf
			globalConfCopy := *config

			wg.Add(1)
			go func(crc ChainRuntimeConfig, gc Config) {
				defer wg.Done()
				// Pass ChainRuntimeConfig now
				runChainSnapshotter(ctx, &crc, &gc, ipfs)
			}(rtConfCopy, globalConfCopy)

			wg.Add(1)
			go func(crc ChainRuntimeConfig, gc Config) {
				defer wg.Done()
				// Pass ChainRuntimeConfig now
				runPerChainPruning(ctx, crc, &gc, ipfs)
			}(rtConfCopy, globalConfCopy)
		} else {
			logger.Printf("Skipping disabled chain: %s (%s)", runtimeConf.Name, runtimeConf.ChainID)
		}
	}

	if finalChainCount == 0 {
		logger.Printf("Warning: No chains enabled or configured for processing.")
	} else {
		logger.Printf("Launched snapshot/pruning goroutines for %d enabled chains.", finalChainCount)
	}

	// Mutual pinning is disabled
	// go manageMutualPinning(ctx, ipfs, config)

	<-ctx.Done()
	logger.Printf("Daemon shutting down...")
	wg.Wait()
	logger.Printf("All chain processors stopped.")
	return ctx.Err()
}

// --- Helper Functions for Config Merging ---

// createRuntimeConfig merges registry info and overrides from config.toml [[chains]]
func createRuntimeConfig(basicInfo *BasicChainInfo, globalConfig *Config, override *ChainOverrideConfig) *ChainRuntimeConfig {
	rt := &ChainRuntimeConfig{
		RegistryName: basicInfo.RegistryName,
		Name:         basicInfo.PrettyName,
		ChainID:      basicInfo.ChainID,
		// Defaults from global config
		Enabled:                     true, // Default to enabled if selected
		SnapshotInterval:            globalConfig.GlobalSnapshotInterval,
		SnapshotIntervalRaw:         globalConfig.GlobalSnapshotIntervalRaw,
		MaxSnapshotsToKeepPerChain:  globalConfig.GlobalMaxSnapshotsToKeep,
		PruneInterval:               globalConfig.GlobalPruneInterval,
		EnablePeerDiscoveryFallback: true, // Always enable peer discovery
		// Data from registry
		SeedNodesP2P: getP2PAddresses(basicInfo.Peers.Seeds), // Add persistent peers too if available
	}

	// Add persistent peers if available
	if len(basicInfo.Peers.PersistentPeers) > 0 {
		rt.SeedNodesP2P = append(rt.SeedNodesP2P, getP2PAddresses(basicInfo.Peers.PersistentPeers)...)
	}

	// Apply overrides
	if override != nil {
		if override.Enabled != nil {
			rt.Enabled = *override.Enabled
		}
		if override.Name != "" {
			rt.Name = override.Name
		} // Allow overriding pretty name
		if len(override.RPCEndpoints) > 0 {
			rt.RPCEndpoints = override.RPCEndpoints
		}
		if len(override.RESTEndpoints) > 0 {
			rt.RESTEndpoints = override.RESTEndpoints
		}
		if len(override.SeedNodesP2P) > 0 {
			rt.SeedNodesP2P = override.SeedNodesP2P
		}
		if override.EnablePeerDiscoveryFallback != nil {
			rt.EnablePeerDiscoveryFallback = *override.EnablePeerDiscoveryFallback
		}
		if override.SnapshotInterval > 0 {
			rt.SnapshotInterval = override.SnapshotInterval
		}
		if override.SnapshotIntervalRaw != "" {
			rt.SnapshotIntervalRaw = override.SnapshotIntervalRaw
		} // Override raw too
		if override.MaxSnapshotsToKeepPerChain != nil && *override.MaxSnapshotsToKeepPerChain >= 0 {
			rt.MaxSnapshotsToKeepPerChain = *override.MaxSnapshotsToKeepPerChain
		}
		if override.PruneInterval > 0 {
			rt.PruneInterval = override.PruneInterval
		}
	}

	// Always ensure peer discovery is enabled when we have seed nodes
	if len(rt.SeedNodesP2P) > 0 {
		rt.EnablePeerDiscoveryFallback = true
	}

	return rt
}

// createRuntimeConfigFromOverrideOnly creates a runtime config based *only* on a [[chains]] block
func createRuntimeConfigFromOverrideOnly(override *ChainOverrideConfig, globalConfig *Config) *ChainRuntimeConfig {
	if override == nil {
		return nil
	}

	// Essential info MUST be in the override if not in registry
	if override.ChainID == "" {
		logger.Printf("Warning: Skipping chain '%s' defined only in config: missing required 'chain_id'.", override.Name)
		return nil
	}
	// Need seeds or direct endpoints if not relying on registry lookup
	if len(override.SeedNodesP2P) == 0 && len(override.RPCEndpoints) == 0 && len(override.RESTEndpoints) == 0 {
		logger.Printf("Warning: Skipping chain '%s' defined only in config: must provide 'seed_nodes_p2p', 'rpc_endpoints', or 'rest_endpoints'.", override.Name)
		return nil
	}

	rt := &ChainRuntimeConfig{
		RegistryName: override.Name, // Use config name as key
		Name:         override.Name, // Use config name unless overridden later?
		ChainID:      override.ChainID,
		// Defaults from global config, overridden by specific chain block
		Enabled:                     true, // Default enabled unless explicitly set false
		SnapshotInterval:            globalConfig.GlobalSnapshotInterval,
		SnapshotIntervalRaw:         globalConfig.GlobalSnapshotIntervalRaw,
		MaxSnapshotsToKeepPerChain:  globalConfig.GlobalMaxSnapshotsToKeep,
		PruneInterval:               globalConfig.GlobalPruneInterval,
		EnablePeerDiscoveryFallback: true, // Default true
		SeedNodesP2P:                override.SeedNodesP2P,
		RPCEndpoints:                override.RPCEndpoints,
		RESTEndpoints:               override.RESTEndpoints,
	}

	// Apply non-empty overrides from the struct
	if override.Enabled != nil {
		rt.Enabled = *override.Enabled
	}
	if override.SnapshotInterval > 0 {
		rt.SnapshotInterval = override.SnapshotInterval
	}
	if override.SnapshotIntervalRaw != "" {
		rt.SnapshotIntervalRaw = override.SnapshotIntervalRaw
	}
	if override.MaxSnapshotsToKeepPerChain != nil && *override.MaxSnapshotsToKeepPerChain >= 0 {
		rt.MaxSnapshotsToKeepPerChain = *override.MaxSnapshotsToKeepPerChain
	}
	if override.PruneInterval > 0 {
		rt.PruneInterval = override.PruneInterval
	}
	if override.EnablePeerDiscoveryFallback != nil {
		rt.EnablePeerDiscoveryFallback = *override.EnablePeerDiscoveryFallback
	}

	// Disable peer discovery fallback if no seeds are available
	if len(rt.SeedNodesP2P) == 0 {
		rt.EnablePeerDiscoveryFallback = false
	}

	return rt
}

// --- Snapshotting and Pruning Logic ---
// (runChainSnapshotter, takeAndProcessSnapshotForChain, runPerChainPruning, pruneOldSnapshotsForChain)
// These functions now accept ChainRuntimeConfig

// Updated runChainSnapshotter to accept ChainRuntimeConfig
func runChainSnapshotter(ctx context.Context, chainConfig *ChainRuntimeConfig, globalConfig *Config, ipfs *IPFSNode) {
	logger.Printf("[%s / %s] Starting snapshotter. Interval: %s", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotIntervalRaw)

	isContinuous := chainConfig.SnapshotIntervalRaw == "0s" || chainConfig.SnapshotInterval == 0
	var ticker *time.Ticker
	if !isContinuous {
		if chainConfig.SnapshotInterval <= 0 {
			logger.Printf("[%s / %s] Warning: Invalid non-zero snapshot interval %s, defaulting to 4h.", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotInterval)
			chainConfig.SnapshotInterval = 4 * time.Hour // Fallback
		}
		ticker = time.NewTicker(chainConfig.SnapshotInterval)
		defer ticker.Stop()
	} else {
		logger.Printf("[%s / %s] Running in continuous mode.", chainConfig.Name, chainConfig.ChainID)
	}

	// Initial snapshot attempt
	logger.Printf("[%s / %s] Attempting initial snapshot...", chainConfig.Name, chainConfig.ChainID)
	if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
		logger.Printf("[%s / %s] Error during initial snapshot: %v", chainConfig.Name, chainConfig.ChainID, err)
	}

	// Main loop (logic remains the same, just uses ChainRuntimeConfig)
	for {
		var delay time.Duration
		if isContinuous {
			err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs)
			if err != nil {
				logger.Printf("[%s / %s] Error in continuous snapshot: %v. Retrying after 1 minute.", chainConfig.Name, chainConfig.ChainID, err)
				delay = 1 * time.Minute
			} else {
				delay = 1 * time.Second
			}
		} else {
			select {
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping snapshotter (timer).", chainConfig.Name, chainConfig.ChainID)
				return
			case <-ticker.C:
				if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
					logger.Printf("[%s / %s] Error taking scheduled snapshot: %v", chainConfig.Name, chainConfig.ChainID, err)
				}
			}
		}
		if delay > 0 {
			select {
			case <-time.After(delay): // continue loop
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping continuous snapshotter during delay.", chainConfig.Name, chainConfig.ChainID)
				return
			}
		} else {
			select {
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping snapshotter loop.", chainConfig.Name, chainConfig.ChainID)
				return
			default: // Non-blocking check
			}
		}
	}
}

// Updated takeAndProcessSnapshotForChain to accept ChainRuntimeConfig
func takeAndProcessSnapshotForChain(ctx context.Context, chainConfig *ChainRuntimeConfig, globalConfig *Config, ipfs *IPFSNode) error {
	logger.Printf("[%s / %s] Checking for new blocks...", chainConfig.Name, chainConfig.ChainID)

	// Need to adapt getHealthyEndpoint to work with ChainRuntimeConfig
	rpcURL, err := getHealthyEndpointRuntime(ctx, chainConfig, "rpc") // New helper? Or adapt existing one
	if err != nil {
		return fmt.Errorf("[%s] no healthy RPC endpoint found: %w", chainConfig.ChainID, err)
	}

	height, err := getLatestBlockHeight(rpcURL)
	if err != nil {
		return fmt.Errorf("[%s] failed to get latest block height from %s: %w", chainConfig.ChainID, rpcURL, err)
	}

	if height <= chainConfig.LastSuccessfulSnapshotHeight && chainConfig.LastSuccessfulSnapshotHeight > 0 {
		return nil // No new blocks
	}
	if height == chainConfig.LastAttemptedSnapshotHeight && chainConfig.LastAttemptedSnapshotHeight > 0 {
		logger.Printf("[%s / %s] Height %d was already attempted. Skipping.", chainConfig.Name, chainConfig.ChainID, height)
		return nil
	}

	logger.Printf("[%s / %s] Attempting snapshot for height: %d", chainConfig.Name, chainConfig.ChainID, height)
	chainConfig.LastAttemptedSnapshotHeight = height // Mark attempt

	chainSnapshotBaseDir := filepath.Join(globalConfig.SnapshotBaseDir, chainConfig.ChainID) // Use ChainID for folder structure
	snapshotDir, err := ensureSnapshotDir(height, chainSnapshotBaseDir)
	if err != nil {
		return fmt.Errorf("[%s] failed ensure snapshot dir %s: %w", chainConfig.ChainID, chainSnapshotBaseDir, err)
	}

	restURL, err := getHealthyEndpointRuntime(ctx, chainConfig, "rest")
	if err != nil {
		return fmt.Errorf("[%s] no healthy REST endpoint found: %w", chainConfig.ChainID, err)
	}
	logger.Printf("[%s / %s] Using REST endpoint: %s for height %d", chainConfig.Name, chainConfig.ChainID, restURL, height)

	// Pass ChainRuntimeConfig to takeSnapshot
	if err := takeSnapshotRuntime(height, *chainConfig, restURL, snapshotDir); err != nil {
		logger.Printf("[%s / %s] Error during snapshot creation/saving height %d: %v", chainConfig.Name, chainConfig.ChainID, height, err)
		return fmt.Errorf("[%s] failed take snapshot: %w", chainConfig.ChainID, err)
	}

	// Add to IPFS
	cid, err := ipfs.AddPath(snapshotDir)
	if err != nil {
		logger.Printf("[%s / %s] Warning: Snapshot dir %s created but failed add to IPFS: %v", chainConfig.Name, chainConfig.ChainID, snapshotDir, err)
		return fmt.Errorf("failed add snapshot dir %s to IPFS: %w", snapshotDir, err)
	}
	logger.Printf("[%s / %s] Added snapshot height %d to IPFS. CID: %s", chainConfig.Name, chainConfig.ChainID, height, cid)

	// Save CID metadata
	cidFilePath := filepath.Join(snapshotDir, "ipfs_cid.txt")
	if err := os.WriteFile(cidFilePath, []byte(cid), 0644); err != nil {
		logger.Printf("[%s / %s] CRITICAL WARNING: Failed write IPFS CID %s to %s: %v. Pruning cannot unpin!", chainConfig.Name, chainConfig.ChainID, cid, cidFilePath, err)
	}

	// Mark successful
	chainConfig.LastSuccessfulSnapshotHeight = height
	logger.Printf("[%s / %s] Successfully processed snapshot height %d (CID: %s).", chainConfig.Name, chainConfig.ChainID, height, cid)
	return nil
}

// Need to adapt getHealthyEndpoint to use ChainRuntimeConfig
// Renaming slightly to avoid conflict if old one is kept temporarily
func getHealthyEndpointRuntime(ctx context.Context, chainConfig *ChainRuntimeConfig, endpointType string) (string, error) {
	state := getChainEndpointsState(chainConfig.ChainID) // Use ChainID for state cache key
	state.mu.Lock()
	defer state.mu.Unlock()

	var candidates []DiscoveredEndpoint

	// Use endpoints directly from the ChainRuntimeConfig first (these are from overrides or registry hints)
	if endpointType == "rpc" {
		for _, url := range chainConfig.RPCEndpoints {
			candidates = append(candidates, DiscoveredEndpoint{Address: url, Type: "rpc", Source: "config/registry_hint"})
		}
		candidates = append(candidates, state.RPCEndpoints...) // Add previously discovered/cached
	} else {
		for _, url := range chainConfig.RESTEndpoints {
			candidates = append(candidates, DiscoveredEndpoint{Address: url, Type: "rest", Source: "config/registry_hint"})
		}
		candidates = append(candidates, state.RESTEndpoints...) // Add previously discovered/cached
	}

	// Deduplicate candidates based on Address
	uniqueCandidates := make([]DiscoveredEndpoint, 0, len(candidates))
	seenAddr := make(map[string]bool)
	for _, c := range candidates {
		if !seenAddr[c.Address] {
			uniqueCandidates = append(uniqueCandidates, c)
			seenAddr[c.Address] = true
		}
	}
	candidates = uniqueCandidates

	// Try candidates
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	for _, ep := range candidates {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var err error
		if endpointType == "rpc" {
			err = checkRPC(checkCtx, ep.Address, chainConfig.ChainID)
		} else {
			err = checkREST(checkCtx, ep.Address, chainConfig.ChainID)
		}
		cancel()
		if err == nil {
			// logger.Printf("[%s] Using healthy %s endpoint: %s (source: %s)", chainConfig.ChainID, endpointType, ep.Address, ep.Source)
			return ep.Address, nil
		}
		// logger.Printf("[%s] Endpoint %s (%s) failed health check: %v", chainConfig.ChainID, ep.Address, endpointType, err)
	}

	// Attempt discovery if enabled and needed
	shouldDiscover := chainConfig.EnablePeerDiscoveryFallback && (time.Since(state.lastDiscovery) > 15*time.Minute || len(candidates) == 0)

	if shouldDiscover {
		logger.Printf("[%s] No healthy %s endpoint found in cache/hints. Attempting peer discovery.", chainConfig.ChainID, endpointType)
		state.lastDiscovery = time.Now()
		state.mu.Unlock() // Unlock during discovery
		// Use seeds from runtime config (came from registry or override)
		discovered, discErr := DiscoverEndpoints(ctx, chainConfig.ChainID, chainConfig.RPCEndpoints, chainConfig.SeedNodesP2P)
		state.mu.Lock() // Re-lock

		if discErr != nil {
			logger.Printf("[%s] Peer discovery for %s failed: %v", chainConfig.ChainID, endpointType, discErr)
		} else {
			var newlyAddedEndpoints []DiscoveredEndpoint
			for _, discEp := range discovered {
				alreadyKnown := false
				for _, known := range candidates {
					if known.Address == discEp.Address {
						alreadyKnown = true
						break
					}
				} // Check against initial candidates too
				if alreadyKnown {
					continue
				}

				if discEp.Type == endpointType {
					newlyAddedEndpoints = append(newlyAddedEndpoints, discEp)
					checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					var checkErr error
					if endpointType == "rpc" {
						checkErr = checkRPC(checkCtx, discEp.Address, chainConfig.ChainID)
					} else {
						checkErr = checkREST(checkCtx, discEp.Address, chainConfig.ChainID)
					}
					cancel()
					if checkErr == nil {
						logger.Printf("[%s] Using newly discovered healthy %s endpoint: %s", chainConfig.ChainID, endpointType, discEp.Address)
						if endpointType == "rpc" {
							state.RPCEndpoints = append(state.RPCEndpoints, discEp)
						} else {
							state.RESTEndpoints = append(state.RESTEndpoints, discEp)
						}
						return discEp.Address, nil
					}
					// logger.Printf("[%s] Discovered endpoint %s (%s) failed check: %v", chainConfig.ChainID, discEp.Address, endpointType, checkErr)
				}
			}
			// Add newly found but potentially unhealthy endpoints to cache
			if endpointType == "rpc" {
				state.RPCEndpoints = append(state.RPCEndpoints, newlyAddedEndpoints...)
			} else {
				state.RESTEndpoints = append(state.RESTEndpoints, newlyAddedEndpoints...)
			}
			if len(newlyAddedEndpoints) == 0 {
				logger.Printf("[%s] Peer discovery ran, no new usable %s endpoints found.", chainConfig.ChainID, endpointType)
			}
		}
	}
	return "", fmt.Errorf("[%s] no healthy %s endpoint found after all attempts", chainConfig.ChainID, endpointType)
}

// Updated runPerChainPruning to accept ChainRuntimeConfig
func runPerChainPruning(ctx context.Context, chainConfig ChainRuntimeConfig, globalConfig *Config, ipfs *IPFSNode) {
	logger.Printf("[%s / %s] Starting pruning service. Interval: %s, Keep: %d",
		chainConfig.Name, chainConfig.ChainID, chainConfig.PruneInterval, chainConfig.MaxSnapshotsToKeepPerChain)

	if chainConfig.PruneInterval <= 0 {
		logger.Printf("[%s / %s] Pruning interval zero/negative, disabling.", chainConfig.Name, chainConfig.ChainID)
		return
	}
	ticker := time.NewTicker(chainConfig.PruneInterval)
	defer ticker.Stop()

	// Initial run
	select {
	case <-time.After(1 * time.Minute):
		logger.Printf("[%s / %s] Running initial pruning check...", chainConfig.Name, chainConfig.ChainID)
		if err := pruneOldSnapshotsForChain(ipfs, globalConfig, &chainConfig); err != nil { // Pass pointer for modification? No, pruning doesn't modify runtime conf.
			logger.Printf("[%s / %s] Error initial pruning: %v", chainConfig.Name, chainConfig.ChainID, err)
		}
	case <-ctx.Done():
		logger.Printf("[%s / %s] Stopping pruning before initial run.", chainConfig.Name, chainConfig.ChainID)
		return
	}

	// Scheduled runs
	for {
		select {
		case <-ctx.Done():
			logger.Printf("[%s / %s] Stopping pruning service.", chainConfig.Name, chainConfig.ChainID)
			return
		case <-ticker.C:
			logger.Printf("[%s / %s] Running scheduled pruning...", chainConfig.Name, chainConfig.ChainID)
			if err := pruneOldSnapshotsForChain(ipfs, globalConfig, &chainConfig); err != nil {
				logger.Printf("[%s / %s] Error scheduled pruning: %v", chainConfig.Name, chainConfig.ChainID, err)
			}
		}
	}
}

// Updated pruneOldSnapshotsForChain to accept ChainRuntimeConfig
func pruneOldSnapshotsForChain(ipfs *IPFSNode, config *Config, chainConfig *ChainRuntimeConfig) error { // Accepts pointer or value? Value seems fine.
	if chainConfig.MaxSnapshotsToKeepPerChain <= 0 {
		return nil
	} // Pruning disabled

	logger.Printf("[%s / %s] Pruning check (keep %d)...", chainConfig.Name, chainConfig.ChainID, chainConfig.MaxSnapshotsToKeepPerChain)
	chainSnapshotBaseDir := filepath.Join(config.SnapshotBaseDir, chainConfig.ChainID) // Use ChainID for path
	entries, err := os.ReadDir(chainSnapshotBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed read dir %s: %w", chainSnapshotBaseDir, err)
	}

	type snapshotInfo struct {
		path    string
		height  int64
		modTime time.Time
	}
	var snapshots []snapshotInfo
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "height_") {
			continue
		}
		var height int64
		if _, errS := fmt.Sscanf(entry.Name(), "height_%d", &height); errS != nil {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			continue
		}
		snapshots = append(snapshots, snapshotInfo{path: filepath.Join(chainSnapshotBaseDir, entry.Name()), height: height, modTime: info.ModTime()})
	}

	if len(snapshots) <= chainConfig.MaxSnapshotsToKeepPerChain {
		return nil
	} // Within limit

	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].height > snapshots[j].height }) // Newest first

	snapshotsToPrune := snapshots[chainConfig.MaxSnapshotsToKeepPerChain:]
	logger.Printf("[%s / %s] Found %d snapshots, pruning %d oldest.", chainConfig.Name, chainConfig.ChainID, len(snapshots), len(snapshotsToPrune))

	for _, snapshot := range snapshotsToPrune {
		logger.Printf("[%s / %s] Pruning height %d (%s)", chainConfig.Name, chainConfig.ChainID, snapshot.height, snapshot.path)
		cidFilePath := filepath.Join(snapshot.path, "ipfs_cid.txt")
		cidBytes, err := os.ReadFile(cidFilePath)
		if err != nil {
			logger.Printf("[%s / %s] Warning: Failed read CID file %s: %v. Cannot unpin.", chainConfig.Name, chainConfig.ChainID, cidFilePath, err)
		} else {
			cidToUnpin := strings.TrimSpace(string(cidBytes))
			if cidToUnpin != "" {
				if err := ipfs.UnpinCID(cidToUnpin); err != nil {
					logger.Printf("[%s / %s] Warning: Failed unpin CID %s: %v", chainConfig.Name, chainConfig.ChainID, cidToUnpin, err)
				} else {
					logger.Printf("[%s / %s] Unpinned CID %s", chainConfig.Name, chainConfig.ChainID, cidToUnpin)
				}
			} else {
				logger.Printf("[%s / %s] Warning: CID file %s empty.", chainConfig.Name, chainConfig.ChainID, cidFilePath)
			}
		}
		if err := os.RemoveAll(snapshot.path); err != nil {
			logger.Printf("[%s / %s] Error removing dir %s: %v", chainConfig.Name, chainConfig.ChainID, snapshot.path, err)
		}
	}
	return nil
}

// getDirSize remains the same

// manageMutualPinning remains disabled - Removed function definition
// func manageMutualPinning(ctx context.Context, node *IPFSNode, config *Config) {
// 	logger.Printf("Mutual pinning service is currently disabled.")
// 	<-ctx.Done()
// 	logger.Printf("Mutual pinning service stopped.")
// }

// Need matching takeSnapshotRuntime function (can be in snapshot_core.go)
// Need getLatestBlockHeight definition that matches call
// Need checkRPC/checkREST definitions (can be in peer_discovery.go)
// Need ensureSnapshotDir definition (can be in snapshot_utils.go)
