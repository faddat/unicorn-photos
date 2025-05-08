package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	comethttp "github.com/cometbft/cometbft/rpc/client/http"
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

func runDaemon(ctx context.Context, chainNames []string) error {
	// Create a done channel to signal when shutdown is complete
	doneChan := make(chan struct{})

	// Apply a global timeout to the context to ensure we don't hang forever
	// User might set a very long timeout or run without one, this is a safety net.
	// For now, let daemon context be managed by main.go's signal handling.
	// ctx, cancel := context.WithTimeout(ctx, 2*time.Hour) // 2 hour max runtime
	// defer cancel()

	// Setup a separate goroutine to monitor for context cancellation (handled by main.go)
	// go func() {
	// 	<-ctx.Done()
	// 	logger.Printf("Context canceled, initiating graceful shutdown...")
	// }()

	// Debug output to verify chainNames parameter
	// Using logger for consistency, assuming logger is set up.
	// If these are needed before logger fully setup, fmt is okay.
	logger.Printf("DEBUG: Chain names received: %v", chainNames)

	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	logger.Printf("DEBUG: Before override - config.AllChains=%v, config.ChainsToSnapshot=%v",
		config.AllChains, config.ChainsToSnapshot)

	logger.Printf("Starting Unicorn Photos IPFS snapshot daemon")

	// Initialize and start the status tracker
	statusTracker := GetStatusTracker()
	// IMPORTANT: Set StatusFilePath *before* LoadStatusFile or Start
	statusTracker.StatusFilePath = filepath.Join(config.SnapshotBaseDir, "status.json")

	if err := statusTracker.LoadStatusFile(); err != nil {
		logger.Printf("Warning: Failed to load existing status file (%s): %v. Starting fresh.", statusTracker.StatusFilePath, err)
		// If load fails, statusTracker.LoadStatusFile already resets to a fresh state.
		// Ensure StartTime is current for a "fresh start".
		statusTracker.StartTime = time.Now()
		statusTracker.LastUpdateTime = time.Now()
	}
	statusTracker.Start()      // Start periodic saving and logging.
	defer statusTracker.Stop() // Ensure the status tracker is stopped when daemon exits.

	// If chainNames is provided via CLI, override config settings
	if len(chainNames) > 0 {
		logger.Printf("DEBUG: Setting command line chains: %v", chainNames)
		logger.Printf("Overriding config with command-line specified chains: %v", chainNames)
		config.AllChains = false
		config.ChainsToSnapshot = chainNames
		logger.Printf("DEBUG: After override - config.AllChains=%v, config.ChainsToSnapshot=%v",
			config.AllChains, config.ChainsToSnapshot)
	} else if config.AllChains {
		logger.Printf("Processing all chains from the registry (all_chains=true)")
	} else if len(config.ChainsToSnapshot) > 0 {
		logger.Printf("Processing specific chains from config.toml: %v", config.ChainsToSnapshot)
	} else {
		logger.Printf("No chains specified. Will only process chains explicitly defined in config.toml [[chains]] section, if any.")
	}

	// --- Determine and Manage Chain Registry Path ---
	userHome, err := os.UserHomeDir()
	if err != nil {
		logger.Printf("Warning: Could not determine user home directory: %v", err)
	}

	registryPathInput := config.ChainRegistryPath
	effectiveRegistryPath := registryPathInput
	useManagedDefault := false
	defaultManagedPath := ""

	if userHome != "" {
		defaultManagedPath = filepath.Join(userHome, ".chain-registry")
		if strings.HasPrefix(registryPathInput, "~/") {
			effectiveRegistryPath = filepath.Join(userHome, registryPathInput[2:])
		}
	}
	effectiveRegistryPath = filepath.Clean(effectiveRegistryPath)

	if effectiveRegistryPath == defaultManagedPath || strings.Contains(effectiveRegistryPath, ".chain-registry") { // Second part is heuristic
		useManagedDefault = true
	}

	registryPathToLoad := effectiveRegistryPath

	if useManagedDefault {
		logger.Printf("Managing chain registry at location: %s", effectiveRegistryPath)
		registryPathToLoad = effectiveRegistryPath
		_, statErr := os.Stat(effectiveRegistryPath)
		if os.IsNotExist(statErr) {
			if err := os.MkdirAll(filepath.Dir(effectiveRegistryPath), 0755); err != nil {
				logger.Printf("ERROR: Failed to create parent directory for chain registry %s: %v", filepath.Dir(effectiveRegistryPath), err)
			}
			logger.Printf("Cloning cosmos/chain-registry to %s...", effectiveRegistryPath)
			gitErr := runGitCommand(ctx, 2*time.Minute, "", "clone", "https://github.com/cosmos/chain-registry", effectiveRegistryPath) // Pass effectiveRegistryPath as clone target
			if gitErr != nil {
				logger.Printf("ERROR: Failed to clone chain registry: %v. Registry data may be unavailable.", gitErr)
			} else {
				logger.Printf("Chain registry successfully cloned.")
			}
		} else if statErr == nil {
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
		} else {
			logger.Printf("Error checking chain registry path %s: %v. Trying to proceed.", effectiveRegistryPath, statErr)
		}
	} else {
		logger.Printf("Using user-specified chain registry path: %s", effectiveRegistryPath)
	}

	logger.Printf("Snapshot Base Directory: %s", config.SnapshotBaseDir)
	logger.Printf("Chain Registry Path for Loading: %s", registryPathToLoad) // Log the path used for loading
	logger.Printf("Snapshot All Chains: %t", config.AllChains)
	if !config.AllChains {
		logger.Printf("Specific Chains to Snapshot: %v", config.ChainsToSnapshot)
	}

	resolvedIPFSPath := config.IPFSRepoPath
	if strings.HasPrefix(resolvedIPFSPath, "~/") {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return fmt.Errorf("failed to resolve home directory for IPFS path: %w", homeErr)
		}
		resolvedIPFSPath = filepath.Join(home, resolvedIPFSPath[2:])
	}
	resolvedIPFSPath = filepath.Clean(resolvedIPFSPath)
	logger.Printf("Using IPFS repository at: %s", resolvedIPFSPath)
	config.IPFSRepoPath = resolvedIPFSPath

	var ipfs *IPFSNode
	var initErr error
	for attempts := 0; attempts < 3; attempts++ {
		logger.Printf("Attempting IPFS initialization (attempt %d/3)...", attempts+1)
		ipfsCtx, cancelInit := context.WithTimeout(ctx, 30*time.Second)
		ipfs, initErr = NewIPFSNode(ipfsCtx, config)
		cancelInit()
		if initErr == nil {
			break
		}
		logger.Printf("IPFS initialization attempt %d failed: %v. Retrying...", attempts+1, initErr)
		select {
		case <-time.After(2 * time.Second): // Wait before retry
		case <-ctx.Done():
			return fmt.Errorf("IPFS initialization interrupted: %w", ctx.Err())
		}
		lockFile := filepath.Join(resolvedIPFSPath, "repo.lock")
		if _, statErr := os.Stat(lockFile); statErr == nil {
			logger.Printf("Attempting to remove IPFS lock file: %s", lockFile)
			os.Remove(lockFile)
		}
	}
	if initErr != nil {
		return fmt.Errorf("failed to initialize IPFS node after 3 attempts: %v", initErr)
	}
	// Defer IPFS node close until the very end of runDaemon
	// defer func() {
	// 	if ipfs != nil {
	// 		if errClose := ipfs.Close(); errClose != nil {
	// 			logger.Printf("Error closing IPFS node: %v", errClose)
	// 		}
	// 	}
	// }()
	logger.Printf("IPFS node initialized")

	chainsToProcess := make(map[string]*ChainRuntimeConfig)
	configOverrides := make(map[string]*ChainOverrideConfig)
	for i := range config.ChainOverrides {
		configOverrides[config.ChainOverrides[i].Name] = &config.ChainOverrides[i]
	}

	var registryChains map[string]*BasicChainInfo
	if config.AllChains || len(config.ChainsToSnapshot) > 0 {
		logger.Printf("DEBUG: Loading registry chains. AllChains=%v, ChainsToSnapshot=%v",
			config.AllChains, config.ChainsToSnapshot)
		registryChains, err = LoadRegistryChains(registryPathToLoad, "mainnet")
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
		for name, override := range configOverrides {
			if _, exists := chainsToProcess[name]; !exists {
				runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
				if runtimeConf != nil {
					chainsToProcess[name] = runtimeConf
				}
			}
		}
	} else if len(config.ChainsToSnapshot) > 0 {
		requestedChainsSet := make(map[string]bool)
		for _, name := range config.ChainsToSnapshot {
			requestedChainsSet[name] = true
			logger.Printf("Will attempt to process chain: %s (from command line or config.chains_to_snapshot)", name)
		}
		logger.Printf("Processing %d specific chain(s) requested...", len(requestedChainsSet))
		if registryChains != nil {
			for name, basicInfo := range registryChains {
				if requestedChainsSet[name] {
					logger.Printf("Found requested chain '%s' in registry.", name)
					runtimeConf := createRuntimeConfig(basicInfo, config, configOverrides[name])
					chainsToProcess[name] = runtimeConf
					delete(requestedChainsSet, name)
				}
			}
		}
		for name := range requestedChainsSet { // Remaining chains not found in registry
			if override, ok := configOverrides[name]; ok {
				logger.Printf("Using override-only config for chain: %s", name)
				runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
				if runtimeConf != nil {
					chainsToProcess[name] = runtimeConf
				} else {
					logger.Printf("Warning: Invalid or incomplete override config for chain '%s', skipping.", name)
				}
			} else {
				logger.Printf("Warning: Requested chain '%s' not found in registry or config overrides, skipping.", name)
			}
		}
	} else { // Not all_chains and no chains_to_snapshot, so only from [[chains]]
		logger.Printf("Processing only chains explicitly defined in config [[chains]] blocks...")
		for name, override := range configOverrides {
			runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
			if runtimeConf != nil {
				if (len(runtimeConf.SeedNodesP2P) == 0 && len(runtimeConf.RPCEndpoints) == 0) || runtimeConf.ChainID == "" { // Heuristic: if critical info missing from override
					logger.Printf("Explicit chain '%s' missing ChainID or Seed/RPC Endpoints, attempting registry lookup for enrichment...", name)
					if registryChains != nil {
						if basicInfo, ok := registryChains[name]; ok {
							if runtimeConf.ChainID == "" {
								runtimeConf.ChainID = basicInfo.ChainID
							}
							if len(runtimeConf.SeedNodesP2P) == 0 { // Only get P2P seeds from registry
								runtimeConf.SeedNodesP2P = getP2PAddresses(basicInfo.Peers.Seeds)
								// Extract RPC endpoints for peer discovery only
								runtimeConf.SeedRPCsForDiscovery = getRPCAddresses(basicInfo.APIs.RPC)
							}
							if runtimeConf.Name == "" || runtimeConf.Name == name {
								runtimeConf.Name = basicInfo.PrettyName
							} // Prefer pretty name
						} else {
							logger.Printf("Warning: Cannot find registry info for explicitly defined chain '%s' to fill missing details.", name)
						}
					} else {
						logger.Printf("Warning: Registry not loaded, cannot enrich missing details for '%s'.", name)
					}
				}
				if runtimeConf.ChainID != "" && (len(runtimeConf.SeedNodesP2P) > 0 || len(runtimeConf.RPCEndpoints) > 0) {
					chainsToProcess[name] = runtimeConf
				} else {
					logger.Printf("Warning: Skipping explicitly defined chain '%s' due to missing ChainID or connectivity info (Seeds/RPCs).", name)
				}
			}
		}
	}

	logger.Printf("Preparing to snapshot %d total chains after initial filtering and config parsing.", len(chainsToProcess))

	// This section for command-line chain names acts as a final filter on chainsToProcess.
	// The previous logic already considers config.ChainsToSnapshot.
	// `chainNames` comes from CLI args. If it's non-empty, it's the definitive list.
	if len(chainNames) > 0 { // This means CLI args were given for specific chains
		filteredChains := make(map[string]*ChainRuntimeConfig)
		for _, requestedName := range chainNames {
			if chain, exists := chainsToProcess[requestedName]; exists {
				filteredChains[requestedName] = chain
				logger.Printf("Including explicitly requested chain from CLI: %s", requestedName)
			} else {
				logger.Printf("Warning: Chain '%s' requested via CLI not found in loadable configurations, skipping.", requestedName)
			}
		}
		chainsToProcess = filteredChains // Replace with the CLI specified list
		logger.Printf("Filtered to %d chain(s) based on command-line arguments.", len(chainsToProcess))
	}

	// Register all chains with the status tracker (AFTER all filtering)
	for _, rtConfig := range chainsToProcess { // Use registryName as ID for consistency
		if rtConfig.Enabled {
			// Use rtConfig.RegistryName as the unique key for status tracking.
			// rtConfig.Name is the pretty name.
			statusTracker.RegisterChain(rtConfig.RegistryName, rtConfig.Name)
		} else {
			logger.Printf("Skipping registration of disabled chain: %s (%s)", rtConfig.Name, rtConfig.ChainID)
		}
	}
	// At this point, the deadlock should be fixed, so RegisterChain will not hang.

	var wg sync.WaitGroup
	finalChainCount := 0 // Count of chains for which goroutines are launched.

	select {
	case <-ctx.Done():
		logger.Printf("Shutdown requested before starting chain processors.")
		return ctx.Err()
	default:
	}

	for _, runtimeConf := range chainsToProcess { // Iterate over the final set of chains
		if runtimeConf.Enabled {
			finalChainCount++
			rtConfCopy := *runtimeConf // Important: copy loop variable for goroutine
			globalConfCopy := *config

			select {
			case <-ctx.Done():
				logger.Printf("Shutdown requested during goroutine creation for chain %s.", runtimeConf.RegistryName)
				goto waitForGoroutines
			default:
			}

			wg.Add(1)
			go func(crc ChainRuntimeConfig, gc Config) {
				defer wg.Done()
				runChainSnapshotter(ctx, &crc, &gc, ipfs)
			}(rtConfCopy, globalConfCopy)

			wg.Add(1)
			go func(crc ChainRuntimeConfig, gc Config) {
				defer wg.Done()
				runPerChainPruning(ctx, crc, &gc, ipfs)
			}(rtConfCopy, globalConfCopy)
		} else {
			// This was already logged during registration or filtering steps.
			// logger.Printf("Skipping disabled chain: %s (%s)", runtimeConf.Name, runtimeConf.ChainID)
		}
	}

	if finalChainCount == 0 {
		logger.Printf("Warning: No chains enabled or configured for processing. Daemon will idle.")
	} else {
		logger.Printf("Launched snapshot/pruning goroutines for %d enabled chains.", finalChainCount)
	}

waitForGoroutines:
	go func() {
		wg.Wait()
		logger.Printf("All chain processors and pruners have stopped.")
		statusTracker.LogStatusSummary() // Final summary
		if ipfs != nil {
			logger.Printf("Closing IPFS node...")
			if errClose := ipfs.Close(); errClose != nil {
				logger.Printf("Error closing IPFS node: %v", errClose)
			} else {
				logger.Printf("IPFS node closed successfully.")
			}
		}
		close(doneChan) // Signal that all cleanup is complete
	}()

	select {
	case <-ctx.Done(): // Triggered by SIGINT/SIGTERM via main.go
		logger.Printf("Daemon shutting down (context canceled)... Waiting for cleanup.")
	case <-doneChan: // All goroutines finished, IPFS closed.
		logger.Printf("Daemon completed its work and all goroutines finished.")
		return nil // Normal exit if all work is done (e.g. if not continuous)
	}

	// If context was canceled, wait for doneChan or timeout.
	select {
	case <-doneChan:
		logger.Printf("Graceful shutdown completed.")
	case <-time.After(30 * time.Second): // Fallback timeout
		logger.Printf("WARNING: Forced exit after timeout waiting for graceful shutdown of goroutines.")
	}

	return ctx.Err() // Return the context error (e.g., context.Canceled)
}

// --- Helper Functions for Config Merging ---

// createRuntimeConfig merges registry info and overrides from config.toml [[chains]]
func createRuntimeConfig(basicInfo *BasicChainInfo, globalConfig *Config, override *ChainOverrideConfig) *ChainRuntimeConfig {
	rt := &ChainRuntimeConfig{
		RegistryName: basicInfo.RegistryName,
		Name:         basicInfo.PrettyName,
		ChainID:      basicInfo.ChainID,
		// Defaults from global config
		Enabled:                     true, // Default to enabled if selected from registry
		SnapshotInterval:            globalConfig.GlobalSnapshotInterval,
		SnapshotIntervalRaw:         globalConfig.GlobalSnapshotIntervalRaw,
		MaxSnapshotsToKeepPerChain:  globalConfig.GlobalMaxSnapshotsToKeep,
		PruneInterval:               globalConfig.GlobalPruneInterval,
		EnablePeerDiscoveryFallback: true, // Default to true
		// Data from registry
		SeedNodesP2P: getP2PAddresses(basicInfo.Peers.Seeds),
		// Extract RPC endpoints from registry but only use them for peer discovery via /net_info
		// This ensures we rely on dynamically discovered endpoints for operations
		SeedRPCsForDiscovery: getRPCAddresses(basicInfo.APIs.RPC),
		// Empty configs for actual operations - will be filled by discovery
		RPCEndpoints:  []string{},
		RESTEndpoints: []string{},
	}

	if len(basicInfo.Peers.PersistentPeers) > 0 {
		rt.SeedNodesP2P = append(rt.SeedNodesP2P, getP2PAddresses(basicInfo.Peers.PersistentPeers)...)
		// Deduplicate if necessary, though getP2PAddresses itself doesn't
	}

	if override != nil {
		if override.Enabled != nil {
			rt.Enabled = *override.Enabled
		}
		// if override.Name != "" { rt.Name = override.Name } // Allow overriding pretty name
		if override.ChainID != "" {
			rt.ChainID = override.ChainID
		} // Override chain_id if specified
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
		if override.SnapshotIntervalRaw != "" { // Check raw string for "0s" or valid duration
			parsedInterval, err := time.ParseDuration(override.SnapshotIntervalRaw)
			if err == nil {
				rt.SnapshotInterval = parsedInterval
				rt.SnapshotIntervalRaw = override.SnapshotIntervalRaw
			} else {
				logger.Printf("Warning: Invalid snapshot_interval '%s' for chain %s. Using global default. Error: %v", override.SnapshotIntervalRaw, rt.RegistryName, err)
			}
		}
		if override.MaxSnapshotsToKeepPerChain != nil && *override.MaxSnapshotsToKeepPerChain >= 0 {
			rt.MaxSnapshotsToKeepPerChain = *override.MaxSnapshotsToKeepPerChain
		}
		if override.PruneInterval > 0 { // PruneInterval from GetDuration is already parsed
			rt.PruneInterval = override.PruneInterval
		}
	}

	return rt
}

// createRuntimeConfigFromOverrideOnly creates a runtime config based *only* on a [[chains]] block
func createRuntimeConfigFromOverrideOnly(override *ChainOverrideConfig, globalConfig *Config) *ChainRuntimeConfig {
	if override == nil || override.Name == "" { // Name is key
		return nil
	}

	if override.ChainID == "" {
		logger.Printf("Warning: Skipping chain '%s' defined only in config: missing required 'chain_id'.", override.Name)
		return nil
	}
	if len(override.SeedNodesP2P) == 0 && len(override.RPCEndpoints) == 0 && len(override.RESTEndpoints) == 0 {
		logger.Printf("Warning: Skipping chain '%s' defined only in config: must provide 'seed_nodes_p2p', 'rpc_endpoints', or 'rest_endpoints'.", override.Name)
		return nil
	}

	rt := &ChainRuntimeConfig{
		RegistryName: override.Name, // Use config name (which should match registry folder if it exists)
		Name:         override.Name, // Default to config name, can be enriched by registry later if matched
		ChainID:      override.ChainID,

		Enabled:                     true, // Default enabled unless explicitly set false
		SnapshotInterval:            globalConfig.GlobalSnapshotInterval,
		SnapshotIntervalRaw:         globalConfig.GlobalSnapshotIntervalRaw,
		MaxSnapshotsToKeepPerChain:  globalConfig.GlobalMaxSnapshotsToKeep,
		PruneInterval:               globalConfig.GlobalPruneInterval,
		EnablePeerDiscoveryFallback: true, // Default true, can be overridden
		SeedNodesP2P:                override.SeedNodesP2P,
		RPCEndpoints:                override.RPCEndpoints,
		RESTEndpoints:               override.RESTEndpoints,
		SeedRPCsForDiscovery:        override.RPCEndpoints, // Use RPCEndpoints from override as seed RPCs for discovery
	}

	if override.Enabled != nil {
		rt.Enabled = *override.Enabled
	}
	if override.SnapshotIntervalRaw != "" {
		parsedInterval, err := time.ParseDuration(override.SnapshotIntervalRaw)
		if err == nil {
			rt.SnapshotInterval = parsedInterval
			rt.SnapshotIntervalRaw = override.SnapshotIntervalRaw
		} else {
			logger.Printf("Warning: Invalid snapshot_interval '%s' for override chain %s. Using global default. Error: %v", override.SnapshotIntervalRaw, rt.RegistryName, err)
		}
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

	if len(rt.SeedNodesP2P) == 0 { // If no seeds given, peer discovery fallback is less useful unless RPCs are seeds.
		// rt.EnablePeerDiscoveryFallback = false // Let user decide this.
	}

	return rt
}

// Updated runChainSnapshotter to accept ChainRuntimeConfig
func runChainSnapshotter(ctx context.Context, chainConfig *ChainRuntimeConfig, globalConfig *Config, ipfs *IPFSNode) {
	logger.Printf("[%s / %s] Starting snapshotter. Interval: %s", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotIntervalRaw)

	statusTracker := GetStatusTracker() // Get global instance
	// Initial status update: chain is now 'pending' or 'active' if attempting snapshot immediately.
	// Let's mark it as 'pending' initially. takeAndProcessSnapshotForChain will set it to 'active'.
	currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName) // Use RegistryName as key
	currentStatus.Status = "pending"
	currentStatus.LastError = "" // Clear previous error
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	// Initial snapshot attempt with a short delay to allow services to settle
	select {
	case <-time.After(5 * time.Second): // Small initial delay
		if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
			logger.Printf("[%s / %s] Error during initial snapshot: %v", chainConfig.Name, chainConfig.ChainID, err)
			// Error is already logged and status updated by takeAndProcessSnapshotForChain
		}
	case <-ctx.Done():
		logger.Printf("[%s / %s] Snapshotter stopping before initial attempt due to context cancellation.", chainConfig.Name, chainConfig.ChainID)
		currentStatus.Status = "pending" // Or some other terminal state like "stopped"
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return
	}

	isContinuous := chainConfig.SnapshotIntervalRaw == "0s" || chainConfig.SnapshotInterval == 0
	var ticker *time.Ticker
	if !isContinuous {
		if chainConfig.SnapshotInterval <= 0 { // Should be caught by config parsing, but defensive.
			logger.Printf("[%s / %s] Warning: Invalid non-zero snapshot interval %v, defaulting to 4h.", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotInterval)
			chainConfig.SnapshotInterval = 4 * time.Hour
		}
		ticker = time.NewTicker(chainConfig.SnapshotInterval)
		defer ticker.Stop()
		logger.Printf("[%s / %s] Scheduled snapshot interval: %v", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotInterval)
	} else {
		logger.Printf("[%s / %s] Running in continuous mode (interval 0s).", chainConfig.Name, chainConfig.ChainID)
	}

	// Status update ticker (for LastAttemptedHeight, etc.) - less frequent than main loop for continuous.
	// More frequent updates are handled within takeAndProcessSnapshotForChain via progress.
	// This ticker is for idle periods.
	// Let's rely on takeAndProcessSnapshotForChain to update status during active work.
	// No separate statusTicker here to simplify. Height updates happen before snapshot attempt.

	for {
		if isContinuous {
			// In continuous mode, always try to take a snapshot.
			// Add a small delay between attempts to prevent tight looping on errors or no new blocks.
			err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs)
			var delay time.Duration
			if err != nil {
				logger.Printf("[%s / %s] Error in continuous snapshot: %v. Retrying after 1 minute.", chainConfig.Name, chainConfig.ChainID, err)
				delay = 1 * time.Minute
			} else {
				// Successful, or no new blocks. Check again shortly.
				delay = 30 * time.Second
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping continuous snapshotter during delay.", chainConfig.Name, chainConfig.ChainID)
				currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName)
				currentStatus.Status = "pending" // Or "stopped"
				statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
				return
			}
		} else { // Scheduled mode
			select {
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping scheduled snapshotter.", chainConfig.Name, chainConfig.ChainID)
				currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName)
				currentStatus.Status = "pending" // Or "stopped"
				statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
				return
			case <-ticker.C:
				logger.Printf("[%s / %s] Scheduled snapshot triggered.", chainConfig.Name, chainConfig.ChainID)
				if err := takeAndProcessSnapshotForChain(ctx, chainConfig, globalConfig, ipfs); err != nil {
					logger.Printf("[%s / %s] Error taking scheduled snapshot: %v", chainConfig.Name, chainConfig.ChainID, err)
					// Error already handled by takeAndProcessSnapshotForChain
				}
			}
		}
	}
}

// New function to get a healthy endpoint with a timeout
func getHealthyEndpointWithTimeout(ctx context.Context, chainConfig *ChainRuntimeConfig, endpointType string, timeout time.Duration) (string, error) {
	resultChan := make(chan string, 1)
	errChan := make(chan error, 1)

	// Create a context specifically for this discovery attempt.
	// The parent ctx might be long-lived.
	discoveryAttemptCtx, cancel := context.WithTimeout(context.Background(), timeout) // Use fresh background context + timeout
	defer cancel()

	go func() {
		// Pass discoveryAttemptCtx to getHealthyEndpointRuntime
		endpoint, err := getHealthyEndpointRuntime(discoveryAttemptCtx, chainConfig, endpointType)
		if err != nil {
			select {
			case errChan <- err:
			case <-discoveryAttemptCtx.Done(): // If timeout already occurred
			}
		} else {
			select {
			case resultChan <- endpoint:
			case <-discoveryAttemptCtx.Done():
			}
		}
	}()

	select {
	case endpoint := <-resultChan:
		return endpoint, nil
	case err := <-errChan:
		return "", err
	case <-discoveryAttemptCtx.Done(): // This is the timeout for this specific attempt
		// Check if parent context (ctx) was also cancelled, to distinguish.
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("parent context cancelled during %s endpoint discovery for %s", endpointType, chainConfig.RegistryName)
		}
		return "", fmt.Errorf("timeout (%v) while trying to find healthy %s endpoint for %s", timeout, endpointType, chainConfig.RegistryName)
	}
}

// Add a function to update chain height information more frequently
func updateChainLatestHeight(ctx context.Context, chainConfig *ChainRuntimeConfig) {
	// This function is not strictly necessary if height is checked before each snapshot attempt.
	// However, if status needs to show "latest known height" even when idle:
	rpcURL, err := getHealthyEndpointRuntime(ctx, chainConfig, "rpc") // Short timeout for this check
	if err != nil {
		// Silently ignore if no endpoint quickly found, not critical for idle update.
		return
	}

	heightCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	height, err := getLatestBlockHeightWithClient(heightCtx, rpcURL) // Use new helper
	if err != nil {
		return
	}

	statusTracker := GetStatusTracker()
	status := statusTracker.GetChainStatus(chainConfig.RegistryName)
	if height > status.LastAttemptedHeight { // Or a new field like LastKnownHeight
		status.LastAttemptedHeight = height // Or LastKnownHeight
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, status)
	}
}

// Helper for getLatestBlockHeight that manages its own client
func getLatestBlockHeightWithClient(ctx context.Context, rpcURL string) (int64, error) {
	if os.Getenv("UNICORN_PHOTOS_TEST_MODE") == "true" {
		// logger.Printf("Running in test mode, returning mock block height 12345") // This might be too noisy
		return 12345, nil
	}
	client, err := comethttp.New(rpcURL)
	if err != nil {
		return 0, fmt.Errorf("failed to create CometBFT client for %s: %w", rpcURL, err)
	}
	status, err := client.Status(ctx) // Use the passed context which should have a timeout
	if err != nil {
		return 0, fmt.Errorf("failed to query /status from %s: %w", rpcURL, err)
	}
	if status == nil || status.SyncInfo.LatestBlockHeight == 0 {
		return 0, fmt.Errorf("invalid status response or zero block height from %s", rpcURL)
	}
	return status.SyncInfo.LatestBlockHeight, nil
}

// Updated takeAndProcessSnapshotForChain to track status
func takeAndProcessSnapshotForChain(ctx context.Context, chainConfig *ChainRuntimeConfig, globalConfig *Config, ipfs *IPFSNode) error {
	statusTracker := GetStatusTracker()
	currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName) // Use RegistryName as key
	currentStatus.Status = "active"                                         // Mark as active now that we are processing it
	currentStatus.LastError = ""                                            // Clear previous error
	// Initialize progress if it's the first time for this height or if status was error/completed
	if currentStatus.LastProgress == nil || currentStatus.LastProgress.Height != chainConfig.LastAttemptedSnapshotHeight || currentStatus.Status != "active" {
		currentStatus.LastProgress = &SnapshotProgress{
			ChainID: chainConfig.RegistryName,
			// Height will be set after fetching it
			StartTime:        time.Now(),
			CompletedModules: []string{},
			// TotalModules might be dynamic, set in takeSnapshotRuntime
		}
	}
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	logger.Printf("[%s / %s] Checking for new blocks...", chainConfig.Name, chainConfig.ChainID)

	// Use getHealthyEndpointWithTimeout for robustness
	rpcDiscoveryCtx, rpcCancel := context.WithTimeout(ctx, 1*time.Minute) // Timeout for finding RPC
	defer rpcCancel()
	rpcURL, err := getHealthyEndpointWithTimeout(rpcDiscoveryCtx, chainConfig, "rpc", 1*time.Minute)
	if err != nil {
		errMsg := fmt.Sprintf("no healthy RPC endpoint found: %v", err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}
	logger.Printf("[%s / %s] Using RPC endpoint: %s", chainConfig.Name, chainConfig.ChainID, rpcURL)

	heightCtx, heightCancel := context.WithTimeout(ctx, 30*time.Second) // Timeout for getting height
	defer heightCancel()
	height, err := getLatestBlockHeightWithClient(heightCtx, rpcURL)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get latest block height from %s: %v", rpcURL, err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}

	// Update progress with actual height
	if currentStatus.LastProgress != nil {
		currentStatus.LastProgress.Height = height
	}
	currentStatus.LastAttemptedHeight = height // Update LastAttemptedHeight now
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	if height <= chainConfig.LastSuccessfulSnapshotHeight && chainConfig.LastSuccessfulSnapshotHeight > 0 {
		logger.Printf("[%s / %s] No new blocks (current: %d, last success: %d). Skipping snapshot.", chainConfig.Name, chainConfig.ChainID, height, chainConfig.LastSuccessfulSnapshotHeight)
		currentStatus.Status = "completed" // No new blocks, so considered "completed" for this cycle
		currentStatus.LastError = ""
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return nil
	}
	// This check might be redundant if LastAttemptedSnapshotHeight is managed by the loop itself
	// if height == chainConfig.LastAttemptedSnapshotHeight && chainConfig.LastAttemptedSnapshotHeight > 0 {
	// 	logger.Printf("[%s / %s] Height %d was already attempted and failed or is in progress. Skipping.", chainConfig.Name, chainConfig.ChainID, height)
	// 	currentStatus.Status = "completed" // Or "pending" if it implies retry later
	// 	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
	// 	return nil
	// }

	logger.Printf("[%s / %s] Attempting snapshot for height: %d", chainConfig.Name, chainConfig.ChainID, height)
	// chainConfig.LastAttemptedSnapshotHeight = height // Already set above

	chainSnapshotBaseDir := filepath.Join(globalConfig.SnapshotBaseDir, chainConfig.RegistryName) // Use RegistryName for dir
	snapshotDir, err := ensureSnapshotDir(height, chainSnapshotBaseDir)
	if err != nil {
		errMsg := fmt.Sprintf("failed to ensure snapshot dir %s: %v", snapshotDir, err) // snapshotDir might be ""
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}

	restDiscoveryCtx, restCancel := context.WithTimeout(ctx, 1*time.Minute) // Timeout for finding REST
	defer restCancel()
	restURL, err := getHealthyEndpointWithTimeout(restDiscoveryCtx, chainConfig, "rest", 1*time.Minute)
	if err != nil {
		errMsg := fmt.Sprintf("no healthy REST endpoint found: %v", err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}
	logger.Printf("[%s / %s] Using REST endpoint: %s for height %d snapshot", chainConfig.Name, chainConfig.ChainID, restURL, height)

	// takeSnapshotRuntime is the function that actually does the heavy lifting and updates progress
	if err := takeSnapshotRuntime(height, *chainConfig, restURL, snapshotDir); err != nil {
		errMsg := fmt.Sprintf("snapshot creation/saving for height %d failed: %v", height, err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		// Progress inside takeSnapshotRuntime should also reflect error if possible
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg) // Return the original error from takeSnapshotRuntime
	}

	// If takeSnapshotRuntime succeeded, add to IPFS
	cid, err := ipfs.AddPath(snapshotDir)
	if err != nil {
		errMsg := fmt.Sprintf("snapshot dir %s created but failed to add to IPFS: %v", snapshotDir, err)
		logger.Printf("[%s / %s] Warning: %s", chainConfig.Name, chainConfig.ChainID, errMsg) // Warning, as snapshot exists locally
		currentStatus.Status = "error"                                                        // Or a special status like "local_only"
		currentStatus.LastError = errMsg + " (local snapshot available)"
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf("failed to add snapshot dir %s to IPFS: %w", snapshotDir, err)
	}
	logger.Printf("[%s / %s] Added snapshot height %d to IPFS. CID: %s", chainConfig.Name, chainConfig.ChainID, height, cid)

	cidFilePath := filepath.Join(snapshotDir, "ipfs_cid.txt")
	if err := os.WriteFile(cidFilePath, []byte(cid), 0644); err != nil {
		// This is critical for pruning.
		logger.Printf("[%s / %s] CRITICAL WARNING: Failed to write IPFS CID %s to %s: %v. Pruning cannot unpin this snapshot!", chainConfig.Name, chainConfig.ChainID, cid, cidFilePath, err)
		// Status should reflect this potential issue.
	}

	// Mark successful
	chainConfig.LastSuccessfulSnapshotHeight = height // Update runtime state (copy)
	currentStatus.LastSuccessfulHeight = height
	currentStatus.SnapshotCount++ // Increment successful snapshots for this chain
	currentStatus.LastSnapshotTimestamp = time.Now()
	currentStatus.Status = "completed" // Completed this cycle, idle until next.
	currentStatus.LastError = ""
	if currentStatus.LastProgress != nil {
		currentStatus.LastProgress.PercentComplete = 100 // Ensure progress shows 100%
	}
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	logger.Printf("[%s / %s] Successfully processed snapshot for height %d (CID: %s).", chainConfig.Name, chainConfig.ChainID, height, cid)
	return nil
}

// Updated getHealthyEndpointRuntime
func getHealthyEndpointRuntime(ctx context.Context, chainConfig *ChainRuntimeConfig, endpointType string) (string, error) {
	state := getChainEndpointsState(chainConfig.RegistryName) // Use RegistryName as key
	state.mu.Lock()
	// Defer unlock until after successful return or full failure, to protect state.lastDiscovery update.
	// defer state.mu.Unlock() // Moved unlock to specific points

	var candidates []DiscoveredEndpoint
	if endpointType == "rpc" {
		for _, u := range chainConfig.RPCEndpoints {
			candidates = append(candidates, DiscoveredEndpoint{Address: u, Type: "rpc", Source: "config"})
		}
		candidates = append(candidates, state.RPCEndpoints...)
	} else { // rest
		for _, u := range chainConfig.RESTEndpoints {
			candidates = append(candidates, DiscoveredEndpoint{Address: u, Type: "rest", Source: "config"})
		}
		candidates = append(candidates, state.RESTEndpoints...)
	}

	uniqueCandidates := make([]DiscoveredEndpoint, 0, len(candidates))
	seenAddr := make(map[string]bool)
	for _, c := range candidates {
		if !seenAddr[c.Address] {
			uniqueCandidates = append(uniqueCandidates, c)
			seenAddr[c.Address] = true
		}
	}
	candidates = uniqueCandidates
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] }) // Shuffle to distribute load

	// logger.Printf("[%s] Checking %d cached/config %s candidates...", chainConfig.RegistryName, len(candidates), endpointType)
	for _, ep := range candidates {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second) // Timeout for this specific check
		var checkErr error
		if endpointType == "rpc" {
			checkErr = checkRPC(checkCtx, ep.Address, chainConfig.ChainID)
		} else {
			checkErr = checkREST(checkCtx, ep.Address, chainConfig.ChainID)
		}
		cancel()
		if checkErr == nil {
			// logger.Printf("[%s] Using healthy %s endpoint: %s (source: %s)", chainConfig.RegistryName, endpointType, ep.Address, ep.Source)
			state.mu.Unlock() // Unlock before returning
			return ep.Address, nil
		}
		// logger.Printf("[%s] Endpoint %s (%s) failed health check: %v", chainConfig.RegistryName, ep.Address, endpointType, checkErr)
	}
	// logger.Printf("[%s] No healthy %s endpoint from cache/config for %s.", chainConfig.RegistryName, endpointType, chainConfig.Name)

	// Attempt discovery if enabled and needed (no healthy found yet OR cache is stale)
	// Cache stale check: time.Since(state.lastDiscovery) > 15*time.Minute
	// No healthy found: this is already true if we reach here.
	shouldDiscover := chainConfig.EnablePeerDiscoveryFallback && (len(state.RPCEndpoints) == 0 && len(state.RESTEndpoints) == 0 || time.Since(state.lastDiscovery) > 15*time.Minute)

	if !shouldDiscover {
		state.mu.Unlock() // Unlock if not discovering
		return "", fmt.Errorf("[%s] no healthy %s endpoint found in cache/config and discovery not triggered", chainConfig.RegistryName, endpointType)
	}

	// Proceed with discovery
	logger.Printf("[%s] No healthy %s endpoint found or cache stale. Attempting peer discovery.", chainConfig.RegistryName, endpointType)
	state.lastDiscovery = time.Now() // Update discovery time stamp
	// Temporarily unlock while discovery runs (it can be long)
	state.mu.Unlock()

	// Use a new context for discovery to not be bound by the original short check timeout
	discoveryPhaseCtx, discoveryCancel := context.WithTimeout(context.Background(), 2*time.Minute) // Generous timeout for full discovery
	defer discoveryCancel()
	// Use SeedRPCsForDiscovery instead of RPCEndpoints for peer discovery
	discovered, discErr := DiscoverEndpoints(discoveryPhaseCtx, chainConfig.ChainID, chainConfig.SeedRPCsForDiscovery, chainConfig.SeedNodesP2P)

	state.mu.Lock()         // Re-lock to update state.RPCEndpoints/state.RESTEndpoints
	defer state.mu.Unlock() // Ensure it's unlocked on all paths from here

	if discErr != nil {
		logger.Printf("[%s] Peer discovery for %s failed: %v", chainConfig.RegistryName, endpointType, discErr)
		// Fall through to return error, as no new endpoints were found.
	} else {
		logger.Printf("[%s] Peer discovery found %d potential endpoints. Checking health...", chainConfig.RegistryName, len(discovered))
		var newlyAddedRPC []DiscoveredEndpoint
		var newlyAddedREST []DiscoveredEndpoint

		for _, discEp := range discovered {
			// Check if this specific type of endpoint is what we are looking for
			if discEp.Type != endpointType {
				// Still cache it for future use if it's the other type
				if discEp.Type == "rpc" {
					newlyAddedRPC = append(newlyAddedRPC, discEp)
				}
				if discEp.Type == "rest" {
					newlyAddedREST = append(newlyAddedREST, discEp)
				}
				continue
			}

			// Check health of the discovered endpoint of the correct type
			checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second) // Use parent ctx for this check's timeout base
			var checkErr error
			if endpointType == "rpc" {
				checkErr = checkRPC(checkCtx, discEp.Address, chainConfig.ChainID)
			} else {
				checkErr = checkREST(checkCtx, discEp.Address, chainConfig.ChainID)
			}
			cancel()

			if checkErr == nil {
				logger.Printf("[%s] Using newly discovered healthy %s endpoint: %s", chainConfig.RegistryName, endpointType, discEp.Address)
				// Add to the correct list in state and return
				if endpointType == "rpc" {
					state.RPCEndpoints = append(state.RPCEndpoints, discEp)
					newlyAddedRPC = nil // clear since it's used
				} else {
					state.RESTEndpoints = append(state.RESTEndpoints, discEp)
					newlyAddedREST = nil // clear
				}
				// Append other discovered types before returning
				state.RPCEndpoints = append(state.RPCEndpoints, newlyAddedRPC...)
				state.RESTEndpoints = append(state.RESTEndpoints, newlyAddedREST...)
				return discEp.Address, nil
			}
			// logger.Printf("[%s] Discovered %s endpoint %s failed health check: %v", chainConfig.RegistryName, endpointType, discEp.Address, checkErr)
			// If health check fails, still add to cache for future attempts
			if discEp.Type == "rpc" {
				newlyAddedRPC = append(newlyAddedRPC, discEp)
			}
			if discEp.Type == "rest" {
				newlyAddedREST = append(newlyAddedREST, discEp)
			}
		}
		// Append all newly discovered (even if unhealthy now) to the state for future use
		state.RPCEndpoints = append(state.RPCEndpoints, newlyAddedRPC...)
		state.RESTEndpoints = append(state.RESTEndpoints, newlyAddedREST...)
		if len(newlyAddedRPC) == 0 && len(newlyAddedREST) == 0 {
			logger.Printf("[%s] Peer discovery ran, but no new usable %s endpoints found.", chainConfig.RegistryName, endpointType)
		}
	}

	return "", fmt.Errorf("[%s] no healthy %s endpoint found after all attempts (including discovery)", chainConfig.RegistryName, endpointType)
}

// Updated runPerChainPruning to accept ChainRuntimeConfig
func runPerChainPruning(ctx context.Context, chainConfig ChainRuntimeConfig, globalConfig *Config, ipfs *IPFSNode) {
	logger.Printf("[%s / %s] Starting pruning service. Interval: %s, Keep: %d",
		chainConfig.Name, chainConfig.ChainID, chainConfig.PruneInterval, chainConfig.MaxSnapshotsToKeepPerChain)

	if chainConfig.PruneInterval <= 0 {
		logger.Printf("[%s / %s] Pruning interval zero or negative, pruning service disabled for this chain.", chainConfig.Name, chainConfig.ChainID)
		return
	}
	if chainConfig.MaxSnapshotsToKeepPerChain <= 0 {
		logger.Printf("[%s / %s] Max snapshots to keep is zero or negative, pruning effectively disabled (no snapshots kept by this rule).", chainConfig.Name, chainConfig.ChainID)
		// It will prune all if >0 snapshots exist. This might be intended.
	}

	ticker := time.NewTicker(chainConfig.PruneInterval)
	defer ticker.Stop()

	// Initial run after a short delay
	select {
	case <-time.After(1 * time.Minute): // Delay initial run slightly
		logger.Printf("[%s / %s] Running initial pruning check...", chainConfig.Name, chainConfig.ChainID)
		if err := pruneOldSnapshotsForChain(ipfs, globalConfig, &chainConfig); err != nil {
			logger.Printf("[%s / %s] Error during initial pruning: %v", chainConfig.Name, chainConfig.ChainID, err)
		}
	case <-ctx.Done():
		logger.Printf("[%s / %s] Stopping pruning service before initial run due to context cancellation.", chainConfig.Name, chainConfig.ChainID)
		return
	}

	for {
		select {
		case <-ctx.Done():
			logger.Printf("[%s / %s] Stopping pruning service due to context cancellation.", chainConfig.Name, chainConfig.ChainID)
			return
		case <-ticker.C:
			logger.Printf("[%s / %s] Running scheduled pruning...", chainConfig.Name, chainConfig.ChainID)
			if err := pruneOldSnapshotsForChain(ipfs, globalConfig, &chainConfig); err != nil {
				logger.Printf("[%s / %s] Error during scheduled pruning: %v", chainConfig.Name, chainConfig.ChainID, err)
			}
		}
	}
}

// Updated pruneOldSnapshotsForChain to accept ChainRuntimeConfig
func pruneOldSnapshotsForChain(ipfs *IPFSNode, config *Config, chainConfig *ChainRuntimeConfig) error {
	if chainConfig.MaxSnapshotsToKeepPerChain < 0 { // Allow 0 to mean "prune all"
		logger.Printf("[%s / %s] Pruning disabled (MaxSnapshotsToKeepPerChain is negative).", chainConfig.Name, chainConfig.ChainID)
		return nil
	}

	logger.Printf("[%s / %s] Pruning check: Keep up to %d snapshots.", chainConfig.Name, chainConfig.ChainID, chainConfig.MaxSnapshotsToKeepPerChain)
	chainSnapshotBaseDir := filepath.Join(config.SnapshotBaseDir, chainConfig.RegistryName) // Use RegistryName
	entries, err := os.ReadDir(chainSnapshotBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Printf("[%s / %s] Snapshot directory %s does not exist, nothing to prune.", chainConfig.Name, chainConfig.ChainID, chainSnapshotBaseDir)
			return nil
		}
		return fmt.Errorf("failed to read snapshot directory %s for chain %s: %w", chainSnapshotBaseDir, chainConfig.RegistryName, err)
	}

	type snapshotInfo struct {
		path    string
		height  int64
		modTime time.Time // Or creation time from metadata if available
	}
	var snapshots []snapshotInfo

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "height_") {
			continue
		}
		var height int64
		if _, errS := fmt.Sscanf(entry.Name(), "height_%d", &height); errS != nil {
			logger.Printf("[%s / %s] Warning: Could not parse height from directory name %s: %v", chainConfig.Name, chainConfig.ChainID, entry.Name(), errS)
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			logger.Printf("[%s / %s] Warning: Could not get info for directory %s: %v", chainConfig.Name, chainConfig.ChainID, entry.Name(), errInfo)
			continue
		}
		snapshots = append(snapshots, snapshotInfo{path: filepath.Join(chainSnapshotBaseDir, entry.Name()), height: height, modTime: info.ModTime()})
	}

	if len(snapshots) <= chainConfig.MaxSnapshotsToKeepPerChain {
		logger.Printf("[%s / %s] Found %d snapshots, which is within the limit of %d. No pruning needed.", chainConfig.Name, chainConfig.ChainID, len(snapshots), chainConfig.MaxSnapshotsToKeepPerChain)
		return nil
	}

	// Sort by height descending (newest first) to identify oldest ones to prune
	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].height > snapshots[j].height })

	snapshotsToPruneCount := len(snapshots) - chainConfig.MaxSnapshotsToKeepPerChain
	snapshotsToPrune := snapshots[len(snapshots)-snapshotsToPruneCount:] // Get the oldest ones

	logger.Printf("[%s / %s] Found %d snapshots. Pruning %d oldest ones to meet limit of %d.", chainConfig.Name, chainConfig.ChainID, len(snapshots), len(snapshotsToPrune), chainConfig.MaxSnapshotsToKeepPerChain)

	for _, snapshot := range snapshotsToPrune {
		logger.Printf("[%s / %s] Pruning snapshot at height %d (path: %s)", chainConfig.Name, chainConfig.ChainID, snapshot.height, snapshot.path)
		cidFilePath := filepath.Join(snapshot.path, "ipfs_cid.txt")
		cidBytes, readErr := os.ReadFile(cidFilePath)
		if readErr != nil {
			logger.Printf("[%s / %s] Warning: Failed to read CID file %s for snapshot at height %d: %v. Cannot unpin from IPFS.", chainConfig.Name, chainConfig.ChainID, cidFilePath, snapshot.height, readErr)
		} else {
			cidToUnpin := strings.TrimSpace(string(cidBytes))
			if cidToUnpin != "" {
				if unpinErr := ipfs.UnpinCID(cidToUnpin); unpinErr != nil {
					logger.Printf("[%s / %s] Warning: Failed to unpin CID %s for snapshot at height %d: %v", chainConfig.Name, chainConfig.ChainID, cidToUnpin, snapshot.height, unpinErr)
				} else {
					logger.Printf("[%s / %s] Successfully unpinned CID %s from IPFS (snapshot height %d).", chainConfig.Name, chainConfig.ChainID, cidToUnpin, snapshot.height)
				}
			} else {
				logger.Printf("[%s / %s] Warning: CID file %s for snapshot at height %d was empty.", chainConfig.Name, chainConfig.ChainID, cidFilePath, snapshot.height)
			}
		}
		// Remove local snapshot directory
		if removeErr := os.RemoveAll(snapshot.path); removeErr != nil {
			logger.Printf("[%s / %s] Error removing local snapshot directory %s for height %d: %v", chainConfig.Name, chainConfig.ChainID, snapshot.path, snapshot.height, removeErr)
		} else {
			logger.Printf("[%s / %s] Successfully removed local snapshot directory %s (height %d).", chainConfig.Name, chainConfig.ChainID, snapshot.path, snapshot.height)
		}
	}
	return nil
}
