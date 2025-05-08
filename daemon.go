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
)

// Struct to hold runtime state for endpoints
type ChainEndpointsState struct {
	RPCEndpoints  []DiscoveredEndpoint
	RESTEndpoints []DiscoveredEndpoint
	lastDiscovery time.Time
	mu            sync.Mutex
}

var chainEndpoints = make(map[string]*ChainEndpointsState)
var chainEndpointsMu sync.Mutex

// REMOVED package-level global config variable
// var globalConfig *Config

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
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if len(args) > 0 && args[0] != "clone" {
				return fmt.Errorf("git directory %s does not exist", dir)
			}
		} else {
			cmdArgs = append(cmdArgs, "-C", dir)
		}
	}
	cmdArgs = append(cmdArgs, args...)

	if len(args) > 0 && args[0] == "clone" && dir != "" {
		hasTargetDirArg := false
		if len(cmdArgs) > 0 {
			lastArg := cmdArgs[len(cmdArgs)-1]
			if !strings.HasPrefix(lastArg, "-") && !strings.Contains(lastArg, "://") {
				hasTargetDirArg = true
			}
		}
		if !hasTargetDirArg {
			cmdArgs = append(cmdArgs, dir)
		}
		filteredArgs := []string{}
		for i := 0; i < len(cmdArgs); i++ {
			if cmdArgs[i] == "-C" {
				i++
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
	return err == nil
}

func runDaemon(ctx context.Context, chainNames []string) error {
	doneChan := make(chan struct{})

	logger.Printf("DEBUG: Chain names received: %v", chainNames)

	// Load config - Assign to local variable `config`
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	// DO NOT assign to a global variable here

	logger.Printf("DEBUG: Before override - config.AllChains=%v, config.ChainsToSnapshot=%v",
		config.AllChains, config.ChainsToSnapshot)

	logger.Printf("Starting Unicorn Photos IPFS snapshot daemon")

	statusTracker := GetStatusTracker()
	statusTracker.StatusFilePath = filepath.Join(config.SnapshotBaseDir, "status.json")

	if err := statusTracker.LoadStatusFile(); err != nil {
		logger.Printf("Warning: Failed to load existing status file (%s): %v. Starting fresh.", statusTracker.StatusFilePath, err)
		statusTracker.StartTime = time.Now()
		statusTracker.LastUpdateTime = time.Now()
	}
	statusTracker.Start()
	defer statusTracker.Stop()

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

	if effectiveRegistryPath == defaultManagedPath || strings.Contains(effectiveRegistryPath, ".chain-registry") {
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
			gitErr := runGitCommand(ctx, 2*time.Minute, "", "clone", "https://github.com/cosmos/chain-registry", effectiveRegistryPath)
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
	logger.Printf("Chain Registry Path for Loading: %s", registryPathToLoad)
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
		case <-time.After(2 * time.Second):
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

	// --- Chain processing logic (remains the same) ---
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
		for name := range requestedChainsSet {
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
	} else {
		logger.Printf("Processing only chains explicitly defined in config [[chains]] blocks...")
		for name, override := range configOverrides {
			runtimeConf := createRuntimeConfigFromOverrideOnly(override, config)
			if runtimeConf != nil {
				if (len(runtimeConf.SeedNodesP2P) == 0 && len(runtimeConf.RPCEndpoints) == 0) || runtimeConf.ChainID == "" {
					logger.Printf("Explicit chain '%s' missing ChainID or Seed/RPC Endpoints, attempting registry lookup for enrichment...", name)
					if registryChains != nil {
						if basicInfo, ok := registryChains[name]; ok {
							if runtimeConf.ChainID == "" {
								runtimeConf.ChainID = basicInfo.ChainID
							}
							if len(runtimeConf.RPCEndpoints) == 0 {
								runtimeConf.RPCEndpoints = getRPCAddresses(basicInfo.APIs.RPC)
							}
							if len(runtimeConf.RESTEndpoints) == 0 {
								runtimeConf.RESTEndpoints = getRESTAddresses(basicInfo.APIs.REST)
							}
							if len(runtimeConf.SeedNodesP2P) == 0 {
								runtimeConf.SeedNodesP2P = getP2PAddresses(basicInfo.Peers.Seeds)
							}
							if len(runtimeConf.SeedRPCsForDiscovery) == 0 {
								runtimeConf.SeedRPCsForDiscovery = getRPCAddresses(basicInfo.APIs.RPC)
							}
							if runtimeConf.Name == "" || runtimeConf.Name == name {
								runtimeConf.Name = basicInfo.PrettyName
							}
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

	if len(chainNames) > 0 {
		filteredChains := make(map[string]*ChainRuntimeConfig)
		for _, requestedName := range chainNames {
			if chain, exists := chainsToProcess[requestedName]; exists {
				filteredChains[requestedName] = chain
				logger.Printf("Including explicitly requested chain from CLI: %s", requestedName)
			} else {
				logger.Printf("Warning: Chain '%s' requested via CLI not found in loadable configurations, skipping.", requestedName)
			}
		}
		chainsToProcess = filteredChains
		logger.Printf("Filtered to %d chain(s) based on command-line arguments.", len(chainsToProcess))
	}

	for _, rtConfig := range chainsToProcess {
		if rtConfig.Enabled {
			statusTracker.RegisterChain(rtConfig.RegistryName, rtConfig.Name)
		} else {
			logger.Printf("Skipping registration of disabled chain: %s (%s)", rtConfig.Name, rtConfig.ChainID)
		}
	}

	var wg sync.WaitGroup
	finalChainCount := 0

	select {
	case <-ctx.Done():
		logger.Printf("Shutdown requested before starting chain processors.")
		return ctx.Err()
	default:
	}

	for _, runtimeConf := range chainsToProcess {
		if runtimeConf.Enabled {
			finalChainCount++
			rtConfCopy := *runtimeConf
			// Pass the config pointer to goroutines
			configPtr := config // Create pointer to the config loaded in runDaemon

			select {
			case <-ctx.Done():
				logger.Printf("Shutdown requested during goroutine creation for chain %s.", runtimeConf.RegistryName)
				goto waitForGoroutines
			default:
			}

			wg.Add(1)
			go func(crc ChainRuntimeConfig, cfg *Config) { // Accept *Config
				defer wg.Done()
				runChainSnapshotter(ctx, &crc, cfg, ipfs) // Pass cfg pointer
			}(rtConfCopy, configPtr)

			wg.Add(1)
			go func(crc ChainRuntimeConfig, cfg *Config) { // Accept *Config
				defer wg.Done()
				runPerChainPruning(ctx, crc, cfg, ipfs) // Pass cfg pointer
			}(rtConfCopy, configPtr)
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
		statusTracker.LogStatusSummary()
		if ipfs != nil {
			logger.Printf("Closing IPFS node...")
			if errClose := ipfs.Close(); errClose != nil {
				logger.Printf("Error closing IPFS node: %v", errClose)
			} else {
				logger.Printf("IPFS node closed successfully.")
			}
		}
		close(doneChan)
	}()

	select {
	case <-ctx.Done():
		logger.Printf("Daemon shutting down (context canceled)... Waiting for cleanup.")
	case <-doneChan:
		logger.Printf("Daemon completed its work and all goroutines finished.")
		return nil
	}

	select {
	case <-doneChan:
		logger.Printf("Graceful shutdown completed.")
	case <-time.After(30 * time.Second):
		logger.Printf("WARNING: Forced exit after timeout waiting for graceful shutdown of goroutines.")
	}

	return ctx.Err()
}

// --- Helper Functions for Config Merging (createRuntimeConfig remains the same as previous fix) ---
func createRuntimeConfig(basicInfo *BasicChainInfo, globalConfig *Config, override *ChainOverrideConfig) *ChainRuntimeConfig {
	rt := &ChainRuntimeConfig{
		RegistryName:                basicInfo.RegistryName,
		Name:                        basicInfo.PrettyName,
		ChainID:                     basicInfo.ChainID,
		Enabled:                     true,
		SnapshotInterval:            globalConfig.GlobalSnapshotInterval,
		SnapshotIntervalRaw:         globalConfig.GlobalSnapshotIntervalRaw,
		MaxSnapshotsToKeepPerChain:  globalConfig.GlobalMaxSnapshotsToKeep,
		PruneInterval:               globalConfig.GlobalPruneInterval,
		EnablePeerDiscoveryFallback: true,
		SeedNodesP2P:                getP2PAddresses(basicInfo.Peers.Seeds),
		RPCEndpoints:                getRPCAddresses(basicInfo.APIs.RPC),
		RESTEndpoints:               getRESTAddresses(basicInfo.APIs.REST),
		SeedRPCsForDiscovery:        getRPCAddresses(basicInfo.APIs.RPC),
	}

	if len(basicInfo.Peers.PersistentPeers) > 0 {
		rt.SeedNodesP2P = append(rt.SeedNodesP2P, getP2PAddresses(basicInfo.Peers.PersistentPeers)...)
	}

	if override != nil {
		if override.Enabled != nil {
			rt.Enabled = *override.Enabled
		}
		if override.ChainID != "" {
			rt.ChainID = override.ChainID
		}
		if len(override.RPCEndpoints) > 0 {
			rt.RPCEndpoints = override.RPCEndpoints
			rt.SeedRPCsForDiscovery = override.RPCEndpoints // Override seeds if RPCs are overridden
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
		if override.SnapshotIntervalRaw != "" {
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
		if override.PruneInterval > 0 {
			rt.PruneInterval = override.PruneInterval
		}
	}
	return rt
}

func createRuntimeConfigFromOverrideOnly(override *ChainOverrideConfig, globalConfig *Config) *ChainRuntimeConfig {
	if override == nil || override.Name == "" {
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
		RegistryName:                override.Name,
		Name:                        override.Name,
		ChainID:                     override.ChainID,
		Enabled:                     true,
		SnapshotInterval:            globalConfig.GlobalSnapshotInterval,
		SnapshotIntervalRaw:         globalConfig.GlobalSnapshotIntervalRaw,
		MaxSnapshotsToKeepPerChain:  globalConfig.GlobalMaxSnapshotsToKeep,
		PruneInterval:               globalConfig.GlobalPruneInterval,
		EnablePeerDiscoveryFallback: true,
		SeedNodesP2P:                override.SeedNodesP2P,
		RPCEndpoints:                override.RPCEndpoints,
		RESTEndpoints:               override.RESTEndpoints,
		SeedRPCsForDiscovery:        override.RPCEndpoints,
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
	return rt
}

// Updated runChainSnapshotter signature to accept *Config
func runChainSnapshotter(ctx context.Context, chainConfig *ChainRuntimeConfig, appConfig *Config, ipfs *IPFSNode) { // Changed globalConfig to appConfig
	logger.Printf("[%s / %s] Starting snapshotter. Interval: %s", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotIntervalRaw)

	statusTracker := GetStatusTracker()
	currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName)
	currentStatus.Status = "pending"
	currentStatus.LastError = ""
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	select {
	case <-time.After(5 * time.Second):
		// Pass appConfig down
		if err := takeAndProcessSnapshotForChain(ctx, chainConfig, appConfig, ipfs); err != nil {
			logger.Printf("[%s / %s] Error during initial snapshot: %v", chainConfig.Name, chainConfig.ChainID, err)
		}
	case <-ctx.Done():
		logger.Printf("[%s / %s] Snapshotter stopping before initial attempt due to context cancellation.", chainConfig.Name, chainConfig.ChainID)
		currentStatus.Status = "pending"
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return
	}

	isContinuous := chainConfig.SnapshotIntervalRaw == "0s" || chainConfig.SnapshotInterval == 0
	var ticker *time.Ticker
	if !isContinuous {
		if chainConfig.SnapshotInterval <= 0 {
			logger.Printf("[%s / %s] Warning: Invalid non-zero snapshot interval %v, defaulting to 4h.", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotInterval)
			chainConfig.SnapshotInterval = 4 * time.Hour
		}
		ticker = time.NewTicker(chainConfig.SnapshotInterval)
		defer ticker.Stop()
		logger.Printf("[%s / %s] Scheduled snapshot interval: %v", chainConfig.Name, chainConfig.ChainID, chainConfig.SnapshotInterval)
	} else {
		logger.Printf("[%s / %s] Running in continuous mode (interval 0s).", chainConfig.Name, chainConfig.ChainID)
	}

	for {
		if isContinuous {
			// Pass appConfig down
			err := takeAndProcessSnapshotForChain(ctx, chainConfig, appConfig, ipfs)
			var delay time.Duration
			if err != nil {
				logger.Printf("[%s / %s] Error in continuous snapshot: %v. Retrying after 1 minute.", chainConfig.Name, chainConfig.ChainID, err)
				delay = 1 * time.Minute
			} else {
				delay = 30 * time.Second
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping continuous snapshotter during delay.", chainConfig.Name, chainConfig.ChainID)
				currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName)
				currentStatus.Status = "pending"
				statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
				return
			}
		} else {
			select {
			case <-ctx.Done():
				logger.Printf("[%s / %s] Stopping scheduled snapshotter.", chainConfig.Name, chainConfig.ChainID)
				currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName)
				currentStatus.Status = "pending"
				statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
				return
			case <-ticker.C:
				logger.Printf("[%s / %s] Scheduled snapshot triggered.", chainConfig.Name, chainConfig.ChainID)
				// Pass appConfig down
				if err := takeAndProcessSnapshotForChain(ctx, chainConfig, appConfig, ipfs); err != nil {
					logger.Printf("[%s / %s] Error taking scheduled snapshot: %v", chainConfig.Name, chainConfig.ChainID, err)
				}
			}
		}
	}
}

// Updated getHealthyEndpointWithTimeout signature to accept *Config
func getHealthyEndpointWithTimeout(ctx context.Context, chainConfig *ChainRuntimeConfig, endpointType string, timeout time.Duration, appConfig *Config) (string, error) {
	resultChan := make(chan string, 1)
	errChan := make(chan error, 1)

	discoveryAttemptCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	go func() {
		// Pass appConfig down
		endpoint, err := getHealthyEndpointRuntime(discoveryAttemptCtx, chainConfig, endpointType, appConfig)
		if err != nil {
			select {
			case errChan <- err:
			case <-discoveryAttemptCtx.Done():
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
	case <-discoveryAttemptCtx.Done():
		if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("parent context cancelled during %s endpoint discovery for %s", endpointType, chainConfig.RegistryName)
		}
		return "", fmt.Errorf("timeout (%v) while trying to find healthy %s endpoint for %s", timeout, endpointType, chainConfig.RegistryName)
	}
}

// updateChainLatestHeight remains the same, doesn't need appConfig
func updateChainLatestHeight(ctx context.Context, chainConfig *ChainRuntimeConfig, appConfig *Config) {
	// We need appConfig to call getHealthyEndpointRuntime
	rpcURL, err := getHealthyEndpointRuntime(ctx, chainConfig, "rpc", appConfig)
	if err != nil {
		return
	}
	heightCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	height, err := getLatestBlockHeightWithClient(heightCtx, rpcURL)
	if err != nil {
		return
	}
	statusTracker := GetStatusTracker()
	status := statusTracker.GetChainStatus(chainConfig.RegistryName)
	if height > status.LastAttemptedHeight {
		status.LastAttemptedHeight = height
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, status)
	}
}

// getLatestBlockHeightWithClient remains the same
func getLatestBlockHeightWithClient(ctx context.Context, rpcURL string) (int64, error) {
	if os.Getenv("UNICORN_PHOTOS_TEST_MODE") == "true" {
		return 12345, nil
	}
	client, err := comethttp.New(rpcURL)
	if err != nil {
		return 0, fmt.Errorf("failed to create CometBFT client for %s: %w", rpcURL, err)
	}
	status, err := client.Status(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to query /status from %s: %w", rpcURL, err)
	}
	if status == nil || status.SyncInfo.LatestBlockHeight == 0 {
		return 0, fmt.Errorf("invalid status response or zero block height from %s", rpcURL)
	}
	return status.SyncInfo.LatestBlockHeight, nil
}

// Updated takeAndProcessSnapshotForChain signature to accept *Config
func takeAndProcessSnapshotForChain(ctx context.Context, chainConfig *ChainRuntimeConfig, appConfig *Config, ipfs *IPFSNode) error { // Changed globalConfig to appConfig
	statusTracker := GetStatusTracker()
	currentStatus := statusTracker.GetChainStatus(chainConfig.RegistryName)
	currentStatus.Status = "active"
	currentStatus.LastError = ""
	if currentStatus.LastProgress == nil || currentStatus.LastProgress.Height != chainConfig.LastAttemptedSnapshotHeight || currentStatus.Status != "active" {
		currentStatus.LastProgress = &SnapshotProgress{
			ChainID:          chainConfig.RegistryName,
			StartTime:        time.Now(),
			CompletedModules: []string{},
		}
	}
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	logger.Printf("[%s / %s] Checking for new blocks...", chainConfig.Name, chainConfig.ChainID)

	rpcDiscoveryCtx, rpcCancel := context.WithTimeout(ctx, 1*time.Minute)
	defer rpcCancel()
	// Pass appConfig down
	rpcURL, err := getHealthyEndpointWithTimeout(rpcDiscoveryCtx, chainConfig, "rpc", 1*time.Minute, appConfig)
	if err != nil {
		errMsg := fmt.Sprintf("no healthy RPC endpoint found: %v", err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}
	logger.Printf("[%s / %s] Using RPC endpoint: %s", chainConfig.Name, chainConfig.ChainID, rpcURL)

	heightCtx, heightCancel := context.WithTimeout(ctx, 30*time.Second)
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

	if currentStatus.LastProgress != nil {
		currentStatus.LastProgress.Height = height
	}
	currentStatus.LastAttemptedHeight = height
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	if height <= chainConfig.LastSuccessfulSnapshotHeight && chainConfig.LastSuccessfulSnapshotHeight > 0 {
		logger.Printf("[%s / %s] No new blocks (current: %d, last success: %d). Skipping snapshot.", chainConfig.Name, chainConfig.ChainID, height, chainConfig.LastSuccessfulSnapshotHeight)
		currentStatus.Status = "completed"
		currentStatus.LastError = ""
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return nil
	}

	logger.Printf("[%s / %s] Attempting snapshot for height: %d", chainConfig.Name, chainConfig.ChainID, height)

	chainSnapshotBaseDir := filepath.Join(appConfig.SnapshotBaseDir, chainConfig.RegistryName) // Use appConfig
	snapshotDir, err := ensureSnapshotDir(height, chainSnapshotBaseDir)
	if err != nil {
		errMsg := fmt.Sprintf("failed to ensure snapshot dir %s: %v", snapshotDir, err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}

	restDiscoveryCtx, restCancel := context.WithTimeout(ctx, 1*time.Minute)
	defer restCancel()
	// Pass appConfig down
	restURL, err := getHealthyEndpointWithTimeout(restDiscoveryCtx, chainConfig, "rest", 1*time.Minute, appConfig)
	if err != nil {
		errMsg := fmt.Sprintf("no healthy REST endpoint found: %v", err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}
	logger.Printf("[%s / %s] Using REST endpoint: %s for height %d snapshot", chainConfig.Name, chainConfig.ChainID, restURL, height)

	// takeSnapshotRuntime doesn't need appConfig directly, just uses chainConfig
	if err := takeSnapshotRuntime(height, *chainConfig, restURL, snapshotDir); err != nil {
		errMsg := fmt.Sprintf("snapshot creation/saving for height %d failed: %v", height, err)
		logger.Printf("[%s / %s] Error: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf(errMsg)
	}

	cid, err := ipfs.AddPath(snapshotDir)
	if err != nil {
		errMsg := fmt.Sprintf("snapshot dir %s created but failed to add to IPFS: %v", snapshotDir, err)
		logger.Printf("[%s / %s] Warning: %s", chainConfig.Name, chainConfig.ChainID, errMsg)
		currentStatus.Status = "error"
		currentStatus.LastError = errMsg + " (local snapshot available)"
		statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)
		return fmt.Errorf("failed to add snapshot dir %s to IPFS: %w", snapshotDir, err)
	}
	logger.Printf("[%s / %s] Added snapshot height %d to IPFS. CID: %s", chainConfig.Name, chainConfig.ChainID, height, cid)

	cidFilePath := filepath.Join(snapshotDir, "ipfs_cid.txt")
	if err := os.WriteFile(cidFilePath, []byte(cid), 0644); err != nil {
		logger.Printf("[%s / %s] CRITICAL WARNING: Failed to write IPFS CID %s to %s: %v. Pruning cannot unpin this snapshot!", chainConfig.Name, chainConfig.ChainID, cid, cidFilePath, err)
	}

	chainConfig.LastSuccessfulSnapshotHeight = height
	currentStatus.LastSuccessfulHeight = height
	currentStatus.SnapshotCount++
	currentStatus.LastSnapshotTimestamp = time.Now()
	currentStatus.Status = "completed"
	currentStatus.LastError = ""
	if currentStatus.LastProgress != nil {
		currentStatus.LastProgress.PercentComplete = 100
	}
	statusTracker.UpdateChainStatus(chainConfig.RegistryName, currentStatus)

	logger.Printf("[%s / %s] Successfully processed snapshot for height %d (CID: %s).", chainConfig.Name, chainConfig.ChainID, height, cid)
	return nil
}

// getHealthyEndpointRuntime finds a working endpoint from config, cached discoveries, or performs discovery
func getHealthyEndpointRuntime(ctx context.Context, chainConfig *ChainRuntimeConfig, endpointType string, appConfig *Config) (string, error) {
	state := getChainEndpointsState(chainConfig.RegistryName)
	state.mu.Lock()

	var candidates []DiscoveredEndpoint

	// First prioritize previously discovered endpoints over registry-provided ones
	if endpointType == "rpc" {
		candidates = append(candidates, state.RPCEndpoints...)
		// Only add registry endpoints if we don't have any discovered ones or if explicitly configured to do so
		if len(candidates) == 0 || appConfig.AllowRegistryEndpoints {
			for _, u := range chainConfig.RPCEndpoints {
				candidates = append(candidates, DiscoveredEndpoint{Address: u, Type: "rpc", Source: "config/registry"})
			}
		}
	} else {
		candidates = append(candidates, state.RESTEndpoints...)
		// Only add registry endpoints if we don't have any discovered ones or if explicitly configured to do so
		if len(candidates) == 0 || appConfig.AllowRegistryEndpoints {
			for _, u := range chainConfig.RESTEndpoints {
				candidates = append(candidates, DiscoveredEndpoint{Address: u, Type: "rest", Source: "config/registry"})
			}
		}
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
	rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })

	// Use appConfig (passed as argument) for Debug check
	if appConfig.Debug { // Access the passed config
		logger.Printf("[%s] DEBUG: Initial %s candidates for %s: %v", chainConfig.RegistryName, endpointType, chainConfig.Name, candidates)
	}

	for _, ep := range candidates {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var checkErr error
		if endpointType == "rpc" {
			checkErr = checkRPC(checkCtx, ep.Address, chainConfig.ChainID)
		} else {
			checkErr = checkREST(checkCtx, ep.Address, chainConfig.ChainID)
		}
		cancel()
		if checkErr == nil {
			logger.Printf("[%s] Using healthy %s endpoint: %s (source: %s)", chainConfig.RegistryName, endpointType, ep.Address, ep.Source)
			state.mu.Unlock()
			return ep.Address, nil
		}
		// Use appConfig (passed as argument) for Debug check
		if appConfig.Debug { // Access the passed config
			logger.Printf("[%s] DEBUG: Endpoint %s (%s) for %s failed health check: %v", chainConfig.RegistryName, ep.Address, endpointType, chainConfig.Name, checkErr)
		}
	}

	shouldDiscover := chainConfig.EnablePeerDiscoveryFallback && (len(state.RPCEndpoints) == 0 && len(state.RESTEndpoints) == 0 || time.Since(state.lastDiscovery) > 15*time.Minute)

	if !shouldDiscover {
		state.mu.Unlock()
		return "", fmt.Errorf("[%s] no healthy %s endpoint found from config/registry/cache and discovery not triggered for %s", chainConfig.RegistryName, endpointType, chainConfig.Name)
	}

	logger.Printf("[%s] No healthy %s endpoint found or cache stale for %s. Attempting peer discovery.", chainConfig.RegistryName, endpointType, chainConfig.Name)
	state.lastDiscovery = time.Now()
	state.mu.Unlock() // Unlock during discovery

	discoveryPhaseCtx, discoveryCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer discoveryCancel()

	// Always use SeedRPCsForDiscovery rather than directly using the endpoint options
	discovered, discErr := DiscoverEndpoints(discoveryPhaseCtx, chainConfig.ChainID, chainConfig.SeedRPCsForDiscovery, chainConfig.SeedNodesP2P)

	state.mu.Lock()
	defer state.mu.Unlock()

	if discErr != nil {
		logger.Printf("[%s] Peer discovery for %s (%s) failed: %v", chainConfig.RegistryName, chainConfig.Name, endpointType, discErr)
	} else {
		logger.Printf("[%s] Peer discovery for %s (%s) found %d potential endpoints. Checking health...", chainConfig.RegistryName, chainConfig.Name, endpointType, len(discovered))
		var newlyAddedRPC []DiscoveredEndpoint
		var newlyAddedREST []DiscoveredEndpoint

		for _, discEp := range discovered {
			if discEp.Type != endpointType {
				if discEp.Type == "rpc" {
					newlyAddedRPC = append(newlyAddedRPC, discEp)
				}
				if discEp.Type == "rest" {
					newlyAddedREST = append(newlyAddedREST, discEp)
				}
				continue
			}

			checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			var checkErr error
			if endpointType == "rpc" {
				checkErr = checkRPC(checkCtx, discEp.Address, chainConfig.ChainID)
			} else {
				checkErr = checkREST(checkCtx, discEp.Address, chainConfig.ChainID)
			}
			cancel()

			if checkErr == nil {
				logger.Printf("[%s] Using newly discovered healthy %s endpoint for %s: %s", chainConfig.RegistryName, endpointType, chainConfig.Name, discEp.Address)
				if endpointType == "rpc" {
					state.RPCEndpoints = append(state.RPCEndpoints, discEp)
					newlyAddedRPC = nil
				} else {
					state.RESTEndpoints = append(state.RESTEndpoints, discEp)
					newlyAddedREST = nil
				}
				state.RPCEndpoints = append(state.RPCEndpoints, newlyAddedRPC...)
				state.RESTEndpoints = append(state.RESTEndpoints, newlyAddedREST...)
				return discEp.Address, nil
			}
			if discEp.Type == "rpc" {
				newlyAddedRPC = append(newlyAddedRPC, discEp)
			}
			if discEp.Type == "rest" {
				newlyAddedREST = append(newlyAddedREST, discEp)
			}
		}
		state.RPCEndpoints = append(state.RPCEndpoints, newlyAddedRPC...)
		state.RESTEndpoints = append(state.RESTEndpoints, newlyAddedREST...)
		if len(newlyAddedRPC) == 0 && len(newlyAddedREST) == 0 && len(discovered) > 0 {
			logger.Printf("[%s] Peer discovery for %s (%s) ran, but no new healthy endpoints of type '%s' found from %d discovered.", chainConfig.RegistryName, chainConfig.Name, endpointType, endpointType, len(discovered))
		} else if len(discovered) == 0 {
			logger.Printf("[%s] Peer discovery for %s (%s) ran, but found no endpoints.", chainConfig.RegistryName, chainConfig.Name, endpointType)
		}
	}

	return "", fmt.Errorf("[%s] no healthy %s endpoint found for %s after all attempts (including discovery)", chainConfig.RegistryName, endpointType, chainConfig.Name)
}

// Updated runPerChainPruning signature to accept *Config
func runPerChainPruning(ctx context.Context, chainConfig ChainRuntimeConfig, appConfig *Config, ipfs *IPFSNode) { // Changed globalConfig to appConfig
	logger.Printf("[%s / %s] Starting pruning service. Interval: %s, Keep: %d",
		chainConfig.Name, chainConfig.ChainID, chainConfig.PruneInterval, chainConfig.MaxSnapshotsToKeepPerChain)

	if chainConfig.PruneInterval <= 0 {
		logger.Printf("[%s / %s] Pruning interval zero or negative, pruning service disabled for this chain.", chainConfig.Name, chainConfig.ChainID)
		return
	}
	if chainConfig.MaxSnapshotsToKeepPerChain <= 0 {
		logger.Printf("[%s / %s] Max snapshots to keep is %d. If 0, all but the newest snapshot will be pruned.", chainConfig.Name, chainConfig.ChainID, chainConfig.MaxSnapshotsToKeepPerChain)
	}

	ticker := time.NewTicker(chainConfig.PruneInterval)
	defer ticker.Stop()

	select {
	case <-time.After(1 * time.Minute):
		logger.Printf("[%s / %s] Running initial pruning check...", chainConfig.Name, chainConfig.ChainID)
		// Pass appConfig down
		if err := pruneOldSnapshotsForChain(ipfs, appConfig, &chainConfig); err != nil {
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
			// Pass appConfig down
			if err := pruneOldSnapshotsForChain(ipfs, appConfig, &chainConfig); err != nil {
				logger.Printf("[%s / %s] Error during scheduled pruning: %v", chainConfig.Name, chainConfig.ChainID, err)
			}
		}
	}
}

// Updated pruneOldSnapshotsForChain signature to accept *Config
func pruneOldSnapshotsForChain(ipfs *IPFSNode, appConfig *Config, chainConfig *ChainRuntimeConfig) error { // Changed config to appConfig
	if chainConfig.MaxSnapshotsToKeepPerChain < 0 {
		logger.Printf("[%s / %s] Pruning disabled (MaxSnapshotsToKeepPerChain is negative).", chainConfig.Name, chainConfig.ChainID)
		return nil
	}

	logger.Printf("[%s / %s] Pruning check: Keep up to %d snapshots.", chainConfig.Name, chainConfig.ChainID, chainConfig.MaxSnapshotsToKeepPerChain)
	// Use appConfig
	chainSnapshotBaseDir := filepath.Join(appConfig.SnapshotBaseDir, chainConfig.RegistryName)
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
		modTime time.Time
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

	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].height > snapshots[j].height })

	snapshotsToPruneCount := len(snapshots) - chainConfig.MaxSnapshotsToKeepPerChain
	snapshotsToPrune := snapshots[len(snapshots)-snapshotsToPruneCount:]

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
		if removeErr := os.RemoveAll(snapshot.path); removeErr != nil {
			logger.Printf("[%s / %s] Error removing local snapshot directory %s for height %d: %v", chainConfig.Name, chainConfig.ChainID, snapshot.path, snapshot.height, removeErr)
		} else {
			logger.Printf("[%s / %s] Successfully removed local snapshot directory %s (height %d).", chainConfig.Name, chainConfig.ChainID, snapshot.path, snapshot.height)
		}
	}
	return nil
}
