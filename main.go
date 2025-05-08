package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	// "log" // Using custom logger now
)

func main() {
	var logFilePath string
	flag.StringVar(&logFilePath, "log", "", "Path to log file. If not set, logs to stdout")
	flag.Parse()

	// Setup logging
	setupLogger(logFilePath)

	// Check for and create lock file
	if err := acquireLock(); err != nil {
		logger.Fatalf("Cannot start unicorn-photos: %v", err)
	}
	defer releaseLock()

	// Handle commands
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	var err error
	cmd := args[0]

	switch cmd {
	case "run":
		err = runCommand(args[1:])
	case "init":
		err = initCommand(args[1:])
	case "status":
		err = cmdStatus()
	case "list-chains":
		err = listChainsCommand()
	case "test-chain":
		err = testChainCommand(args[1:])
	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		logger.Fatalf("Error: %v", err)
	}
}

func printUsage() {
	fmt.Println("Unicorn Photos - Blockchain Snapshot Manager")
	fmt.Println("\nCommands:")
	fmt.Println("  init             Initialize configuration")
	fmt.Println("  run [chains...]  Start the daemon. Optionally specify chain(s) to snapshot (e.g., 'run osmosis cosmoshub')")
	fmt.Println("  status           Display snapshot status")
	fmt.Println("  list-chains      List available chains from the registry")
	fmt.Println("  test-chain <chain> Test connectivity to a specific chain without taking a snapshot")
	fmt.Println("\nFlags:")
	fmt.Println("  --log <file>     Path to log file. If not set, logs to stdout")
}

func mainOld() {
	daemonMode := flag.Bool("daemon", false, "Run in daemon mode for continuous snapshotting")
	// configFile := flag.String("config", "", "Path to config.toml file") // Example for custom config path
	flag.Parse()

	// Initialize logger (basic setup, can be enhanced)
	// logger = log.New(os.Stdout, "[unicorn-photos] ", log.LstdFlags|log.Lmicroseconds)
	logger.Printf("Unicorn Photos starting...")

	if *daemonMode {
		logger.Printf("Running in daemon mode.")
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop() // Important to release resources associated with signal.NotifyContext

		if err := runDaemon(ctx, []string{}); err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				logger.Printf("Daemon shutdown gracefully: %v", err)
			} else {
				logger.Fatalf("Daemon error: %v", err)
			}
		}
		logger.Printf("Unicorn Photos daemon has shut down.")
		return
	}

	// --- Single-run mode (for testing or specific snapshot) ---
	// This part would need significant refactoring to be useful.
	// It would need to load config, select a chain, and run one cycle.
	// For now, daemon mode is the primary operational mode.
	logger.Printf("Single-run mode not fully implemented. Please use -daemon flag.")
	/*
		config, err := LoadConfig()
		if err != nil {
			logger.Fatalf("Failed to load config: %v", err)
		}
		if len(config.Chains) == 0 {
			logger.Fatalf("No chains configured for single run.")
		}
		// Example: run for the first enabled chain
		var selectedChain *ChainConfig
		for i := range config.Chains {
			if config.Chains[i].Enabled {
				selectedChain = &config.Chains[i]
				break
			}
		}
		if selectedChain == nil {
			logger.Fatalf("No enabled chains found in config for single run.")
		}

		logger.Printf("Performing single snapshot run for chain: %s (%s)", selectedChain.Name, selectedChain.ChainID)
		ipfsNode, err := NewIPFSNode(context.Background(), config) // Use temp context for single run
		if err != nil {
			logger.Fatalf("Failed to initialize IPFS node for single run: %v", err)
		}
		defer ipfsNode.Close()

		err = takeAndProcessSnapshotForChain(context.Background(), selectedChain, config, ipfsNode)
		if err != nil {
			logger.Fatalf("Failed to take snapshot for chain %s: %v", selectedChain.ChainID, err)
		}
		logger.Printf("Single snapshot run completed for chain %s.", selectedChain.ChainID)
	*/
}

// setupLogger configures logging to either a file or stdout
func setupLogger(logFilePath string) {
	if logFilePath != "" {
		// Set up logging to a file
		logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		log.SetOutput(logFile)
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
		logger = log.New(logFile, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	} else {
		// Log to stdout
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
		logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile)
	}
}

// runCommand parses any additional arguments and starts the daemon
func runCommand(args []string) error {
	// All args are chain names
	chainNames := args

	// Create a context that will be canceled on interrupt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Println("Received interrupt signal, shutting down...")
		cancel()
	}()

	// Run the daemon with specified chains
	return runDaemon(ctx, chainNames)
}

// initCommand initializes a new configuration file
func initCommand(args []string) error {
	// Create default config
	config := &Config{
		SnapshotBaseDir:           "snapshots",
		IPFSRepoPath:              "~/.unicorn-photos/ipfs",
		ChainRegistryPath:         "~/.chain-registry",
		MaxPinnedSizeGB:           100,
		AllChains:                 true,
		GlobalSnapshotInterval:    4 * time.Hour,
		GlobalSnapshotIntervalRaw: "4h",
		GlobalMaxSnapshotsToKeep:  10,
		GlobalPruneInterval:       1 * time.Hour,
		LogLevel:                  "info",
	}

	// Create the config file
	configPath := "config.toml"
	if len(args) > 0 {
		configPath = args[0]
	}

	// Check if the config file already exists
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("config file %s already exists, will not overwrite", configPath)
	}

	// Save the config
	if err := SaveConfig(config, configPath); err != nil {
		return fmt.Errorf("failed to save config: %w", err)
	}

	logger.Printf("Initialized new configuration file at %s", configPath)
	return nil
}

// listChainsCommand lists available chains from the registry
func listChainsCommand() error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Resolve registry path
	registryPath := config.ChainRegistryPath
	if strings.HasPrefix(registryPath, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to resolve home directory: %w", err)
		}
		registryPath = filepath.Join(home, registryPath[2:])
	}
	registryPath = filepath.Clean(registryPath)

	// Load chains from registry
	fmt.Printf("Loading chains from registry: %s\n", registryPath)
	chains, err := LoadRegistryChains(registryPath, "mainnet")
	if err != nil {
		return fmt.Errorf("failed to load registry chains: %w", err)
	}

	// Create a sorted list of chain names for consistent output
	var chainNames []string
	for name := range chains {
		chainNames = append(chainNames, name)
	}
	sort.Strings(chainNames)

	// Display chains
	fmt.Printf("Found %d chains in registry:\n\n", len(chains))
	fmt.Printf("%-20s %-40s %s\n", "NAME", "CHAIN ID", "PRETTY NAME")
	fmt.Println(strings.Repeat("-", 80))

	for _, name := range chainNames {
		chain := chains[name]
		fmt.Printf("%-20s %-40s %s\n", name, chain.ChainID, chain.PrettyName)
	}

	fmt.Println("\nTo snapshot specific chains, use:")
	fmt.Println("  unicorn-photos run <chain-name> [<chain-name>...]")

	return nil
}

// testChainCommand performs a connectivity test for the specified chain
func testChainCommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("chain name required. Usage: unicorn-photos test-chain <chain-name>")
	}

	chainName := args[0]
	fmt.Printf("Testing connectivity to chain: %s\n", chainName)

	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Enable debug mode for the test
	config.Debug = true

	// Resolve registry path
	registryPath := config.ChainRegistryPath
	if strings.HasPrefix(registryPath, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to resolve home directory: %w", err)
		}
		registryPath = filepath.Join(home, registryPath[2:])
	}
	registryPath = filepath.Clean(registryPath)

	// Load chains from registry
	fmt.Printf("Loading chain '%s' information from registry: %s\n", chainName, registryPath)
	chains, err := LoadRegistryChains(registryPath, "mainnet")
	if err != nil {
		return fmt.Errorf("failed to load registry chains: %w", err)
	}

	// Find the requested chain
	chainInfo, found := chains[chainName]
	if !found {
		return fmt.Errorf("chain '%s' not found in registry", chainName)
	}

	fmt.Printf("Chain found: %s (Chain ID: %s)\n", chainInfo.PrettyName, chainInfo.ChainID)

	// Create a runtime config for the chain
	runtimeConfig := createRuntimeConfig(chainInfo, config, nil)
	if runtimeConfig == nil {
		return fmt.Errorf("failed to create runtime config for chain: %s", chainName)
	}

	// Set up a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Try to find RPC endpoint
	fmt.Printf("Looking for a healthy RPC endpoint (timeout: 60s)...\n")
	rpcEndpoint, err := getHealthyEndpointWithTimeout(ctx, runtimeConfig, "rpc", 60*time.Second)
	if err != nil {
		fmt.Printf("❌ Failed to find RPC endpoint: %v\n", err)
	} else {
		fmt.Printf("✅ Found RPC endpoint: %s\n", rpcEndpoint)

		// Get latest block height
		height, err := getLatestBlockHeight(rpcEndpoint)
		if err != nil {
			fmt.Printf("❌ Failed to get latest block height: %v\n", err)
		} else {
			fmt.Printf("✅ Latest block height: %d\n", height)
		}
	}

	// Try to find REST endpoint
	fmt.Printf("Looking for a healthy REST endpoint (timeout: 60s)...\n")
	restEndpoint, err := getHealthyEndpointWithTimeout(ctx, runtimeConfig, "rest", 60*time.Second)
	if err != nil {
		fmt.Printf("❌ Failed to find REST endpoint: %v\n", err)
	} else {
		fmt.Printf("✅ Found REST endpoint: %s\n", restEndpoint)
	}

	// Show seeds/peers
	fmt.Printf("\nSeed nodes configured for %s:\n", chainName)
	for _, seed := range runtimeConfig.SeedNodesP2P {
		fmt.Printf("- %s\n", seed)
	}

	if err := testEndpoints(ctx, runtimeConfig); err != nil {
		fmt.Printf("\n❌ Endpoint connectivity test failed: %v\n", err)
		return fmt.Errorf("chain connectivity test failed")
	}

	fmt.Printf("\n✅ Chain connectivity test completed successfully!\n")
	return nil
}

// testEndpoints performs basic connectivity tests to the chain's endpoints
func testEndpoints(ctx context.Context, chainConfig *ChainRuntimeConfig) error {
	// This is a placeholder function that would perform additional endpoint tests
	// We're already testing endpoint connectivity in the main function
	return nil
}

// Lock file functions
func getLockFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/unicorn-photos.lock"
	}
	return filepath.Join(home, ".unicorn-photos", "unicorn-photos.lock")
}

// acquireLock creates a lock file to prevent multiple instances from running
func acquireLock() error {
	lockPath := getLockFilePath()

	// Ensure directory exists
	lockDir := filepath.Dir(lockPath)
	if err := os.MkdirAll(lockDir, 0755); err != nil {
		return fmt.Errorf("failed to create lock directory: %w", err)
	}

	// Check if lock file exists
	if _, err := os.Stat(lockPath); err == nil {
		// File exists, read it to check if it's a stale lock
		data, err := os.ReadFile(lockPath)
		if err != nil {
			return fmt.Errorf("another instance appears to be running (lock file exists)")
		}

		pidStr := strings.TrimSpace(string(data))
		pid, err := strconv.Atoi(pidStr)
		if err != nil {
			// Invalid PID, assume stale lock
			fmt.Println("WARNING: Removing stale lock file with invalid PID")
		} else {
			// Check if process is still running
			process, err := os.FindProcess(pid)
			if err == nil {
				// On Unix, FindProcess always succeeds, so we need to send signal 0 to check
				err = process.Signal(syscall.Signal(0))
				if err == nil {
					return fmt.Errorf("another instance is already running (PID: %d)", pid)
				}
			}
			fmt.Printf("WARNING: Removing stale lock file from process %d\n", pid)
		}
	}

	// Create lock file with current PID
	pid := os.Getpid()
	err := os.WriteFile(lockPath, []byte(fmt.Sprintf("%d", pid)), 0644)
	if err != nil {
		return fmt.Errorf("failed to create lock file: %w", err)
	}

	fmt.Printf("Created lock file with PID %d at %s\n", pid, lockPath)
	return nil
}

// releaseLock removes the lock file
func releaseLock() {
	lockPath := getLockFilePath()
	err := os.Remove(lockPath)
	if err != nil {
		fmt.Printf("WARNING: Failed to remove lock file: %v\n", err)
	} else {
		fmt.Printf("Removed lock file at %s\n", lockPath)
	}
}
