package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	fmt.Println("  run              Start the daemon")
	fmt.Println("  status           Display snapshot status")
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

		if err := runDaemon(ctx); err != nil {
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
	daemonMode := flag.Bool("daemon", true, "Run in daemon mode for continuous snapshotting")
	flag.Parse()

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

	// Run the daemon
	if *daemonMode {
		return runDaemon(ctx)
	}

	return fmt.Errorf("non-daemon mode is not currently supported")
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
