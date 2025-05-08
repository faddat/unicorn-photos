package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"
	// "log" // Using custom logger now
)

func main() {
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
