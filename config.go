package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
)

type ChainConfig struct {
	Name                         string        `mapstructure:"name"`
	ChainID                      string        `mapstructure:"chain_id"`
	Enabled                      bool          `mapstructure:"enabled"`
	RPCEndpoints                 []string      `mapstructure:"rpc_endpoints"`
	RESTEndpoints                []string      `mapstructure:"rest_endpoints"`
	SeedNodesP2P                 []string      `mapstructure:"seed_nodes_p2p"` // For peer discovery
	EnablePeerDiscoveryFallback  bool          `mapstructure:"enable_peer_discovery_fallback"`
	SnapshotInterval             time.Duration `mapstructure:"snapshot_interval"`
	SnapshotIntervalRaw          string        `mapstructure:"snapshot_interval"` // Keep raw string for "0s"
	MaxSnapshotsToKeepPerChain   int           `mapstructure:"max_snapshots_to_keep_per_chain"`
	PruneInterval                time.Duration `mapstructure:"prune_interval"`
	LastSuccessfulSnapshotHeight int64         `mapstructure:"-"` // Runtime state, not from config file
	LastAttemptedSnapshotHeight  int64         `mapstructure:"-"` // Runtime state
}

type Config struct {
	SnapshotBaseDir           string        `mapstructure:"snapshot_base_dir"`
	IPFSRepoPath              string        `mapstructure:"ipfs_repo_path"`
	MaxPinnedSizeGB           int64         `mapstructure:"max_pinned_size_gb"`
	GlobalSnapshotInterval    time.Duration `mapstructure:"global_snapshot_interval"`
	GlobalSnapshotIntervalRaw string        `mapstructure:"global_snapshot_interval"`
	PruningThresholdGB        int64         `mapstructure:"pruning_threshold_gb"`
	LogLevel                  string        `mapstructure:"log_level"`
	Chains                    []ChainConfig `mapstructure:"chains"`
	MaxSnapshotsToKeep        int           `mapstructure:"max_snapshots_to_keep"` // Legacy, consider removing or clarifying
	BootstrapPeers            []string      `mapstructure:"bootstrap_peers"`
	MaxPinnedSizeBytes        int64         `mapstructure:"-"` // Calculated
	PruningThresholdBytes     int64         `mapstructure:"-"` // Calculated
	GlobalPruneInterval       time.Duration `mapstructure:"global_prune_interval"`
}

func LoadConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.unicorn-photos")
	viper.AddConfigPath("/etc/unicorn-photos/")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("UP") // e.g. UP_SNAPSHOT_BASE_DIR

	// Set defaults
	viper.SetDefault("snapshot_base_dir", "snapshots")
	homeDir, _ := os.UserHomeDir()
	viper.SetDefault("ipfs_repo_path", filepath.Join(homeDir, ".unicorn-photos", "ipfs"))
	viper.SetDefault("max_pinned_size_gb", 100)
	viper.SetDefault("global_snapshot_interval", "4h") // This default will be parsed
	viper.SetDefault("pruning_threshold_gb", 69)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("bootstrap_peers", []string{}) // IPFS bootstrap peers
	viper.SetDefault("global_prune_interval", "1h") // How often to run global pruning logic

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			logger.Printf("Warning: Config file not found, using defaults and environment variables.")
		} else {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.MaxPinnedSizeBytes = cfg.MaxPinnedSizeGB * 1024 * 1024 * 1024
	cfg.PruningThresholdBytes = cfg.PruningThresholdGB * 1024 * 1024 * 1024

	// Process global snapshot interval - viper.GetDuration handles parsing directly if key exists
	// If we rely on GetString for GlobalSnapshotIntervalRaw and then parse, that's fine too.
	// Let's ensure GlobalSnapshotIntervalRaw is populated from viper before parsing.
	cfg.GlobalSnapshotIntervalRaw = viper.GetString("global_snapshot_interval")
	parsedGlobalInterval, err := time.ParseDuration(cfg.GlobalSnapshotIntervalRaw)
	if err != nil {
		logger.Printf("Warning: Invalid global_snapshot_interval raw string '%s', attempting to use viper.GetDuration or fallback. Error: %v", cfg.GlobalSnapshotIntervalRaw, err)
		// Fallback to viper.GetDuration which might parse it correctly if it's a known format for viper
		parsedGlobalInterval = viper.GetDuration("global_snapshot_interval")
		if parsedGlobalInterval == 0 { // If GetDuration also failed or returned 0 (and 0 is not intended)
			logger.Printf("Using 4h default for global_snapshot_interval due to parsing issues.")
			parsedGlobalInterval = 4 * time.Hour
		}
	}
	cfg.GlobalSnapshotInterval = parsedGlobalInterval

	for i := range cfg.Chains {
		// Get the raw string for chain-specific snapshot_interval to check for "0s"
		// Construct the key carefully for viper to look up nested array elements.
		rawIntervalKey := fmt.Sprintf("chains.%d.snapshot_interval", i)
		cfg.Chains[i].SnapshotIntervalRaw = viper.GetString(rawIntervalKey)

		if cfg.Chains[i].SnapshotIntervalRaw == "" { // Not set for the chain, use global
			cfg.Chains[i].SnapshotInterval = cfg.GlobalSnapshotInterval
			cfg.Chains[i].SnapshotIntervalRaw = cfg.GlobalSnapshotIntervalRaw // also copy the raw global string
		} else {
			parsedInterval, err := time.ParseDuration(cfg.Chains[i].SnapshotIntervalRaw)
			if err != nil {
				logger.Printf("Warning: Invalid snapshot_interval '%s' for chain %s (%s), using global interval %s. Error: %v",
					cfg.Chains[i].SnapshotIntervalRaw, cfg.Chains[i].Name, cfg.Chains[i].ChainID, cfg.GlobalSnapshotInterval, err)
				cfg.Chains[i].SnapshotInterval = cfg.GlobalSnapshotInterval
			} else {
				cfg.Chains[i].SnapshotInterval = parsedInterval
			}
		}
		if cfg.Chains[i].MaxSnapshotsToKeepPerChain == 0 {
			cfg.Chains[i].MaxSnapshotsToKeepPerChain = 10 // Default if not set
		}

		pruneIntervalKey := fmt.Sprintf("chains.%d.prune_interval", i)
		if viper.IsSet(pruneIntervalKey) {
			cfg.Chains[i].PruneInterval = viper.GetDuration(pruneIntervalKey)
		} else {
			cfg.Chains[i].PruneInterval = 1 * time.Hour // Default per-chain prune check
		}
	}
	// TODO: Initialize logger with cfg.LogLevel from main or here.
	return &cfg, nil
}
