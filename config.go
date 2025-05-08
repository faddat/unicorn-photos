package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"
)

// ChainOverrideConfig represents the settings defined in the [[chains]] blocks in config.toml
// These are used to override dynamically loaded chain info or define chains explicitly.
type ChainOverrideConfig struct {
	Name                        string        `mapstructure:"name"`                            // MUST match registry folder name if overriding
	Enabled                     *bool         `mapstructure:"enabled"`                         // Use pointer to check if set
	ChainID                     string        `mapstructure:"chain_id"`                        // Required only if not in registry
	RPCEndpoints                []string      `mapstructure:"rpc_endpoints"`                   // Optional endpoint hints/overrides
	RESTEndpoints               []string      `mapstructure:"rest_endpoints"`                  // Optional endpoint hints/overrides
	SeedNodesP2P                []string      `mapstructure:"seed_nodes_p2p"`                  // Optional seed overrides
	EnablePeerDiscoveryFallback *bool         `mapstructure:"enable_peer_discovery_fallback"`  // Optional override
	SnapshotInterval            time.Duration `mapstructure:"snapshot_interval"`               // Use GetDuration for parsing
	SnapshotIntervalRaw         string        `mapstructure:"snapshot_interval"`               // Keep raw string for "0s" check
	MaxSnapshotsToKeepPerChain  *int          `mapstructure:"max_snapshots_to_keep_per_chain"` // Use pointer
	PruneInterval               time.Duration `mapstructure:"prune_interval"`                  // Use GetDuration
}

// ChainRuntimeConfig represents the final configuration used for a chain snapshotter goroutine.
// It's derived from registry data + config overrides.
type ChainRuntimeConfig struct {
	RegistryName                 string        // Name from registry folder (key for lookups)
	Name                         string        // User-friendly name (from registry or override)
	ChainID                      string        // Verified Chain ID
	Enabled                      bool          // Final enabled state
	RPCEndpoints                 []string      // Endpoints to try first (from override or registry)
	RESTEndpoints                []string      // Endpoints to try first (from override or registry)
	SeedNodesP2P                 []string      // Seeds for discovery (from override or registry)
	EnablePeerDiscoveryFallback  bool          // Final discovery setting
	SnapshotInterval             time.Duration // Final interval
	SnapshotIntervalRaw          string        // Raw interval string
	MaxSnapshotsToKeepPerChain   int           // Final keep count
	PruneInterval                time.Duration // Final prune interval
	LastSuccessfulSnapshotHeight int64         // Runtime state
	LastAttemptedSnapshotHeight  int64         // Runtime state
}

type Config struct {
	SnapshotBaseDir       string   `mapstructure:"snapshot_base_dir"`
	IPFSRepoPath          string   `mapstructure:"ipfs_repo_path"`
	MaxPinnedSizeGB       int64    `mapstructure:"max_pinned_size_gb"`
	LogLevel              string   `mapstructure:"log_level"`
	BootstrapPeers        []string `mapstructure:"bootstrap_peers"`      // IPFS bootstrap peers
	MaxPinnedSizeBytes    int64    `mapstructure:"-"`                    // Calculated
	PruningThresholdGB    int64    `mapstructure:"pruning_threshold_gb"` // For potential future global pruning
	PruningThresholdBytes int64    `mapstructure:"-"`                    // Calculated

	// Chain Selection & Registry
	ChainRegistryPath string   `mapstructure:"chain_registry_path"`
	AllChains         bool     `mapstructure:"all_chains"`
	ChainsToSnapshot  []string `mapstructure:"chains_to_snapshot"`

	// Defaults for chains
	GlobalSnapshotIntervalRaw string        `mapstructure:"global_snapshot_interval"`
	GlobalSnapshotInterval    time.Duration `mapstructure:"-"` // Parsed
	GlobalMaxSnapshotsToKeep  int           `mapstructure:"global_max_snapshots_to_keep"`
	GlobalPruneIntervalRaw    string        `mapstructure:"global_prune_interval"`
	GlobalPruneInterval       time.Duration `mapstructure:"-"` // Parsed

	// Chain Specific Overrides
	ChainOverrides []ChainOverrideConfig `mapstructure:"chains"` // Renamed from 'Chains'
}

func LoadConfig() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.unicorn-photos")
	viper.AddConfigPath("/etc/unicorn-photos/")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("UP")

	// Set defaults
	viper.SetDefault("snapshot_base_dir", "snapshots")
	homeDir, _ := os.UserHomeDir()
	viper.SetDefault("ipfs_repo_path", filepath.Join(homeDir, ".unicorn-photos", "ipfs"))
	viper.SetDefault("max_pinned_size_gb", 100)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("bootstrap_peers", []string{})
	viper.SetDefault("chain_registry_path", "") // No default, MUST be set if needed
	viper.SetDefault("all_chains", false)
	viper.SetDefault("chains_to_snapshot", []string{})
	viper.SetDefault("global_snapshot_interval", "4h")
	viper.SetDefault("global_max_snapshots_to_keep", 10)
	viper.SetDefault("global_prune_interval", "1h")
	viper.SetDefault("pruning_threshold_gb", 0) // Disable global size pruning by default

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			logger.Printf("Warning: Config file not found, using defaults.")
		} else {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	// Use UnmarshalExact to prevent unexpected fields, or Unmarshal for flexibility
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Post-process and validate
	cfg.MaxPinnedSizeBytes = cfg.MaxPinnedSizeGB * 1024 * 1024 * 1024
	if cfg.PruningThresholdGB > 0 {
		cfg.PruningThresholdBytes = cfg.PruningThresholdGB * 1024 * 1024 * 1024
	} else {
		cfg.PruningThresholdBytes = 0 // Explicitly disable if GB is 0 or less
	}

	// Parse global intervals
	var err error
	cfg.GlobalSnapshotInterval, err = time.ParseDuration(cfg.GlobalSnapshotIntervalRaw)
	if err != nil {
		logger.Printf("Warning: Invalid global_snapshot_interval '%s'. Using default 4h. Error: %v", cfg.GlobalSnapshotIntervalRaw, err)
		cfg.GlobalSnapshotInterval = 4 * time.Hour
		cfg.GlobalSnapshotIntervalRaw = "4h"
	}
	cfg.GlobalPruneInterval, err = time.ParseDuration(cfg.GlobalPruneIntervalRaw)
	if err != nil {
		logger.Printf("Warning: Invalid global_prune_interval '%s'. Using default 1h. Error: %v", cfg.GlobalPruneIntervalRaw, err)
		cfg.GlobalPruneInterval = 1 * time.Hour
		cfg.GlobalPruneIntervalRaw = "1h"
	}

	// Process chain overrides (they might need parsing too, done during merge in daemon)
	for i := range cfg.ChainOverrides {
		// Keep the raw snapshot interval string for '0s' check
		cfg.ChainOverrides[i].SnapshotIntervalRaw = viper.GetString(fmt.Sprintf("chains.%d.snapshot_interval", i))
		// Parse durations provided in overrides
		cfg.ChainOverrides[i].SnapshotInterval = viper.GetDuration(fmt.Sprintf("chains.%d.snapshot_interval", i))
		cfg.ChainOverrides[i].PruneInterval = viper.GetDuration(fmt.Sprintf("chains.%d.prune_interval", i))
	}

	// Validation
	if (cfg.AllChains || len(cfg.ChainsToSnapshot) > 0) && cfg.ChainRegistryPath == "" {
		return nil, fmt.Errorf("config error: 'chain_registry_path' must be set when 'all_chains' is true or 'chains_to_snapshot' is used")
	}
	if cfg.ChainRegistryPath != "" {
		if _, err := os.Stat(cfg.ChainRegistryPath); os.IsNotExist(err) {
			return nil, fmt.Errorf("config error: 'chain_registry_path' (%s) does not exist or is not accessible", cfg.ChainRegistryPath)
		}
	}

	return &cfg, nil
}
