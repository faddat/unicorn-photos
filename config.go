package main

import (
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	SnapshotInterval   time.Duration // How often to take snapshots
	MaxPinnedSize      int64         // Maximum size to pin (in bytes)
	SnapshotDir        string        // Base directory for snapshots
	IPFSRepoPath       string        // IPFS repository path
	BootstrapPeers     []string      // Additional bootstrap peers
	MaxSnapshotsToKeep int           // Maximum number of snapshots to retain
}

func LoadConfig() *Config {
	return &Config{
		SnapshotInterval:   4 * time.Hour,
		MaxPinnedSize:      100 * 1024 * 1024 * 1024, // 100GB
		SnapshotDir:        "snapshots",
		IPFSRepoPath:       filepath.Join(os.Getenv("HOME"), ".unicorn-photos", "ipfs"),
		BootstrapPeers:     []string{},
		MaxSnapshotsToKeep: 10,
	}
}
