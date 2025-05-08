package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// IPFSSnapshot represents a single snapshot in IPFS
type IPFSSnapshot struct {
	Height  int64     `json:"height"`
	Time    time.Time `json:"time"`
	Path    string    `json:"path"`
	IPFSCID string    `json:"ipfs_cid"`
	Size    int64     `json:"size"`
}

// SnapshotIndex tracks all snapshots and their IPFS CIDs
type SnapshotIndex struct {
	Snapshots []IPFSSnapshot `json:"snapshots"`
	RootCID   string         `json:"root_cid"`
}

// loadSnapshotIndex loads the snapshot index from disk
func loadSnapshotIndex(config *Config) SnapshotIndex {
	data, err := os.ReadFile(filepath.Join(config.SnapshotDir, "index.json"))
	if err != nil {
		return SnapshotIndex{
			Snapshots: make([]IPFSSnapshot, 0),
		}
	}
	var index SnapshotIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return SnapshotIndex{
			Snapshots: make([]IPFSSnapshot, 0),
		}
	}
	return index
}

// saveSnapshotIndex saves the snapshot index to disk
func saveSnapshotIndex(index SnapshotIndex, config *Config) error {
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(config.SnapshotDir, "index.json"), data, 0644)
}

// addSnapshotToIndex adds a new snapshot to the index and updates IPFS
func addSnapshotToIndex(node *IPFSNode, config *Config, height int64, snapshotDir string) error {
	index := loadSnapshotIndex(config)

	// Check if we already have this height indexed
	for _, s := range index.Snapshots {
		if s.Height == height {
			// Already exists, no need to re-add
			return nil
		}
	}

	// Calculate size
	size, err := getDirSize(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to calculate snapshot size: %w", err)
	}

	// Add to IPFS
	cid, err := node.AddPath(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to add snapshot to IPFS: %w", err)
	}

	// Add to index
	snapshot := IPFSSnapshot{
		Height:  height,
		Time:    time.Now(),
		Path:    snapshotDir,
		IPFSCID: cid,
		Size:    size,
	}

	index.Snapshots = append(index.Snapshots, snapshot)

	// Save the updated index
	if err := saveSnapshotIndex(index, config); err != nil {
		return fmt.Errorf("failed to save snapshot index: %w", err)
	}

	logger.Printf("Added snapshot at height %d to index with CID %s", height, cid)
	return nil
}

// updateReadmeWithIPFS creates a README with information about available snapshots
func updateReadmeWithIPFS(index SnapshotIndex, config *Config) error {
	readmeTemplate := `# unicorn photos

Code and output for a bespoke snapshot utility for Unicorn and Memes that automatically takes snapshots every 4 hours and stores them on IPFS.

## Usage
` + "```" + `bash
# Install
go install ./...

# Run
unicorn-photos
` + "```" + `

## IPFS Snapshots

Latest snapshot index CID: %s

### Recent Snapshots

%s

## Current Features

- Embedded IPFS node
- Automatic snapshots every 4 hours
- Decentralized snapshot storage
- Cosmos-SDK v0.50.x compatible genesis.json generation
- Mutual pinning between nodes (100GB limit)

## Purpose

This frees the unicorn.

Latest snapshot from block height: %d
`

	var snapshotsText string
	if len(index.Snapshots) > 0 {
		lastSnapshot := index.Snapshots[len(index.Snapshots)-1]
		for i := len(index.Snapshots) - 1; i >= max(0, len(index.Snapshots)-5); i-- {
			s := index.Snapshots[i]
			snapshotsText += fmt.Sprintf("- Height %d: ipfs://%s (%d MB)\n",
				s.Height, s.IPFSCID, s.Size/(1024*1024))
		}
		return os.WriteFile("README.md", []byte(fmt.Sprintf(
			readmeTemplate,
			index.RootCID,
			snapshotsText,
			lastSnapshot.Height,
		)), 0644)
	}
	return nil
}

// Helper function for updateReadmeWithIPFS
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
