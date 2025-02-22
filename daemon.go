package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type IPFSSnapshot struct {
	Height  int64     `json:"height"`
	Time    time.Time `json:"time"`
	Path    string    `json:"path"`
	IPFSCID string    `json:"ipfs_cid"`
}

type SnapshotIndex struct {
	Snapshots []IPFSSnapshot `json:"snapshots"`
	RootCID   string         `json:"root_cid"`
}

func runDaemon(ctx context.Context) error {
	// Create IPFS node
	ipfsPath := filepath.Join(os.Getenv("HOME"), ".unicorn-photos", "ipfs")
	node, err := createNode(ctx, ipfsPath)
	if err != nil {
		return fmt.Errorf("failed to create IPFS node: %v", err)
	}
	defer node.Close()

	// Create snapshots directory if it doesn't exist
	if err := os.MkdirAll("snapshots", 0755); err != nil {
		return fmt.Errorf("failed to create snapshots directory: %v", err)
	}

	// Load or create snapshot index
	index := loadSnapshotIndex()

	ticker := time.NewTicker(4 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := takeAndUploadSnapshot(node, &index); err != nil {
				log.Printf("Failed to take snapshot: %v", err)
				continue
			}
		}
	}
}

func loadSnapshotIndex() SnapshotIndex {
	data, err := os.ReadFile("snapshots/index.json")
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

func saveSnapshotIndex(index SnapshotIndex) error {
	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("snapshots/index.json", data, 0644)
}

func takeAndUploadSnapshot(node *IPFSNode, index *SnapshotIndex) error {
	// Get latest block height
	height, err := getLatestBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to get latest block height: %v", err)
	}

	// Create snapshot directory
	snapshotDir, err := ensureSnapshotDir(height)
	if err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	// Take snapshot using existing functionality
	if err := takeSnapshotCore(height); err != nil {
		return fmt.Errorf("failed to take snapshot: %v", err)
	}

	// Add snapshot directory to IPFS
	cid, err := node.AddPath(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to add snapshot to IPFS: %v", err)
	}

	// Update index
	snapshot := IPFSSnapshot{
		Height:  height,
		Time:    time.Now(),
		Path:    snapshotDir,
		IPFSCID: cid,
	}
	index.Snapshots = append(index.Snapshots, snapshot)

	// Add index to IPFS
	indexFile := filepath.Join("snapshots", "index.json")
	if err := saveSnapshotIndex(*index); err != nil {
		return fmt.Errorf("failed to save index: %v", err)
	}

	rootCID, err := node.AddPath(indexFile)
	if err != nil {
		return fmt.Errorf("failed to add index to IPFS: %v", err)
	}

	index.RootCID = rootCID
	if err := saveSnapshotIndex(*index); err != nil {
		return fmt.Errorf("failed to save index: %v", err)
	}

	// Update README with latest IPFS CIDs
	return updateReadmeWithIPFS(*index)
}

func updateReadmeWithIPFS(index SnapshotIndex) error {
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

## Purpose

This frees the unicorn.

Latest snapshot from block height: %d
`

	var snapshotsText string
	if len(index.Snapshots) > 0 {
		lastSnapshot := index.Snapshots[len(index.Snapshots)-1]
		for i := len(index.Snapshots) - 1; i >= max(0, len(index.Snapshots)-5); i-- {
			s := index.Snapshots[i]
			snapshotsText += fmt.Sprintf("- Height %d: ipfs://%s\n", s.Height, s.IPFSCID)
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
