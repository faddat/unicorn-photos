package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func runDaemon(ctx context.Context) error {
	config := LoadConfig()

	logger.Printf("Starting Unicorn Photos IPFS snapshot daemon")
	logger.Printf("Snapshot interval: %s", config.SnapshotInterval)
	logger.Printf("Max pinned size: %d bytes", config.MaxPinnedSize)

	// Initialize IPFS node
	ipfs, err := NewIPFSNode(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to initialize IPFS node: %v", err)
	}
	defer ipfs.Close()

	logger.Printf("IPFS node initialized")

	ticker := time.NewTicker(config.SnapshotInterval)
	defer ticker.Stop()

	// Take initial snapshot
	if err := takeInitialSnapshot(ipfs); err != nil {
		logger.Printf("Warning: failed to take initial snapshot: %v", err)
	}

	// Start the mutual pinning process in a goroutine
	go manageMutualPinning(ipfs, config)

	// Use for range instead of for { select {} }
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := takeScheduledSnapshot(ipfs); err != nil {
				logger.Printf("Error taking scheduled snapshot: %v", err)
			}
		}
	}
}

func takeInitialSnapshot(ipfs *IPFSNode) error {
	logger.Printf("Taking initial snapshot...")

	height, err := getLatestBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to get latest block height: %v", err)
	}

	logger.Printf("Latest block height: %d", height)

	snapshotDir, err := ensureSnapshotDir(height)
	if err != nil {
		return fmt.Errorf("failed to ensure snapshot directory: %v", err)
	}

	if err := takeSnapshot(height); err != nil {
		return fmt.Errorf("failed to take snapshot: %v", err)
	}

	// Add snapshot to index and IPFS
	config := LoadConfig()
	if err := addSnapshotToIndex(ipfs, config, height, snapshotDir); err != nil {
		return fmt.Errorf("failed to add snapshot to index: %v", err)
	}

	// Also generate a main README if needed
	index := loadSnapshotIndex(config)
	if err := updateReadmeWithIPFS(index, config); err != nil {
		logger.Printf("Warning: failed to update main README: %v", err)
	}

	return nil
}

func takeScheduledSnapshot(ipfs *IPFSNode) error {
	logger.Printf("Taking scheduled snapshot...")

	height, err := getLatestBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to get latest block height: %v", err)
	}

	logger.Printf("Latest block height: %d", height)

	snapshotDir, err := ensureSnapshotDir(height)
	if err != nil {
		return fmt.Errorf("failed to ensure snapshot directory: %v", err)
	}

	if err := takeSnapshot(height); err != nil {
		return fmt.Errorf("failed to take snapshot: %v", err)
	}

	// Add snapshot to index and IPFS
	config := LoadConfig()
	if err := addSnapshotToIndex(ipfs, config, height, snapshotDir); err != nil {
		logger.Printf("Warning: failed to add snapshot to index: %v", err)
	}

	// Prune old snapshots if needed
	if err := pruneOldSnapshots(ipfs); err != nil {
		logger.Printf("Warning: failed to prune old snapshots: %v", err)
	}

	return nil
}

func pruneOldSnapshots(ipfs *IPFSNode) error {
	config := LoadConfig()
	logger.Printf("Checking if snapshots need pruning...")

	// List all snapshot directories
	entries, err := os.ReadDir(config.SnapshotDir)
	if err != nil {
		return fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	// Find all snapshot directories and calculate their sizes
	var snapshots []struct {
		path   string
		height int64
		time   time.Time
		size   int64
	}

	var totalSize int64

	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) < 7 || entry.Name()[:7] != "height_" {
			continue
		}

		// Extract height number
		var height int64
		if _, err := fmt.Sscanf(entry.Name(), "height_%d", &height); err != nil {
			logger.Printf("Warning: couldn't parse height from directory name %s", entry.Name())
			continue
		}

		path := filepath.Join(config.SnapshotDir, entry.Name())

		// Calculate directory size
		size, err := getDirSize(path)
		if err != nil {
			logger.Printf("Warning: couldn't calculate size for directory %s: %v", path, err)
			continue
		}

		totalSize += size

		info, err := os.Stat(path)
		if err != nil {
			logger.Printf("Warning: couldn't stat directory %s: %v", path, err)
			continue
		}

		snapshots = append(snapshots, struct {
			path   string
			height int64
			time   time.Time
			size   int64
		}{
			path:   path,
			height: height,
			time:   info.ModTime(),
			size:   size,
		})
	}

	logger.Printf("Total snapshots size: %d bytes (%.2f GB)",
		totalSize, float64(totalSize)/(1024*1024*1024))

	// The pruning threshold: 69GB in bytes
	const pruningThreshold = int64(69) * 1024 * 1024 * 1024

	// If we're below the threshold, no need to prune
	if totalSize <= pruningThreshold {
		logger.Printf("Total size (%.2f GB) is below pruning threshold (69 GB), no pruning needed",
			float64(totalSize)/(1024*1024*1024))
		return nil
	}

	logger.Printf("Need to prune snapshots: %.2f GB > 69 GB threshold",
		float64(totalSize)/(1024*1024*1024))

	// Sort snapshots by time (oldest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].time.Before(snapshots[j].time)
	})

	// Calculate how much we need to free up to get below threshold
	// Include a 5% buffer to avoid immediate re-pruning
	bufferThreshold := float64(pruningThreshold) * 0.95
	spaceToFree := totalSize - int64(bufferThreshold)

	logger.Printf("Need to free up %.2f GB", float64(spaceToFree)/(1024*1024*1024))

	// Keep removing oldest snapshots until we've cleared enough space
	var freedSpace int64
	for i := 0; i < len(snapshots) && freedSpace < spaceToFree; i++ {
		snapshot := snapshots[i]
		logger.Printf("Removing snapshot at height %d (%.2f MB)",
			snapshot.height, float64(snapshot.size)/(1024*1024))

		// Unpin from IPFS first
		// Use snapshot directory name to lookup in index

		if err := os.RemoveAll(snapshot.path); err != nil {
			logger.Printf("Warning: failed to remove snapshot directory %s: %v",
				snapshot.path, err)
			continue
		}

		freedSpace += snapshot.size
		logger.Printf("Freed %.2f MB, total freed: %.2f MB",
			float64(snapshot.size)/(1024*1024), float64(freedSpace)/(1024*1024))
	}

	return nil
}

// Helper function to calculate directory size
func getDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func takeAndUploadSnapshot(node *IPFSNode, index *SnapshotIndex, config *Config) error {
	height, err := getLatestBlockHeight()
	if err != nil {
		return fmt.Errorf("failed to get latest block height: %v", err)
	}

	snapshotDir, err := ensureSnapshotDir(height)
	if err != nil {
		return fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	if err := takeSnapshotCore(height); err != nil {
		return fmt.Errorf("failed to take snapshot: %v", err)
	}

	stat, err := os.Stat(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to get snapshot size: %v", err)
	}

	cid, err := node.AddPath(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to add snapshot to IPFS: %v", err)
	}

	snapshot := IPFSSnapshot{
		Height:  height,
		Time:    time.Now(),
		Path:    snapshotDir,
		IPFSCID: cid,
		Size:    stat.Size(),
	}
	index.Snapshots = append(index.Snapshots, snapshot)

	indexFile := filepath.Join(config.SnapshotDir, "index.json")
	if err := saveSnapshotIndex(*index, config); err != nil {
		return fmt.Errorf("failed to save index: %v", err)
	}

	rootCID, err := node.AddPath(indexFile)
	if err != nil {
		return fmt.Errorf("failed to add index to IPFS: %v", err)
	}

	index.RootCID = rootCID
	if err := saveSnapshotIndex(*index, config); err != nil {
		return fmt.Errorf("failed to save index: %v", err)
	}

	return updateReadmeWithIPFS(*index, config)
}

func cleanupOldSnapshots(index *SnapshotIndex, config *Config) {
	if len(index.Snapshots) <= config.MaxSnapshotsToKeep {
		return
	}

	sort.Slice(index.Snapshots, func(i, j int) bool {
		return index.Snapshots[i].Time.Before(index.Snapshots[j].Time)
	})

	for len(index.Snapshots) > config.MaxSnapshotsToKeep {
		oldest := index.Snapshots[0]
		if err := os.RemoveAll(oldest.Path); err != nil {
			log.Printf("Failed to remove old snapshot %s: %v", oldest.Path, err)
		}
		index.Snapshots = index.Snapshots[1:]
	}

	if err := saveSnapshotIndex(*index, config); err != nil {
		log.Printf("Failed to save updated index: %v", err)
	}
}

// nolint:unused
func manageMutualPinning(node *IPFSNode, config *Config) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	ch := ticker.C
	for range ch {
		index := loadSnapshotIndex(config)
		for _, snapshot := range index.Snapshots {
			go func(cid string) {
				if err := node.PinCID(cid); err != nil {
					logger.Printf("Failed to pin %s: %v", cid, err)
				}
			}(snapshot.IPFSCID)
		}
	}
}
