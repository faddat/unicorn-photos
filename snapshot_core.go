package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// IPFSSnapshot represents a single snapshot in IPFS
type IPFSSnapshot struct {
	ChainID          string    `json:"chain_id"`
	Height           int64     `json:"height"`
	Time             time.Time `json:"time"` // Time snapshot was processed
	Path             string    `json:"path"` // Relative path within snapshot_base_dir
	IPFSCID          string    `json:"ipfs_cid"`
	Size             int64     `json:"size"`                        // Size in bytes
	GenesisTimestamp time.Time `json:"genesis_timestamp,omitempty"` // Timestamp from genesis.json
}

// SnapshotIndex tracks all snapshots and their IPFS CIDs
type SnapshotIndex struct {
	Snapshots   []IPFSSnapshot `json:"snapshots"` // Should be kept sorted for easier processing
	RootCID     string         `json:"root_cid"`  // CID of this index.json file itself on IPFS
	LastUpdated time.Time      `json:"last_updated"`
}

// loadSnapshotIndex loads the snapshot index from disk
func loadSnapshotIndex(config *Config) SnapshotIndex {
	indexPath := filepath.Join(config.SnapshotBaseDir, "index.json")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Printf("Snapshot index file not found at %s, creating new.", indexPath)
		} else {
			logger.Printf("Error reading snapshot index %s: %v. Starting with empty index.", indexPath, err)
		}
		return SnapshotIndex{Snapshots: make([]IPFSSnapshot, 0)}
	}

	var index SnapshotIndex
	if err := json.Unmarshal(data, &index); err != nil {
		logger.Printf("Error unmarshalling snapshot index %s: %v. Starting with empty index.", indexPath, err)
		return SnapshotIndex{Snapshots: make([]IPFSSnapshot, 0)}
	}
	// Ensure snapshots are sorted on load, primarily by ChainID then by Height descending
	sortSnapshots(index.Snapshots)
	return index
}

func sortSnapshots(snapshots []IPFSSnapshot) {
	sort.SliceStable(snapshots, func(i, j int) bool {
		if snapshots[i].ChainID != snapshots[j].ChainID {
			return snapshots[i].ChainID < snapshots[j].ChainID
		}
		// Sort by height descending (newest first)
		return snapshots[i].Height > snapshots[j].Height
	})
}

// saveSnapshotIndex saves the snapshot index to disk
func saveSnapshotIndex(index SnapshotIndex, config *Config) error {
	index.LastUpdated = time.Now().UTC()
	sortSnapshots(index.Snapshots) // Ensure sorted before save

	data, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot index for saving: %w", err)
	}
	indexPath := filepath.Join(config.SnapshotBaseDir, "index.json")
	if err := os.WriteFile(indexPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot index to %s: %w", indexPath, err)
	}
	return nil
}

// addSnapshotToIndex adds a new snapshot to the index, saves the index, and publishes the index to IPFS.
func addSnapshotToIndex(node *IPFSNode, config *Config, chainID string, height int64, snapshotDir string) error {
	index := loadSnapshotIndex(config)

	// Check if this specific snapshot (chainID + height) already exists
	for i, s := range index.Snapshots {
		if s.ChainID == chainID && s.Height == height {
			logger.Printf("[%s] Snapshot at height %d already indexed. Updating if CID/Size changed.", chainID, height)

			// Re-calculate size and re-add to IPFS to get potentially new CID (if content changed)
			newSize, err := getDirSize(snapshotDir)
			if err != nil {
				return fmt.Errorf("failed to recalculate size for existing snapshot %s: %w", snapshotDir, err)
			}
			newCid, err := node.AddPath(snapshotDir) // AddPath also pins
			if err != nil {
				return fmt.Errorf("failed to re-add existing snapshot %s to IPFS: %w", snapshotDir, err)
			}

			index.Snapshots[i].IPFSCID = newCid
			index.Snapshots[i].Size = newSize
			index.Snapshots[i].Time = time.Now().UTC() // Update timestamp
			// Potentially re-read genesis timestamp

			return SaveAndPublishIndex(index, config, node)
		}
	}

	size, err := getDirSize(snapshotDir)
	if err != nil {
		return fmt.Errorf("failed to calculate snapshot size for %s: %w", snapshotDir, err)
	}

	cid, err := node.AddPath(snapshotDir) // AddPath also pins
	if err != nil {
		return fmt.Errorf("failed to add snapshot directory %s to IPFS: %w", snapshotDir, err)
	}

	relativePath, _ := filepath.Rel(config.SnapshotBaseDir, snapshotDir)
	if strings.HasPrefix(relativePath, "../") { // Ensure it's a relative path within base dir
		relativePath = filepath.Base(snapshotDir) // Fallback for safety
	}

	var genTimestamp time.Time
	genPath := filepath.Join(snapshotDir, "genesis.json")
	if _, statErr := os.Stat(genPath); statErr == nil {
		genFile, readErr := os.ReadFile(genPath)
		if readErr == nil {
			var genDoc struct {
				GenesisTime string `json:"genesis_time"`
			}
			if json.Unmarshal(genFile, &genDoc) == nil {
				genTimestamp, _ = time.Parse(time.RFC3339Nano, genDoc.GenesisTime)
			}
		}
	}

	newSnapshot := IPFSSnapshot{
		ChainID:          chainID,
		Height:           height,
		Time:             time.Now().UTC(),
		Path:             relativePath,
		IPFSCID:          cid,
		Size:             size,
		GenesisTimestamp: genTimestamp,
	}
	index.Snapshots = append(index.Snapshots, newSnapshot)
	logger.Printf("[%s] Added snapshot for height %d to index. Local Path: %s, IPFS CID: %s, Size: %dMB",
		chainID, height, snapshotDir, cid, size/(1024*1024))

	return SaveAndPublishIndex(index, config, node)
}

// SaveAndPublishIndex saves the index locally and then publishes it to IPFS.
func SaveAndPublishIndex(index SnapshotIndex, config *Config, node *IPFSNode) error {
	if err := saveSnapshotIndex(index, config); err != nil { // Save locally first
		return err // If local save fails, don't try to publish
	}

	// Publish the updated index.json file to IPFS
	indexFilePath := filepath.Join(config.SnapshotBaseDir, "index.json")
	newIndexCID, err := node.AddPath(indexFilePath) // AddPath also pins
	if err != nil {
		logger.Printf("Warning: Failed to add updated index.json to IPFS: %v. Local index is saved.", err)
		// Non-fatal for the snapshot process itself, but the public RootCID won't update.
		return nil // Or return err if IPFS publish is critical path
	}

	// The index.RootCID should ideally point to itself after being published.
	// However, this creates a recursive definition if stored inside the file being CID'd.
	// So, RootCID in the struct might represent the *previous* CID, or be managed externally.
	// For simplicity, let's log the new CID. A separate mechanism (e.g. IPNS) would publish this new RootCID.
	logger.Printf("Successfully published updated snapshot index to IPFS. New index.json CID: %s", newIndexCID)
	// index.RootCID = newIndexCID // If we want the file to contain its own latest CID (careful with update loops)
	// if err := saveSnapshotIndex(index, config); err != nil { // Re-save if RootCID is updated
	// 	 logger.Printf("Warning: Failed to re-save index.json with its own new RootCID: %v", err)
	// }

	// Update the main README.md with this new index CID
	// This might be better done by a separate function that reads the latest index.json CID
	// and updates the README rather than being directly in the save path of the index.
	if err := updateReadmeWithIPFS(index, config, node); err != nil {
		logger.Printf("Warning: failed to update main README.md after publishing index: %v", err)
	}

	return nil
}

// updateReadmeWithIPFS creates/updates the main README.md with snapshot info
func updateReadmeWithIPFS(index SnapshotIndex, config *Config, ipfsNode *IPFSNode) error {
	readmePath := "README.md" // Assuming README.md is in the root of the project/repo

	// Get the CID of the latest index.json file from IPFS
	// This ensures the README points to the IPFS-hosted index.
	var latestIndexCID string
	indexLocalPath := filepath.Join(config.SnapshotBaseDir, "index.json")
	if _, err := os.Stat(indexLocalPath); err == nil {
		// Add the local index.json to IPFS to get its current CID
		// This is a bit redundant if SaveAndPublishIndex just did it, but ensures accuracy.
		cidStr, err := ipfsNode.AddPath(indexLocalPath)
		if err == nil {
			latestIndexCID = cidStr
		} else {
			logger.Printf("Warning: Could not get IPFS CID for local index.json to update README: %v", err)
			latestIndexCID = index.RootCID // Fallback to stored RootCID
		}
	} else {
		latestIndexCID = index.RootCID // Fallback if local index.json doesn't exist for some reason
	}

	var content strings.Builder
	content.WriteString("# Unicorn Photos - Cosmos SDK Multi-Chain Snapshots\n\n")
	content.WriteString("Automated snapshot utility for Cosmos SDK chains, stored on IPFS.\n\n")
	content.WriteString(fmt.Sprintf("**Latest Snapshot Index (index.json) on IPFS:** `ipfs://%s`\n\n", latestIndexCID))
	content.WriteString(fmt.Sprintf("Last updated: %s UTC\n\n", time.Now().UTC().Format(time.RFC1123)))

	content.WriteString("## Usage\n\n")
	content.WriteString("```bash\n# Install (from source)\n")
	content.WriteString("git clone https://github.com/your-repo/unicorn-photos.git\n")
	content.WriteString("cd unicorn-photos\n")
	content.WriteString("go install ./...\n\n")
	content.WriteString("# Run as daemon\n")
	content.WriteString("unicorn-photos -daemon\n")
	content.WriteString("```\n\n")
	content.WriteString("Configuration is typically in `config.toml` (in the working directory, `$HOME/.unicorn-photos`, or `/etc/unicorn-photos/`).\n\n")

	content.WriteString("## Available Snapshots by Chain\n\n")

	// Group snapshots by chain for the README
	snapshotsByChain := make(map[string][]IPFSSnapshot)
	for _, s := range index.Snapshots {
		snapshotsByChain[s.ChainID] = append(snapshotsByChain[s.ChainID], s)
	}

	// Sort chain IDs for consistent README output
	var chainIDs []string
	for cid := range snapshotsByChain {
		chainIDs = append(chainIDs, cid)
	}
	sort.Strings(chainIDs)

	maxDisplayPerChain := 5 // Max snapshots to list per chain in README

	for _, chainID := range chainIDs {
		content.WriteString(fmt.Sprintf("### %s (`%s`)\n\n", getChainNameFromID(chainID, config), chainID)) // Helper to get full name
		chainSnaps := snapshotsByChain[chainID]
		// Already sorted by height descending due to loadSnapshotIndex
		// sort.Slice(chainSnaps, func(i, j int) bool { return chainSnaps[i].Height > chainSnaps[j].Height })

		if len(chainSnaps) == 0 {
			content.WriteString("No snapshots available yet.\n\n")
			continue
		}

		for i, s := range chainSnaps {
			if i >= maxDisplayPerChain {
				content.WriteString(fmt.Sprintf("- ... and %d more older snapshots\n", len(chainSnaps)-maxDisplayPerChain))
				break
			}
			genTimeStr := ""
			if !s.GenesisTimestamp.IsZero() {
				genTimeStr = fmt.Sprintf(" (Genesis: %s)", s.GenesisTimestamp.Format("2006-01-02 15:04 UTC"))
			}
			content.WriteString(fmt.Sprintf("- **Height %d**: `ipfs://%s` (Size: %d MB, Taken: %s%s)\n",
				s.Height, s.IPFSCID, s.Size/(1024*1024), s.Time.Format("2006-01-02 15:04 UTC"), genTimeStr))
		}
		content.WriteString("\n")
	}

	if len(index.Snapshots) == 0 {
		content.WriteString("No snapshots available yet across any chain.\n")
	}

	content.WriteString("## Features\n\n")
	content.WriteString("- Automated, continuous snapshots for multiple configured Cosmos SDK chains.\n")
	content.WriteString("- Snapshots stored on IPFS for decentralized access.\n")
	content.WriteString("- Generates `genesis.json` compatible with starting new chains from state.\n")
	content.WriteString("- Embedded IPFS node for snapshot storage and pinning.\n")
	content.WriteString("- Configurable snapshot frequency, including 'as frequent as possible'.\n")
	content.WriteString("- Peer discovery for RPC/REST endpoints as a fallback.\n")
	content.WriteString("- Per-chain snapshot pruning based on retention count.\n")
	content.WriteString("- Mutual pinning support (experimental).\n\n")
	content.WriteString(fmt.Sprintf("This README was last auto-generated at: %s\n", time.Now().UTC().Format(time.RFC1123)))

	return os.WriteFile(readmePath, []byte(content.String()), 0644)
}

func getChainNameFromID(chainID string, config *Config) string {
	for _, c := range config.Chains {
		if c.ChainID == chainID {
			if c.Name != "" {
				return c.Name
			}
			return chainID // Fallback to chain_id if name is empty
		}
	}
	return chainID // Fallback if not found in config (should not happen if index is consistent)
}
