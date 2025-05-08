package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// --- Structs for Chain Registry Data ---

// BasicChainInfo holds essential details loaded from the registry.
type BasicChainInfo struct {
	RegistryName string // Directory name in the registry (e.g., "cosmoshub")
	ChainName    string `json:"chain_name"` // Pretty name
	ChainID      string `json:"chain_id"`
	NetworkType  string `json:"network_type"` // "mainnet", "testnet", "devnet"
	PrettyName   string `json:"pretty_name"`
	Website      string `json:"website"`
	Bech32Prefix string `json:"bech32_prefix"`    // From address_prefix in chain.json
	Slip44       int    `json:"slip44,omitempty"` // from slip44 in chain.json
	// APIs         APIs     `json:"apis"` // Can be complex, load selectively if needed
	Peers Peers `json:"peers"`
	// Add other fields as needed (e.g., codebase, status, logo_URIs)
}

// Address prefix field
// type AddressPrefix struct {
// 	AddressPrefix string `json:"address_prefix"`
// }

// APIs contains endpoint lists (simplified).
// type APIs struct {
// 	RPC  []Endpoint `json:"rpc"`
// 	REST []Endpoint `json:"rest"`
// 	GRPC []Endpoint `json:"grpc"`
// }

// Endpoint represents a single API endpoint.
// type Endpoint struct {
// 	Address  string `json:"address"`
// 	Provider string `json:"provider"`
// }

// Peers contains seed and persistent peer lists.
type Peers struct {
	Seeds           []PeerInfo `json:"seeds"`
	PersistentPeers []PeerInfo `json:"persistent_peers"`
}

// PeerInfo represents a seed or persistent peer.
type PeerInfo struct {
	ID       string `json:"id"`       // Node ID
	Address  string `json:"address"`  // Host:Port or IP:Port
	Provider string `json:"provider"` // Optional provider name
}

// --- Registry Loading Logic ---

// LoadRegistryChains scans the chain registry path and returns info for matching chains.
func LoadRegistryChains(registryPath string, networkTypeFilter string) (map[string]*BasicChainInfo, error) {
	chains := make(map[string]*BasicChainInfo)
	logger.Printf("Loading chains from registry path: %s (Filter: %s)", registryPath, networkTypeFilter)

	// Walk the registry directory
	err := filepath.WalkDir(registryPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			// Skip directories that can't be read, log warning
			logger.Printf("Warning: Error accessing path %q: %v", path, err)
			return filepath.SkipDir // Skip this directory if inaccessible
		}

		// Look for chain.json files, but not within hidden dirs (.git) or assetlist.json etc.
		if !d.IsDir() && d.Name() == "chain.json" {
			// Check if parent directory is hidden
			parentDir := filepath.Dir(path)
			if strings.HasPrefix(filepath.Base(parentDir), ".") {
				return nil // Skip hidden directories like .git
			}
			// Also skip if it's at the root of the registry path
			relPath, _ := filepath.Rel(registryPath, parentDir)
			if relPath == "." {
				return nil // Skip chain.json at the very root
			}

			registryName := filepath.Base(parentDir) // Use the folder name as the key

			// Read and parse chain.json
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				logger.Printf("Warning: Failed to read %s: %v", path, readErr)
				return nil // Continue walking
			}

			var chainInfo BasicChainInfo
			if parseErr := json.Unmarshal(data, &chainInfo); parseErr != nil {
				logger.Printf("Warning: Failed to parse %s: %v", path, parseErr)
				return nil // Continue walking
			}

			// Apply filter
			if networkTypeFilter == "" || strings.EqualFold(chainInfo.NetworkType, networkTypeFilter) {
				// Basic validation
				if chainInfo.ChainID == "" || chainInfo.ChainName == "" {
					logger.Printf("Warning: Skipping %s, missing chain_id or chain_name.", path)
					return nil
				}

				// Populate derived/missing fields
				chainInfo.RegistryName = registryName
				if chainInfo.PrettyName == "" {
					chainInfo.PrettyName = chainInfo.ChainName
				}

				// Bech32 prefix might be nested differently, adjust if needed based on registry structure
				// Example: Look for "bech32_prefix" directly
				var extraData map[string]interface{}
				if json.Unmarshal(data, &extraData) == nil {
					if prefix, ok := extraData["bech32_prefix"].(string); ok {
						chainInfo.Bech32Prefix = prefix
					}
				}

				// TODO: Optionally parse API endpoints if needed as hints
				// var apiData struct { Apis APIs `json:"apis"` }
				// json.Unmarshal(data, &apiData)
				// chainInfo.APIs = apiData.Apis

				chains[registryName] = &chainInfo
				// logger.Printf("Loaded chain: %s (ID: %s)", chainInfo.PrettyName, chainInfo.ChainID)
			}
		}
		return nil // Continue walking
	})

	if err != nil {
		return nil, fmt.Errorf("error walking registry path %s: %w", registryPath, err)
	}

	logger.Printf("Finished loading registry. Found %d chains matching filter '%s'.", len(chains), networkTypeFilter)
	return chains, nil
}

// Helper to get P2P addresses from PeerInfo slice
func getP2PAddresses(peers []PeerInfo) []string {
	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		// Format: id@address (if ID is present) or just address
		if p.ID != "" && p.Address != "" {
			addrs = append(addrs, fmt.Sprintf("%s@%s", p.ID, p.Address))
		} else if p.Address != "" {
			addrs = append(addrs, p.Address)
		}
	}
	return addrs
}
