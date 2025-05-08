package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/ipfs/boxo/files"
	corepath "github.com/ipfs/boxo/path"
	"github.com/ipfs/kubo/config" // Corrected import for Kubo config
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	iface "github.com/ipfs/kubo/core/coreiface"
	"github.com/ipfs/kubo/core/node"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type IPFSNode struct {
	node   *core.IpfsNode
	api    iface.CoreAPI
	ctx    context.Context
	cancel context.CancelFunc
	config *Config          // Changed to use the main application Config
	pinned map[string]int64 // CID -> Size in bytes
	mu     sync.Mutex
}

func NewIPFSNode(ctx context.Context, appConfig *Config) (*IPFSNode, error) { // Takes main Config
	nodeCtx, cancel := context.WithCancel(ctx)

	if err := os.MkdirAll(appConfig.IPFSRepoPath, 0755); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create IPFS repo directory %s: %w", appConfig.IPFSRepoPath, err)
	}

	if !fsrepo.IsInitialized(appConfig.IPFSRepoPath) {
		cfg, err := config.Init(os.Stdout, 2048) // Use Kubo's config.Init
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create default IPFS config: %w", err)
		}
		// Customize config (e.g., swarm addresses, API, Gateway)
		cfg.Addresses.Swarm = []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip6/::/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
			"/ip6/::/udp/4001/quic-v1",
		}
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/5001"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/8080"}

		// Set custom bootstrap peers from config if provided
		if len(appConfig.BootstrapPeers) > 0 {
			cfg.Bootstrap = appConfig.BootstrapPeers
		}

		if err := fsrepo.Init(appConfig.IPFSRepoPath, cfg); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to init IPFS repo at %s: %w", appConfig.IPFSRepoPath, err)
		}
		logger.Printf("Initialized new IPFS repository at %s", appConfig.IPFSRepoPath)
	}

	repo, err := fsrepo.Open(appConfig.IPFSRepoPath)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to open IPFS repo at %s: %w", appConfig.IPFSRepoPath, err)
	}

	// Construct the node
	nodeOptions := &node.BuildCfg{
		Online: true, // Online mode connects to P2P network
		Repo:   repo,
		// ExtraOpts can enable/disable experimental features or specific subsystems
		ExtraOpts: map[string]bool{
			"pubsub": true, // Example: Enable pubsub
			"ipnsps": true, // Example: Enable IPNS pubsub
		},
	}

	// Setup plugins. Usually an empty path means load bundled plugins.
	plugins, err := loader.NewPluginLoader("")
	if err != nil {
		cancel()
		repo.Close()
		return nil, fmt.Errorf("error loading IPFS plugins: %w", err)
	}
	if err := plugins.Initialize(); err != nil {
		cancel()
		repo.Close()
		return nil, fmt.Errorf("error initializing IPFS plugins: %w", err)
	}
	if err := plugins.Inject(); err != nil {
		cancel()
		repo.Close()
		return nil, fmt.Errorf("error injecting IPFS plugins: %w", err)
	}

	n, err := core.NewNode(nodeCtx, nodeOptions)
	if err != nil {
		cancel()
		repo.Close()
		return nil, fmt.Errorf("failed to create IPFS node: %w", err)
	}

	api, err := coreapi.NewCoreAPI(n)
	if err != nil {
		n.Close() // Close the node if API fails
		cancel()
		repo.Close() // Close repo if API fails
		return nil, fmt.Errorf("failed to get IPFS CoreAPI: %w", err)
	}

	ipfsNode := &IPFSNode{
		node:   n,
		api:    api,
		ctx:    nodeCtx,
		cancel: cancel,
		config: appConfig,
		pinned: make(map[string]int64),
	}

	// Load existing pins from the IPFS node to populate internal `pinned` map
	if err := ipfsNode.loadExistingPins(); err != nil {
		logger.Printf("Warning: failed to load existing IPFS pins: %v", err)
		// Continue, as this is not fatal for startup
	}

	// Connect to bootstrap peers (configured + default Kubo ones)
	if err := ipfsNode.connectToBootstrapPeers(); err != nil {
		logger.Printf("Warning: failed to connect to some bootstrap peers: %v", err)
	}

	logger.Printf("IPFS Node ID: %s", n.Identity.String())
	logger.Printf("IPFS Swarm listening on: %v", n.PeerHost.Addrs())

	return ipfsNode, nil
}

func (n *IPFSNode) loadExistingPins() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	pins, err := n.api.Pin().Ls(n.ctx, coreiface.Pin.Recursive())
	if err != nil {
		return fmt.Errorf("failed to list pins: %w", err)
	}

	totalSize := int64(0)
	for pin := range pins {
		if pin.Err() != nil {
			logger.Printf("Error iterating pins: %v", pin.Err())
			continue
		}
		// Getting the size of an arbitrary CID without having the content locally is non-trivial.
		// For pins made by this daemon, we store size. For others, we might need to estimate or ignore.
		// For now, we only track pins made by this daemon via AddPath or PinCID with size.
		// This function mainly ensures our internal map `n.pinned` is somewhat consistent if started with existing repo.
		// However, accurately getting sizes for all pre-existing pins is hard.
		// Let's assume for now that we primarily manage pins added through this application.
		// We can get stats for a CID IF the data is local.
		stat, err := n.api.Object().Stat(n.ctx, pin.Path())
		if err == nil {
			n.pinned[pin.Path().RootCid().String()] = int64(stat.CumulativeSize)
			totalSize += int64(stat.CumulativeSize)
		} else {
			// If we can't stat it, maybe it's not fully local. Use a placeholder or skip.
			// For simplicity, we'll only fully account for what we add/pin via our methods.
		}
	}
	logger.Printf("IPFS: Loaded %d existing pins, estimated total size: %d bytes", len(n.pinned), totalSize)
	return nil
}

func (n *IPFSNode) connectToBootstrapPeers() error {
	var wg sync.WaitGroup

	// Get bootstrap peers from IPFS config (these are the default Kubo ones initially)
	cfg, err := n.node.Repo.Config()
	if err != nil {
		return fmt.Errorf("failed to get IPFS repo config: %w", err)
	}

	// Combine with peers from our app config
	allPeers := cfg.Bootstrap
	if len(n.config.BootstrapPeers) > 0 { // These are app-specific IPFS bootstrap peers
		allPeers = append(allPeers, n.config.BootstrapPeers...)
	}

	// Deduplicate
	peerMap := make(map[string]bool)
	uniquePeers := []string{}
	for _, peerAddr := range allPeers {
		if !peerMap[peerAddr] {
			peerMap[peerAddr] = true
			uniquePeers = append(uniquePeers, peerAddr)
		}
	}

	for _, addr := range uniquePeers {
		if addr == "" {
			continue
		}
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			targetAddr, err := ma.NewMultiaddr(address)
			if err != nil {
				logger.Printf("IPFS: Failed to parse bootstrap peer address '%s': %s", address, err)
				return
			}
			targetInfo, err := peer.AddrInfoFromP2pAddr(targetAddr)
			if err != nil {
				logger.Printf("IPFS: Failed to create peer info from bootstrap address '%s': %s", address, err)
				return
			}
			connectCtx, cancel := context.WithTimeout(n.ctx, 30*time.Second) // Connection timeout
			defer cancel()
			if err := n.node.PeerHost.Connect(connectCtx, *targetInfo); err != nil {
				logger.Printf("IPFS: Failed to connect to bootstrap peer %s: %s", address, err)
			} else {
				logger.Printf("IPFS: Successfully connected to bootstrap peer: %s", address)
			}
		}(addr)
	}
	wg.Wait()
	logger.Printf("IPFS: Bootstrap connection process complete.")
	return nil
}

// AddPath adds a file or directory to IPFS and pins it.
// Returns the CID string.
func (n *IPFSNode) AddPath(path string) (string, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("failed to stat path %s: %w", path, err)
	}

	// Create a files.Node from the filesystem path
	fileNode, err := files.NewSerialFile(path, false, stat)
	if err != nil {
		return "", fmt.Errorf("failed to create serial file for %s: %w", path, err)
	}

	// Add to IPFS UnixFS
	// Consider options likecid.Pin, cid.RawLeaves(true/false)
	ipfsPath, err := n.api.Unixfs().Add(n.ctx, fileNode, coreiface.Unixfs.Pin(true))
	if err != nil {
		return "", fmt.Errorf("failed to add path %s to IPFS: %w", path, err)
	}

	cidStr := ipfsPath.RootCid().String()

	// Pin the added content (Add with Pin(true) should handle this, but explicit Add can be used too)
	// if err := n.api.Pin().Add(n.ctx, ipfsPath); err != nil {
	// 	return "", fmt.Errorf("failed to pin CID %s for path %s: %w", cidStr, path, err)
	// }

	n.mu.Lock()
	defer n.mu.Unlock()

	// Update total pinned size
	currentTotalSize := int64(0)
	for _, s := range n.pinned {
		currentTotalSize += s
	}

	if currentTotalSize+stat.Size() > n.config.MaxPinnedSizeBytes && n.config.MaxPinnedSizeBytes > 0 {
		// Attempt to unpin to make space is complex here.
		// Better to prevent adding if it exceeds. Pruning should handle space management.
		logger.Printf("Warning: Pinning %s (size %d) might exceed total max pinned size %d (current: %d). Pruning should manage this.",
			cidStr, stat.Size(), n.config.MaxPinnedSizeBytes, currentTotalSize)
		// Not returning error, allowing pin but relying on pruning.
	}

	n.pinned[cidStr] = stat.Size()
	logger.Printf("IPFS: Added and pinned path %s -> CID %s (Size: %d bytes)", path, cidStr, stat.Size())
	return cidStr, nil
}

// PinCID pins a given CID. The size must be provided for accounting.
func (n *IPFSNode) PinCID(cidStr string, size int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.pinned[cidStr]; exists {
		// logger.Printf("IPFS: CID %s already in managed pinset.", cidStr)
		return nil // Already tracked, assume pinned
	}

	totalSize := int64(0)
	for _, s := range n.pinned {
		totalSize += s
	}

	if n.config.MaxPinnedSizeBytes > 0 && totalSize+size > n.config.MaxPinnedSizeBytes {
		return fmt.Errorf("pinning %s (size %d) would exceed %d byte limit (current total: %d)",
			cidStr, size, n.config.MaxPinnedSizeBytes, totalSize)
	}

	parsedPath, err := corepath.NewPath("/ipfs/" + cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID '%s' for pinning: %w", cidStr, err)
	}

	// Check if already pinned in IPFS daemon
	pinInfo, err := n.api.Pin().IsPinned(n.ctx, parsedPath)
	isPinned := false
	if err == nil && len(pinInfo) > 0 && pinInfo[0].Pinned() { // Check specific pin type if needed
		isPinned = true
	}

	if !isPinned {
		if err := n.api.Pin().Add(n.ctx, parsedPath); err != nil {
			return fmt.Errorf("failed to pin CID %s in IPFS daemon: %w", cidStr, err)
		}
		logger.Printf("IPFS: Successfully pinned CID %s (Size: %d bytes)", cidStr, size)
	} else {
		// logger.Printf("IPFS: CID %s was already pinned in daemon.", cidStr)
	}

	n.pinned[cidStr] = size // Add to our tracking map
	return nil
}

func (n *IPFSNode) UnpinCID(cidStr string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	parsedPath, err := corepath.NewPath("/ipfs/" + cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID '%s' for unpinning: %w", cidStr, err)
	}

	// Check if it's actually pinned before trying to remove
	pinInfo, err := n.api.Pin().IsPinned(n.ctx, parsedPath)
	if err != nil {
		// If error is "not pinned", then our job is done.
		if strings.Contains(err.Error(), "not pinned") || strings.Contains(err.Error(), "is not pinned") {
			logger.Printf("IPFS: CID %s was not pinned in daemon, no action needed for unpin.", cidStr)
			delete(n.pinned, cidStr) // Remove from our tracking
			return nil
		}
		return fmt.Errorf("failed to check pin status for %s: %w", cidStr, err)
	}

	isEffectivelyPinned := false
	for _, p := range pinInfo {
		if p.Pinned() { // Check for any type of pin (Recursive, Direct, etc.)
			isEffectivelyPinned = true
			break
		}
	}

	if !isEffectivelyPinned {
		logger.Printf("IPFS: CID %s reported as not effectively pinned by daemon, no unpin action needed.", cidStr)
		delete(n.pinned, cidStr)
		return nil
	}

	if err := n.api.Pin().Rm(n.ctx, parsedPath); err != nil {
		// Kubo might return an error if trying to unpin a CID that's part of another pin (e.g. a file in a pinned dir)
		// or if it's not pinned. Check error message.
		if strings.Contains(err.Error(), "not pinned") {
			logger.Printf("IPFS: Unpin attempt for %s reported as 'not pinned' by daemon.", cidStr)
		} else {
			return fmt.Errorf("failed to unpin CID %s from IPFS daemon: %w", cidStr, err)
		}
	} else {
		logger.Printf("IPFS: Successfully unpinned CID %s.", cidStr)
	}

	delete(n.pinned, cidStr) // Remove from our tracking map
	return nil
}

func (n *IPFSNode) Close() error {
	logger.Printf("Closing IPFS node...")
	n.cancel() // Signal context cancellation to internal operations

	// Explicitly close the CoreAPI, which can help release resources.
	// The n.node.Close() should handle this, but being explicit can be good.
	// if closer, ok := n.api.(io.Closer); ok {
	// 	if err := closer.Close(); err != nil {
	// 		logger.Printf("Error closing IPFS CoreAPI: %v", err)
	// 	}
	// }

	err := n.node.Close()
	if err != nil {
		logger.Printf("Error closing IPFS node: %v", err)
		return err
	}

	// repo is closed by node.Close()
	logger.Printf("IPFS node closed.")
	return nil
}
