package main

import (
	"context"
	"fmt"
	"os"
	"strings" // Added import
	"sync"
	"time" // Added import

	"github.com/ipfs/boxo/files"
	corepath "github.com/ipfs/boxo/path"
	"github.com/ipfs/kubo/config" // Corrected import for Kubo config
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	iface "github.com/ipfs/kubo/core/coreiface"   // Renamed import
	"github.com/ipfs/kubo/core/coreiface/options" // Added import for options
	"github.com/ipfs/kubo/core/node"
	"github.com/ipfs/kubo/plugin/loader"
	"github.com/ipfs/kubo/repo/fsrepo"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type IPFSNode struct {
	node   *core.IpfsNode
	api    iface.CoreAPI // Used renamed import
	ctx    context.Context
	cancel context.CancelFunc
	config *Config
	pinned map[string]int64 // CID -> Size in bytes
	mu     sync.Mutex
}

func NewIPFSNode(ctx context.Context, appConfig *Config) (*IPFSNode, error) {
	nodeCtx, cancel := context.WithCancel(ctx)

	if err := os.MkdirAll(appConfig.IPFSRepoPath, 0755); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create IPFS repo directory %s: %w", appConfig.IPFSRepoPath, err)
	}

	if !fsrepo.IsInitialized(appConfig.IPFSRepoPath) {
		cfg, err := config.Init(os.Stdout, 2048)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create default IPFS config: %w", err)
		}
		cfg.Addresses.Swarm = []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip6/::/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
			"/ip6/::/udp/4001/quic-v1",
		}
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/5001"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/8080"}
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

	plugins, err := loader.NewPluginLoader("")
	if err != nil {
		cancel()
		if errClose := repo.Close(); errClose != nil {
			logger.Printf("IPFS: Error closing repo after plugin load failure: %v", errClose)
		}
		return nil, fmt.Errorf("error loading IPFS plugins: %w", err)
	}
	if err := plugins.Initialize(); err != nil {
		cancel()
		if errClose := repo.Close(); errClose != nil {
			logger.Printf("IPFS: Error closing repo after plugin init failure: %v", errClose)
		}
		return nil, fmt.Errorf("error initializing IPFS plugins: %w", err)
	}
	if err := plugins.Inject(); err != nil {
		cancel()
		if errClose := repo.Close(); errClose != nil {
			logger.Printf("IPFS: Error closing repo after plugin inject failure: %v", errClose)
		}
		return nil, fmt.Errorf("error injecting IPFS plugins: %w", err)
	}

	nodeOptions := &node.BuildCfg{
		Online: true,
		Repo:   repo,
		ExtraOpts: map[string]bool{
			"pubsub": true,
			"ipnsps": true,
		},
	}

	n, err := core.NewNode(nodeCtx, nodeOptions)
	if err != nil {
		cancel()
		repo.Close()
		return nil, fmt.Errorf("failed to create IPFS node: %w", err)
	}

	api, err := coreapi.NewCoreAPI(n)
	if err != nil {
		if errNodeClose := n.Close(); errNodeClose != nil {
			logger.Printf("IPFS: Error closing node after CoreAPI failure: %v", errNodeClose)
		}
		cancel()
		if errClose := repo.Close(); errClose != nil {
			logger.Printf("IPFS: Error closing repo after CoreAPI failure: %v", errClose)
		}
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

	if err := ipfsNode.loadExistingPins(); err != nil {
		logger.Printf("Warning: failed to load existing IPFS pins: %v", err)
	}

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

	pins, err := n.api.Pin().Ls(n.ctx, options.Pin.Ls.Recursive())
	if err != nil {
		return fmt.Errorf("failed to list pins: %w", err)
	}

	totalSize := int64(0)
	for pin := range pins {
		if pin.Err() != nil {
			logger.Printf("Error iterating pins: %v", pin.Err())
			continue
		}
		stat, err := n.api.Object().Stat(n.ctx, pin.Path())
		if err == nil {
			n.pinned[pin.Path().RootCid().String()] = int64(stat.CumulativeSize)
			totalSize += int64(stat.CumulativeSize)
		}
	}
	logger.Printf("IPFS: Loaded %d existing pins, estimated total size: %d bytes", len(n.pinned), totalSize)
	return nil
}

func (n *IPFSNode) connectToBootstrapPeers() error {
	var wg sync.WaitGroup
	cfg, err := n.node.Repo.Config()
	if err != nil {
		return fmt.Errorf("failed to get IPFS repo config: %w", err)
	}
	allPeers := cfg.Bootstrap
	if len(n.config.BootstrapPeers) > 0 {
		allPeers = append(allPeers, n.config.BootstrapPeers...)
	}
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
			connectCtx, cancelConnect := context.WithTimeout(n.ctx, 30*time.Second) // Corrected: time.Second
			defer cancelConnect()
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

func (n *IPFSNode) AddPath(path string) (string, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("failed to stat path %s: %w", path, err)
	}
	fileNode, err := files.NewSerialFile(path, false, stat)
	if err != nil {
		return "", fmt.Errorf("failed to create serial file for %s: %w", path, err)
	}
	ipfsPath, err := n.api.Unixfs().Add(n.ctx, fileNode, options.Unixfs.Pin(true)) // Corrected: options.Unixfs
	if err != nil {
		return "", fmt.Errorf("failed to add path %s to IPFS: %w", path, err)
	}
	cidStr := ipfsPath.RootCid().String()
	n.mu.Lock()
	defer n.mu.Unlock()
	currentTotalSize := int64(0)
	for _, s := range n.pinned {
		currentTotalSize += s
	}
	if currentTotalSize+stat.Size() > n.config.MaxPinnedSizeBytes && n.config.MaxPinnedSizeBytes > 0 {
		logger.Printf("Warning: Pinning %s (size %d) might exceed total max pinned size %d (current: %d). Pruning should manage this.",
			cidStr, stat.Size(), n.config.MaxPinnedSizeBytes, currentTotalSize)
	}
	n.pinned[cidStr] = stat.Size()
	logger.Printf("IPFS: Added and pinned path %s -> CID %s (Size: %d bytes)", path, cidStr, stat.Size())
	return cidStr, nil
}

func (n *IPFSNode) PinCID(cidStr string, size int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.pinned[cidStr]; exists {
		return nil
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

	_, pinnedStatus, err := n.api.Pin().IsPinned(n.ctx, parsedPath) // Corrected assignment

	if !pinnedStatus { // If not pinned (or IsPinned had an error that implies not pinned)
		if err := n.api.Pin().Add(n.ctx, parsedPath); err != nil {
			return fmt.Errorf("failed to pin CID %s in IPFS daemon: %w", cidStr, err)
		}
		logger.Printf("IPFS: Successfully pinned CID %s (Size: %d bytes)", cidStr, size)
	}
	n.pinned[cidStr] = size
	return nil
}

func (n *IPFSNode) UnpinCID(cidStr string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	parsedPath, err := corepath.NewPath("/ipfs/" + cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID '%s' for unpinning: %w", cidStr, err)
	}

	reason, pinnedStatus, err := n.api.Pin().IsPinned(n.ctx, parsedPath) // Corrected assignment
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "not pinned") { // Corrected: strings.Contains
			logger.Printf("IPFS: CID %s was not pinned in daemon (check error: %v), no action needed for unpin.", cidStr, err)
			delete(n.pinned, cidStr)
			return nil
		}
		return fmt.Errorf("failed to check pin status for %s: %w (reason: %s)", cidStr, err, reason)
	}
	if !pinnedStatus {
		logger.Printf("IPFS: CID %s reported as not effectively pinned by daemon (reason: %s), no unpin action needed.", cidStr, reason)
		delete(n.pinned, cidStr)
		return nil
	}

	if err := n.api.Pin().Rm(n.ctx, parsedPath); err != nil {
		if strings.Contains(err.Error(), "not pinned") { // Corrected: strings.Contains
			logger.Printf("IPFS: Unpin attempt for %s reported as 'not pinned' by daemon.", cidStr)
		} else {
			return fmt.Errorf("failed to unpin CID %s from IPFS daemon: %w", cidStr, err)
		}
	} else {
		logger.Printf("IPFS: Successfully unpinned CID %s.", cidStr)
	}
	delete(n.pinned, cidStr)
	return nil
}

func (n *IPFSNode) Close() error {
	logger.Printf("Closing IPFS node...")
	n.cancel()
	err := n.node.Close()
	if err != nil {
		logger.Printf("Error closing IPFS node: %v", err)
		return err
	}
	logger.Printf("IPFS node closed.")
	return nil
}
