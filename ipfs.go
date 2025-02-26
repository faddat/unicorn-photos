package main

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/kubo/config"
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
	config *Config
	pinned map[string]int64 // CID -> Size in bytes
	mu     sync.Mutex
}

func NewIPFSNode(ctx context.Context, config *Config) (*IPFSNode, error) {
	ctx, cancel := context.WithCancel(ctx)

	if err := os.MkdirAll(config.IPFSRepoPath, 0755); err != nil {
		cancel()
		return nil, err
	}

	if !fsrepo.IsInitialized(config.IPFSRepoPath) {
		cfg, err := config.Init(os.Stdout, 2048)
		if err != nil {
			cancel()
			return nil, err
		}
		cfg.Addresses.Swarm = []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/tcp/4002/ws",
		}
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/5001"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/8080"}
		if err := fsrepo.Init(config.IPFSRepoPath, cfg); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to init repo: %s", err)
		}
	}

	repo, err := fsrepo.Open(config.IPFSRepoPath)
	if err != nil {
		cancel()
		return nil, err
	}

	plugins, err := loader.NewPluginLoader("")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("error loading plugins: %s", err)
	}
	if err := plugins.Initialize(); err != nil {
		cancel()
		return nil, fmt.Errorf("error initializing plugins: %s", err)
	}
	if err := plugins.Inject(); err != nil {
		cancel()
		return nil, fmt.Errorf("error injecting plugins: %s", err)
	}

	nodeOptions := &node.BuildCfg{
		Online: true,
		Repo:   repo,
		ExtraOpts: map[string]bool{
			"pubsub": true,
			"ipnsps": true,
			"mplex":  true,
		},
	}

	n, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		cancel()
		return nil, err
	}

	api, err := coreapi.NewCoreAPI(n)
	if err != nil {
		cancel()
		return nil, err
	}

	node := &IPFSNode{
		node:   n,
		api:    api,
		ctx:    ctx,
		cancel: cancel,
		config: config,
		pinned: make(map[string]int64),
	}

	if err := node.connectToPeers(config.BootstrapPeers); err != nil {
		logger.Printf("Warning: failed to connect to peers: %s", err)
	}

	return node, nil
}

func (n *IPFSNode) connectToPeers(peers []string) error {
	var wg sync.WaitGroup
	for _, addr := range append(peers, config.DefaultBootstrapAddresses...) {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			targetAddr, err := ma.NewMultiaddr(address)
			if err != nil {
				logger.Printf("Failed to parse peer address: %s", err)
				return
			}
			targetInfo, err := peer.AddrInfoFromP2pAddr(targetAddr)
			if err != nil {
				logger.Printf("Failed to create peer info: %s", err)
				return
			}
			if err := n.node.PeerHost.Connect(n.ctx, *targetInfo); err != nil {
				logger.Printf("Failed to connect to peer: %s", err)
			}
		}(addr)
	}
	wg.Wait()
	return nil
}

func (n *IPFSNode) AddPath(path string) (string, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	var f files.Node
	if stat.IsDir() {
		f, err = files.NewSerialFile(path, false, stat)
	} else {
		f, err = files.NewSerialFile(path, false, stat)
	}
	if err != nil {
		return "", err
	}

	ipfsPath, err := n.api.Unixfs().Add(n.ctx, f)
	if err != nil {
		return "", err
	}

	if err := n.api.Pin().Add(n.ctx, ipfsPath); err != nil {
		return "", err
	}

	cid := ipfsPath.Cid().String()
	n.mu.Lock()
	n.pinned[cid] = stat.Size()
	n.mu.Unlock()

	return cid, nil
}

func (n *IPFSNode) PinCID(cid string, size int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	totalSize := int64(0)
	for _, s := range n.pinned {
		totalSize += s
	}

	if totalSize+size > n.config.MaxPinnedSize {
		return fmt.Errorf("pinning %s would exceed 100GB limit (current: %d bytes)", cid, totalSize)
	}

	path := iface.IpfsPath(cid)
	if err := n.api.Pin().Add(n.ctx, path); err != nil {
		return err
	}
	n.pinned[cid] = size
	return nil
}

func (n *IPFSNode) UnpinCID(cid string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	path := iface.IpfsPath(cid)
	if err := n.api.Pin().Rm(n.ctx, path); err != nil {
		return err
	}
	delete(n.pinned, cid)
	return nil
}

func (n *IPFSNode) Close() error {
	if err := n.node.Close(); err != nil {
		return err
	}
	n.cancel()
	return nil
}
