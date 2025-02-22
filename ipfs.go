package main

import (
	"context"
	"fmt"
	"os"
	"strings"
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
}

// Creates and returns a new IPFS node
func createNode(ctx context.Context, repoPath string) (*IPFSNode, error) {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(ctx)

	// Create the repo path if it doesn't exist
	if err := os.MkdirAll(repoPath, 0755); err != nil {
		cancel()
		return nil, err
	}

	// Check if repo needs to be initialized
	if !fsrepo.IsInitialized(repoPath) {
		cfg, err := config.Init(os.Stdout, 2048)
		if err != nil {
			cancel()
			return nil, err
		}

		// Configure the node
		cfg.Addresses.Swarm = []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/tcp/4002/ws",
		}
		cfg.Addresses.API = []string{"/ip4/127.0.0.1/tcp/5001"}
		cfg.Addresses.Gateway = []string{"/ip4/127.0.0.1/tcp/8080"}

		// Initialize the repo
		if err := fsrepo.Init(repoPath, cfg); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to init repo: %s", err)
		}
	}

	// Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		cancel()
		return nil, err
	}

	// Load plugins if any
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

	// Create the node
	nodeOptions := &node.BuildCfg{
		Online: true,
		Repo:   repo,
		ExtraOpts: map[string]bool{
			"pubsub": true,
			"ipnsps": true,
			"mplex":  true,
		},
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		cancel()
		return nil, err
	}

	// Create the CoreAPI
	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		cancel()
		return nil, err
	}

	// Connect to bootstrap nodes
	if err := connectToBootstrapNodes(ctx, node); err != nil {
		logger.Printf("Warning: failed to connect to bootstrap nodes: %s", err)
	}

	return &IPFSNode{
		node:   node,
		api:    api,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Connects to the default IPFS bootstrap nodes
func connectToBootstrapNodes(ctx context.Context, node *core.IpfsNode) error {
	var bootstrapNodes = []string{
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	}

	var wg sync.WaitGroup
	for _, addr := range bootstrapNodes {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			targetAddr, err := ma.NewMultiaddr(address)
			if err != nil {
				logger.Printf("Failed to parse bootstrap address: %s", err)
				return
			}

			targetInfo, err := peer.AddrInfoFromP2pAddr(targetAddr)
			if err != nil {
				logger.Printf("Failed to create peer info: %s", err)
				return
			}

			if err := node.PeerHost.Connect(ctx, *targetInfo); err != nil {
				logger.Printf("Failed to connect to bootstrap node: %s", err)
			}
		}(addr)
	}

	wg.Wait()
	return nil
}

// Adds a file or directory to IPFS and returns its CID
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

	// Add the file to IPFS
	ipfsPath, err := n.api.Unixfs().Add(n.ctx, f)
	if err != nil {
		return "", err
	}

	// Pin the file
	if err := n.api.Pin().Add(n.ctx, ipfsPath); err != nil {
		return "", err
	}

	// Extract CID from path string (format: /ipfs/<cid>)
	pathStr := ipfsPath.String()
	parts := strings.Split(pathStr, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid IPFS path: %s", pathStr)
	}
	return parts[2], nil
}

// Stops the IPFS node
func (n *IPFSNode) Close() error {
	if err := n.node.Close(); err != nil {
		return err
	}
	n.cancel()
	return nil
}
