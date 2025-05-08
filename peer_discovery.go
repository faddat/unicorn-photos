package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	standardhttp "net/http" // Alias to avoid collision with cometbft client
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	comethttp "github.com/cometbft/cometbft/rpc/client/http"
	// ctypes "github.com/cometbft/cometbft/rpc/core/types" // Not directly used here, but useful for type checking
)

const (
	defaultRPCPort      = "26657"          // Default Tendermint/CometBFT RPC port
	defaultRESTPort     = "1317"           // Default Cosmos SDK REST/API port
	discoveryTimeout    = 10 * time.Second // Timeout for individual discovery probes
	netInfoTimeout      = 15 * time.Second // Timeout for fetching /net_info
	probeRPCTimeout     = 5 * time.Second  // Timeout for checking RPC status
	probeRESTTimeout    = 5 * time.Second  // Timeout for checking REST node_info
	maxConcurrentProbes = 20               // Limit concurrent IP probes
)

var (
	// Regex to extract IP from P2P address like tcp://1.2.3.4:26656 or id@1.2.3.4:26656
	// It tries to capture IPv4 addresses.
	ipFromP2PRegex = regexp.MustCompile(`(?:tcp://)?([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+):[0-9]+`)
	// For URLs like "http://somehost:port" or "somehost:port" - Removed unused regex
	// hostFromURLRegex = regexp.MustCompile(`^(?:(?:https?|tcp)://)?([^:/]+)`)
)

type DiscoveredEndpoint struct {
	Address string // Full URL: http(s)://ip:port
	Type    string // "rpc" or "rest"
	Source  string // "config", "discovered_seed", "discovered_peer"
}

// DiscoverEndpoints attempts to find RPC and REST endpoints.
// Uses seedRPCs for /net_info, p2pSeedNodes for direct IP extraction, then probes.
func DiscoverEndpoints(ctx context.Context, chainID string, seedRPCs []string, p2pSeedNodes []string) ([]DiscoveredEndpoint, error) {
	logger.Printf("[%s] Starting endpoint discovery with %d seed RPCs and %d P2P seed nodes...", chainID, len(seedRPCs), len(p2pSeedNodes))
	var discovered []DiscoveredEndpoint
	var wg sync.WaitGroup
	mu := &sync.Mutex{}                 // Protects `discovered` slice and `checkedIPs` map
	checkedIPs := make(map[string]bool) // Tracks IPs already queued or probed to avoid redundant work

	// Channel to control concurrency of IP probing
	probeSemaphore := make(chan struct{}, maxConcurrentProbes)

	// Filter out unhealthy seed RPCs before using them
	var validSeedRPCs []string
	if len(seedRPCs) > 0 {
		logger.Printf("[%s] Validating %d registry-provided RPC endpoints before using for discovery...", chainID, len(seedRPCs))
		for _, rpc := range seedRPCs {
			checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := checkRPC(checkCtx, rpc, chainID)
			cancel()
			if err == nil {
				validSeedRPCs = append(validSeedRPCs, rpc)
				logger.Printf("[%s] Validated seed RPC endpoint for discovery: %s", chainID, rpc)
			} else {
				logger.Printf("[%s] Skipping invalid seed RPC endpoint: %s (error: %v)", chainID, rpc, err)
			}
		}
		logger.Printf("[%s] Found %d valid seed RPC endpoints out of %d provided", chainID, len(validSeedRPCs), len(seedRPCs))
		seedRPCs = validSeedRPCs
	}

	// Step 1: Start by querying /net_info from seed RPCs
	// This is the most valuable source of peers as it gives actual connected peers
	logger.Printf("[%s] Discovery: Querying seed RPCs for peers using /net_info...", chainID)
	peerIPsFromRPC := getPeerIPsFromSeedRPCs(ctx, chainID, seedRPCs)
	logger.Printf("[%s] Discovery: Found %d peer IPs from seed RPCs' /net_info", chainID, len(peerIPsFromRPC))

	if len(peerIPsFromRPC) > 0 {
		// We have peers from RPCs, queue them for probing first
		for _, ip := range peerIPsFromRPC {
			mu.Lock()
			if !checkedIPs[ip] {
				checkedIPs[ip] = true
				wg.Add(1)
				probeSemaphore <- struct{}{} // Acquire semaphore
				go func(pIP string) {
					defer wg.Done()
					probeIPForServices(ctx, mu, pIP, chainID, &discovered, "discovered_peer")
					<-probeSemaphore // Release semaphore
				}(ip)
			}
			mu.Unlock()
		}
	} else if len(p2pSeedNodes) > 0 {
		// If we couldn't get peers from RPCs, extract IPs from P2P seed nodes
		logger.Printf("[%s] Discovery: No peers from seed RPCs, extracting IPs from P2P seed nodes...", chainID)
		peerIPsFromP2PSeeds := getIPsFromP2PAddresses(p2pSeedNodes)
		logger.Printf("[%s] Discovery: Found %d IPs from P2P seed addresses", chainID, len(peerIPsFromP2PSeeds))

		// First probe these IPs directly as they might have RPC services
		for _, ip := range peerIPsFromP2PSeeds {
			mu.Lock()
			if !checkedIPs[ip] {
				checkedIPs[ip] = true
				wg.Add(1)
				probeSemaphore <- struct{}{} // Acquire semaphore
				go func(pIP string) {
					defer wg.Done()
					probeIPForServices(ctx, mu, pIP, chainID, &discovered, "discovered_seed")
					<-probeSemaphore // Release semaphore
				}(ip)
			}
			mu.Unlock()
		}

		// Wait for probing of initial P2P seeds to complete
		wg.Wait()

		// If we found RPCs from P2P seeds, use those to get more peers
		var foundRPCs []string
		mu.Lock()
		for _, ep := range discovered {
			if ep.Type == "rpc" {
				foundRPCs = append(foundRPCs, ep.Address)
			}
		}
		mu.Unlock()

		if len(foundRPCs) > 0 {
			logger.Printf("[%s] Found %d RPC endpoints from P2P seed nodes, using them to discover more peers", chainID, len(foundRPCs))
			moreIPsFromRPC := getPeerIPsFromSeedRPCs(ctx, chainID, foundRPCs)
			logger.Printf("[%s] Discovery: Found %d more peer IPs from newly discovered RPCs", chainID, len(moreIPsFromRPC))

			// Queue these additional peers for probing
			for _, ip := range moreIPsFromRPC {
				mu.Lock()
				if !checkedIPs[ip] {
					checkedIPs[ip] = true
					wg.Add(1)
					probeSemaphore <- struct{}{} // Acquire semaphore
					go func(pIP string) {
						defer wg.Done()
						probeIPForServices(ctx, mu, pIP, chainID, &discovered, "discovered_peer")
						<-probeSemaphore // Release semaphore
					}(ip)
				}
				mu.Unlock()
			}
		}
	}

	// Wait for all probing to complete
	wg.Wait()
	close(probeSemaphore)

	// Count the number of RPC and REST endpoints discovered
	rpcCount := 0
	restCount := 0
	mu.Lock()
	for _, ep := range discovered {
		if ep.Type == "rpc" {
			rpcCount++
		} else if ep.Type == "rest" {
			restCount++
		}
	}
	mu.Unlock()

	if len(discovered) == 0 {
		logger.Printf("[%s] No new viable RPC/REST endpoints were discovered.", chainID)
		return nil, fmt.Errorf("[%s] no new endpoints discovered after probing", chainID)
	}

	logger.Printf("[%s] Discovery complete: found %d endpoints (%d RPC, %d REST)",
		chainID, len(discovered), rpcCount, restCount)
	return discovered, nil
}

// getIPsFromP2PAddresses extracts IPs from a list of P2P node addresses (e.g., id@ip:port, ip:port).
func getIPsFromP2PAddresses(p2pAddresses []string) []string {
	var ips []string
	seenIPs := make(map[string]bool)

	for _, nodeAddr := range p2pAddresses {
		var host string
		// Try to parse with common P2P formats
		if strings.Contains(nodeAddr, "@") { // format id@host:port
			parts := strings.SplitN(nodeAddr, "@", 2)
			if len(parts) == 2 {
				host, _, _ = net.SplitHostPort(parts[1]) // Ignore error, host might be IP
				if host == "" {
					host = parts[1]
				} // If SplitHostPort fails, assume parts[1] is host
			}
		} else { // format host:port or tcp://host:port
			u, err := url.Parse(nodeAddr)
			if err == nil && u.Host != "" { // Handles tcp://host:port
				host = u.Hostname()
			} else { // Handles host:port
				host, _, _ = net.SplitHostPort(nodeAddr)
				if host == "" {
					host = nodeAddr
				} // If SplitHostPort fails, assume nodeAddr is host
			}
		}

		if host != "" {
			resolvedIPs, err := net.LookupIP(host) // Resolve hostname to IP(s)
			if err == nil {
				for _, ipAddr := range resolvedIPs {
					ipv4 := ipAddr.To4()
					if ipv4 != nil && !isPrivateIP(ipv4.String()) { // Prefer IPv4 and public IPs
						if !seenIPs[ipv4.String()] {
							ips = append(ips, ipv4.String())
							seenIPs[ipv4.String()] = true
						}
					}
				}
			}
		}
	}
	return ips
}

// getPeerIPsFromSeedRPCs queries /net_info from a list of known RPC endpoints.
func getPeerIPsFromSeedRPCs(ctx context.Context, chainID string, seedRPCs []string) []string {
	var peerIPs []string
	seenIPs := make(map[string]bool)
	var wg sync.WaitGroup
	mu := &sync.Mutex{}

	logger.Printf("[%s] Discovery: Attempting to query /net_info from %d seed RPC endpoints", chainID, len(seedRPCs))
	if len(seedRPCs) == 0 {
		logger.Printf("[%s] Discovery: No seed RPC endpoints provided for /net_info queries", chainID)
		return peerIPs
	}

	// Print out all the seed RPCs being used for discovery
	for i, rpc := range seedRPCs {
		logger.Printf("[%s] Discovery: Seed RPC #%d for /net_info: %s", chainID, i+1, rpc)
	}

	for _, rpcAddr := range seedRPCs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			logger.Printf("[%s] Discovery: Querying /net_info from %s", chainID, addr)
			client, err := comethttp.New(addr) // Path doesn't matter for /net_info
			if err != nil {
				logger.Printf("[%s] Discovery: Failed to create client for seed RPC %s: %v", chainID, addr, err)
				return
			}

			netInfoCtx, cancel := context.WithTimeout(ctx, netInfoTimeout)
			defer cancel()
			netInfo, err := client.NetInfo(netInfoCtx)
			if err != nil {
				logger.Printf("[%s] Discovery: Failed to get /net_info from %s: %v", chainID, addr, err)
				return
			}

			logger.Printf("[%s] Discovery: Successfully queried /net_info from %s, found %d peers",
				chainID, addr, len(netInfo.Peers))

			mu.Lock()
			defer mu.Unlock()
			for _, peer := range netInfo.Peers {
				// NodeInfo.ListenAddr is usually "tcp://ip:port"
				// RemoteIP is just the IP string.
				var potentialIPs []string
				if peer.NodeInfo.ListenAddr != "" {
					matches := ipFromP2PRegex.FindStringSubmatch(peer.NodeInfo.ListenAddr)
					if len(matches) > 1 {
						potentialIPs = append(potentialIPs, matches[1])
					}
				}
				if peer.RemoteIP != "" {
					// Validate RemoteIP looks like an IP before adding
					parsedRemoteIP := net.ParseIP(peer.RemoteIP)
					if parsedRemoteIP != nil {
						potentialIPs = append(potentialIPs, peer.RemoteIP)
					}
				}

				for _, ip := range potentialIPs {
					if !seenIPs[ip] && !isPrivateIP(ip) {
						peerIPs = append(peerIPs, ip)
						seenIPs[ip] = true
						logger.Printf("[%s] Discovery: Added peer IP %s from /net_info", chainID, ip)
					}
				}
			}
		}(rpcAddr)
	}
	wg.Wait()
	logger.Printf("[%s] Discovery: Found %d unique peer IPs from seed RPCs' /net_info.", chainID, len(peerIPs))
	return peerIPs
}

func isPrivateIP(ipStr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return true // Invalid IPs are treated as private/unusable
	}
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate()
}

// probeIPForServices checks an IP for open RPC and REST services on common ports.
func probeIPForServices(ctx context.Context, mu *sync.Mutex, ip, targetChainID string, discoveredList *[]DiscoveredEndpoint, source string) {
	schemes := []string{"http", "https"} // Prefer http, then try https. Some nodes only expose one.

	// Common alternative ports for RPC - include more common Tendermint/CometBFT ports
	rpcPorts := []string{
		defaultRPCPort, // 26657
		"26656",        // P2P port that sometimes has RPC too
		"26658",        // Alternative RPC port
		"26660",        // Sometimes used for RPC
		"1317",         // Sometimes RPC is on REST port
	}

	// Common alternative ports for REST - expanded list
	restPorts := []string{
		defaultRESTPort, // 1317
	}

	logger.Printf("[%s] Probing IP %s for RPC/REST services...", targetChainID, ip)

	// Probe RPC on different ports
	rpcFound := false
	for _, scheme := range schemes {
		if rpcFound {
			break // If we already found an RPC on this IP with one scheme, skip the other scheme
		}
		for _, port := range rpcPorts {
			rpcURL := fmt.Sprintf("%s://%s:%s", scheme, ip, port)
			probeCtx, cancel := context.WithTimeout(ctx, probeRPCTimeout)
			if err := checkRPC(probeCtx, rpcURL, targetChainID); err == nil {
				logger.Printf("[%s] SUCCESS: Found working RPC: %s (source: %s, port: %s)", targetChainID, rpcURL, source, port)
				mu.Lock()
				*discoveredList = append(*discoveredList, DiscoveredEndpoint{Address: rpcURL, Type: "rpc", Source: source})
				mu.Unlock()
				cancel()
				rpcFound = true
				// Found working RPC, no need to check other ports for this scheme
				break
			}
			cancel()
		}
	}

	// Probe REST on different ports
	restFound := false
	for _, scheme := range schemes {
		if restFound {
			break // If we already found a REST on this IP with one scheme, skip the other scheme
		}
		for _, port := range restPorts {
			restURL := fmt.Sprintf("%s://%s:%s", scheme, ip, port)
			probeCtx, cancel := context.WithTimeout(ctx, probeRESTTimeout)
			if err := checkREST(probeCtx, restURL, targetChainID); err == nil {
				logger.Printf("[%s] SUCCESS: Found working REST: %s (source: %s, port: %s)", targetChainID, restURL, source, port)
				mu.Lock()
				*discoveredList = append(*discoveredList, DiscoveredEndpoint{Address: restURL, Type: "rest", Source: source})
				mu.Unlock()
				cancel()
				restFound = true
				// Found working REST, no need to check other ports for this scheme
				break
			}
			cancel()
		}
	}

	if rpcFound || restFound {
		logger.Printf("[%s] Successfully found services on IP %s (RPC: %v, REST: %v)", targetChainID, ip, rpcFound, restFound)
	} else {
		logger.Printf("[%s] No services found on IP %s", targetChainID, ip)
	}
}

// checkRPC verifies if an RPC endpoint is alive and for the correct chain.
func checkRPC(ctx context.Context, rpcURL, targetChainID string) error {
	client, err := comethttp.New(rpcURL) // Default path for websocket, not critical for /status
	if err != nil {
		return fmt.Errorf("create client for %s failed: %w", rpcURL, err)
	}
	// Use the timeout from the passed context for the Status call
	// client.SetTimeout() is not used here as context handles it

	status, err := client.Status(ctx) // ctx already has timeout
	if err != nil {
		return fmt.Errorf("status check for %s failed: %w", rpcURL, err)
	}
	if status.NodeInfo.Network != targetChainID {
		return fmt.Errorf("chain ID mismatch at %s: expected %s, got %s", rpcURL, targetChainID, status.NodeInfo.Network)
	}
	return nil
}

// checkREST verifies if a REST endpoint is alive. Chain ID check is more complex for REST.
func checkREST(ctx context.Context, restURL, targetChainID string) error {
	// A lightweight check, e.g., /node_info or a base SDK path.
	// /cosmos/base/tendermint/v1beta1/node_info is common.
	nodeInfoURL := fmt.Sprintf("%s/cosmos/base/tendermint/v1beta1/node_info", restURL)

	httpClient := standardhttp.Client{Timeout: discoveryTimeout} // Use the context's timeout via request

	req, err := standardhttp.NewRequestWithContext(ctx, "GET", nodeInfoURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request for %s: %w", nodeInfoURL, err)
	}
	req.Header.Set("User-Agent", "UnicornPhotos-Discovery/1.0")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s failed: %w", nodeInfoURL, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Printf("Error closing response body for %s: %v", nodeInfoURL, err)
		}
	}()

	if resp.StatusCode != standardhttp.StatusOK {
		return fmt.Errorf("%s returned status %d", nodeInfoURL, resp.StatusCode)
	}

	// Optionally, parse the response to verify Chain ID if available and necessary for discovery phase.
	// However, DefaultNodeInfo.Network might be missing or unreliable in some REST setups.
	// The main snapshot logic will do more thorough checks.
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body from %s: %w", nodeInfoURL, err)
	}

	var nodeInfoResp struct {
		DefaultNodeInfo struct {
			Network string `json:"network"`
		} `json:"default_node_info"`
	}
	if err := json.Unmarshal(bodyBytes, &nodeInfoResp); err == nil {
		if nodeInfoResp.DefaultNodeInfo.Network != "" && nodeInfoResp.DefaultNodeInfo.Network != targetChainID {
			return fmt.Errorf("chain ID mismatch on REST %s: expected %s, got %s", restURL, targetChainID, nodeInfoResp.DefaultNodeInfo.Network)
		}
	} // If unmarshal fails or network field is empty, we can't confirm chain_id here.
	// Consider it a basic liveness pass for discovery.

	return nil
}
