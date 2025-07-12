package chainutil

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/Masterminds/semver"
	rpcclient "github.com/cometbft/cometbft/v2/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
)

const discoverTimeout = 5 * time.Second

// DiscoverNodes fetches some basic information about the nodes
// from the given remotes. If `expand` is true, it will also expand the
// remotes by querying the the chain network info.
func DiscoverNodes(
	ctx context.Context,
	remotes []string,
	expand bool,
	logger *slog.Logger,
) (Nodes, error) {
	if len(remotes) == 0 {
		return nil, fmt.Errorf("no remotes provided for discovery")
	}

	if expand {
		var err error
		remotes, err = expandRemotes(ctx, remotes, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to expand remotes: %w", err)
		}
	}

	var wg sync.WaitGroup
	nodesChan := make(chan Node, len(remotes))
	for _, remote := range remotes {
		wg.Add(1)
		go discoverNode(ctx, remote, &wg, nodesChan, logger)
	}
	wg.Wait()
	close(nodesChan)
	var nodes Nodes
	for node := range nodesChan {
		logger.Debug(
			"Discovered node",
			"id", node.Id,
			"remote", node.RemoteURL,
			"app_version", node.AppVersion,
			"earliest_height", node.EarliestHeight,
			"latest_height", node.LatestHeight,
		)

		nodes = append(nodes, node)
	}
	return nodes, nil
}

func expandRemotes(ctx context.Context, remotes []string, logger *slog.Logger) ([]string, error) {
	i := -1
	logger.Info("Expanding remotes list")
	for {
		i++
		if i >= len(remotes) {
			return nil, fmt.Errorf("no functional remote found to discover nodes")
		}
		remote := remotes[i]

		rpc, err := rpcclient.New(remote)
		if err != nil {
			logger.Warn("Failed to create RPC client", "remote", remote, "error", err)
			continue
		}

		netInfo, err := rpc.NetInfo(ctx)
		if err != nil {
			logger.Warn("Failed to get net info", "remote", remote, "error", err)
			continue
		}
		logger.Info("Available network peers", "remote", remote, "count", len(netInfo.Peers))

		for _, peer := range netInfo.Peers {
			nodeRPC, err := getNodeRPCAddress(peer)
			if err != nil {
				logger.Debug("Skipping node", "address", peer.NodeInfo.ListenAddr, "error", err)
				continue
			}
			remotes = append(remotes, nodeRPC)
		}
		break
	}

	return remotes, nil
}

func getNodeRPCAddress(peer coretypes.Peer) (string, error) {
	nodeHost, _, err := net.SplitHostPort(peer.NodeInfo.ListenAddr)
	if err != nil {
		return "", err
	}
	if nodeIP := net.ParseIP(nodeHost); nodeIP != nil && nodeIP.IsPrivate() {
		return "", fmt.Errorf("node %s has a private IP address", nodeHost)
	}
	rpcAddr, err := url.Parse(peer.NodeInfo.Other.RPCAddress)
	if err != nil {
		return "", fmt.Errorf("failed to parse node RPC address: %w", err)
	}

	// The remote RPC address is constructed from the listen address and the RPC port
	return fmt.Sprintf("http://%s:%s/", nodeHost, rpcAddr.Port()), nil
}

func discoverNode(
	ctx context.Context,
	remote string,
	wg *sync.WaitGroup,
	resultChan chan<- Node,
	logger *slog.Logger,
) {
	ctx, cancel := context.WithTimeout(ctx, discoverTimeout)
	defer cancel()
	defer wg.Done()

	rpc, err := rpcclient.New(remote)
	if err != nil {
		logger.Warn("Skipping node", "remote", remote, "error", err)
		return
	}

	abciInfo, err := rpc.ABCIInfo(ctx)
	if err != nil {
		logger.Warn("Skipping node", "remote", remote, "error", fmt.Errorf("failed to get ABCI info: %w", err))
		return
	}

	status, err := rpc.Status(ctx)
	if err != nil {
		logger.Warn("Skipping node", "remote", remote, "error", fmt.Errorf("failed to get status: %w", err))
		return
	}

	appVersion, err := semver.NewVersion(abciInfo.Response.Version)
	if err != nil {
		logger.Warn(
			"Skipping node",
			"remote", remote,
			"version", abciInfo.Response.Version,
			"error", fmt.Errorf("failed to parse node app version: %w", err),
		)
		return
	}

	resultChan <- Node{
		Id:             status.NodeInfo.ID(),
		RemoteURL:      remote,
		AppVersion:     appVersion,
		EarliestHeight: status.SyncInfo.EarliestBlockHeight,
		LatestHeight:   status.SyncInfo.LatestBlockHeight,
		RPC:            rpc,
	}
}
