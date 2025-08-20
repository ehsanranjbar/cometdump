package chainutil

import (
	"github.com/Masterminds/semver"
	rpcclient "github.com/cometbft/cometbft/v2/rpc/client/http"
	"golang.org/x/exp/rand"
)

// Nodes is a slice of Node, representing a collection of discovered nodes.
type Nodes []Node

// ConstrainByVersion filters the discovered nodes by the given semver constraints.
func (nodes Nodes) ConstrainByVersion(c semver.Constraints) Nodes {
	var constrainedNodes Nodes
	for _, node := range nodes {
		if node.AppVersion != nil && c.Check(node.AppVersion) {
			constrainedNodes = append(constrainedNodes, node)
		}
	}
	return constrainedNodes
}

// LatestVersion returns the nodes with the latest application version.
func (nodes Nodes) LatestVersion() Nodes {
	latestVersion := semver.MustParse("0.0.0")
	candidates := make([]Node, 0)
	for _, node := range nodes {
		if node.AppVersion.GreaterThan(latestVersion) {
			latestVersion = node.AppVersion
			candidates = candidates[:0] // Clear the slice to only keep the latest version
		}
		if node.AppVersion.Equal(latestVersion) {
			candidates = append(candidates, node)
		}
	}
	return candidates
}

// ByHeightRange filters the discovered nodes by the given height range.
func (nodes Nodes) ByHeightRange(minHeight, maxHeight int64) Nodes {
	var filteredNodes Nodes
	for _, node := range nodes {
		if node.EarliestHeight <= maxHeight && node.LatestHeight >= minHeight {
			filteredNodes = append(filteredNodes, node)
		}
	}
	return filteredNodes
}

// EarliestAvailableHeight returns the minimum earliest height among the discovered nodes.
func (nodes Nodes) EarliestAvailableHeight() int64 {
	if len(nodes) == 0 {
		return 0
	}
	minHeight := nodes[0].EarliestHeight
	for _, node := range nodes {
		if node.EarliestHeight < minHeight {
			minHeight = node.EarliestHeight
		}
	}
	return minHeight
}

// LatestAvailableHeight returns the maximum latest height among the discovered nodes.
func (nodes Nodes) LatestAvailableHeight() int64 {
	if len(nodes) == 0 {
		return 0
	}
	maxHeight := nodes[0].LatestHeight
	for _, node := range nodes {
		if node.LatestHeight > maxHeight {
			maxHeight = node.LatestHeight
		}
	}
	return maxHeight
}

// PickRandom returns a random node from the discovered nodes.
// If the slice is empty, it returns nil.
func (nodes Nodes) PickRandom() Node {
	if len(nodes) == 0 {
		return Node{}
	}

	return nodes[rand.Intn(len(nodes))]
}

// Node represents a cometbft node with its remote address,
// application version, and base height.
type Node struct {
	Id             string
	RemoteURL      string
	AppVersion     *semver.Version
	EarliestHeight int64
	LatestHeight   int64
	RPC            *rpcclient.HTTP
}
