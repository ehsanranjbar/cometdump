package cometdump

import (
	"fmt"

	cmtmath "github.com/cometbft/cometbft/v2/libs/math"
	rpccore "github.com/cometbft/cometbft/v2/rpc/core"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
	rpcserver "github.com/cometbft/cometbft/v2/rpc/jsonrpc/server"
	rpctypes "github.com/cometbft/cometbft/v2/rpc/jsonrpc/types"
	cmtypes "github.com/cometbft/cometbft/v2/types"
)

// RPCServer is a struct that provides methods to handle RPC requests for the cometdump store.
// It contains a reference to the Store, which is used to access blockchain data.
type RPCServer struct {
	store *Store
}

// NewRPCServer creates a new RPC server for the cometdump store.
func NewRPCServer(store *Store) *RPCServer {
	return &RPCServer{store: store}
}

// GetRoutes returns a map of RPC function names to their corresponding RPC functions.
// Each function is registered with its expected parameters and caching behavior.
// The functions handle requests for block data, block results, blockchain information, and headers.
func (server *RPCServer) GetRoutes() rpccore.RoutesMap {
	return map[string]*rpcserver.RPCFunc{
		"block":         rpcserver.NewRPCFunc(server.Block, "height", rpcserver.Cacheable()),
		"block_results": rpcserver.NewRPCFunc(server.BlockResults, "height", rpcserver.Cacheable()),
		"blockchain":    rpcserver.NewRPCFunc(server.BlockchainInfo, "minHeight,maxHeight", rpcserver.Cacheable()),
		"header":        rpcserver.NewRPCFunc(server.Header, "height", rpcserver.Cacheable()),
	}
}

// BlockchainInfo gets block headers for minHeight <= height <= maxHeight.
// More: https://docs.cometbft.com/main/rpc/#/Info/blockchain
func (server *RPCServer) BlockchainInfo(_ *rpctypes.Context, minHeight, maxHeight int64) (*coretypes.ResultBlockchainInfo, error) {
	const limit int64 = 20
	var err error
	minHeight, maxHeight, err = filterMinMax(
		server.store.chunks.startHeight(),
		server.store.chunks.endHeight(),
		minHeight,
		maxHeight,
		limit,
	)
	if err != nil {
		return nil, err
	}

	recs, err := server.store.BlocksInRange(minHeight, maxHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to get blocks in range %d-%d: %w", minHeight, maxHeight, err)
	}
	var metas []*cmtypes.BlockMeta
	for _, br := range recs {
		if br == nil {
			continue
		}

		partSet, err := br.Block.MakePartSet(br.BlockID.PartSetHeader.Total)
		if err != nil {
			return nil, fmt.Errorf("failed to make part set for block %d: %w", br.Block.Height, err)
		}
		metas = append(metas, cmtypes.NewBlockMeta(br.Block, partSet))
	}
	return &coretypes.ResultBlockchainInfo{
		LastHeight: server.store.chunks.endHeight(),
		BlockMetas: metas,
	}, nil
}

func filterMinMax(base, height, min, max, limit int64) (minHeight, maxHeight int64, err error) {
	// filter negatives
	if min < 0 || max < 0 {
		return min, max, rpccore.ErrNegativeHeight
	}

	// adjust for default values
	if min == 0 {
		min = 1
	}
	if max == 0 {
		max = height
	}

	// limit max to the height
	max = cmtmath.MinInt64(height, max)

	// limit min to the base
	min = cmtmath.MaxInt64(base, min)

	// limit min to within `limit` of max
	// so the total number of blocks returned will be `limit`
	min = cmtmath.MaxInt64(min, max-limit+1)

	if min > max {
		return min, max, rpccore.ErrHeightMinGTMax{Min: min, Max: max}
	}
	return min, max, nil
}

// Header gets block header at a given height.
// If no height is provided, it will fetch the latest header.
// More: https://docs.cometbft.com/main/rpc/#/Info/header
func (server *RPCServer) Header(_ *rpctypes.Context, heightPtr *int64) (*coretypes.ResultHeader, error) {
	height, err := server.getHeight(heightPtr)
	if err != nil {
		return nil, err
	}

	br, err := server.store.BlockAt(height)
	if err != nil {
		return nil, err
	}
	return &coretypes.ResultHeader{
		Header: &br.Block.Header,
	}, nil
}

func (server *RPCServer) getHeight(heightPtr *int64) (int64, error) {
	latestHeight := server.store.chunks.endHeight()
	if heightPtr != nil {
		height := *heightPtr
		if height <= 0 {
			return 0, fmt.Errorf("height must be greater than 0, but got %d", height)
		}
		if height > latestHeight {
			return 0, fmt.Errorf("height %d must be less than or equal to the current blockchain height %d",
				height, latestHeight)
		}
		base := server.store.chunks.startHeight()
		if height < base {
			return 0, fmt.Errorf("height %d is not available, lowest height is %d",
				height, base)
		}
		return height, nil
	}
	return latestHeight, nil
}

// Block gets block at a given height.
// If no height is provided, it will fetch the latest block.
// More: https://docs.cometbft.com/main/rpc/#/Info/block
func (server *RPCServer) Block(_ *rpctypes.Context, heightPtr *int64) (*coretypes.ResultBlock, error) {
	height, err := server.getHeight(heightPtr)
	if err != nil {
		return nil, err
	}

	br, err := server.store.BlockAt(height)
	if err != nil {
		return nil, err
	}
	return br.ToResultBlock(), nil
}

// BlockResults gets ABCIResults at a given height.
// If no height is provided, it will fetch results for the latest block.
//
// Results are for the height of the block containing the txs.
// Thus response.results.deliver_tx[5] is the results of executing
// getBlock(h).Txs[5]
// More: https://docs.cometbft.com/main/rpc/#/Info/block_results
func (server *RPCServer) BlockResults(_ *rpctypes.Context, heightPtr *int64) (*coretypes.ResultBlockResults, error) {
	height, err := server.getHeight(heightPtr)
	if err != nil {
		return nil, err
	}

	br, err := server.store.BlockAt(height)
	if err != nil {
		return nil, err
	}
	return br.ToResultBlockResults(), nil
}
