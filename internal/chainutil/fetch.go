package chainutil

import (
	"context"
	"fmt"

	rpcclient "github.com/cometbft/cometbft/v2/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
	"github.com/cometbft/cometbft/v2/rpc/jsonrpc/types"
)

// FetchBlocks fetches blocks and their results from the CometBFT RPC server.
func FetchBlocks(ctx context.Context, rpc *rpcclient.HTTP, fromHeight, toHeight int64) (
	[]*coretypes.ResultBlock, []*coretypes.ResultBlockResults, error) {
	batch := rpc.NewBatch()
	for h := fromHeight; h <= toHeight; h++ {
		batch.Block(ctx, &h)
		batch.BlockResults(ctx, &h)
	}

	responses, err := batch.Send(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to send batch request: %w", err)
	}

	var blocks []*coretypes.ResultBlock
	var blockResults []*coretypes.ResultBlockResults
	for i := 0; i < len(responses); i += 1 {
		switch resp := responses[i].(type) {
		case *coretypes.ResultBlock:
			blocks = append(blocks, resp)
		case *coretypes.ResultBlockResults:
			blockResults = append(blockResults, resp)
		case *types.RPCError:
			return nil, nil, fmt.Errorf("error fetching block %d: %s", int(fromHeight)+i/2, resp.Error())
		}
	}

	return blocks, blockResults, nil
}
