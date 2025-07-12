package chainutil

import (
	"context"
	"fmt"

	rpcclient "github.com/cometbft/cometbft/v2/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
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
	for i := 0; i < len(responses); i += 2 {
		blocks = append(blocks, responses[i].(*coretypes.ResultBlock))
	}
	var blockResults []*coretypes.ResultBlockResults
	for i := 1; i < len(responses); i += 2 {
		blockResults = append(blockResults, responses[i].(*coretypes.ResultBlockResults))
	}

	return blocks, blockResults, nil
}
