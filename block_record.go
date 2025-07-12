package cometdump

import (
	"fmt"

	cmtproto "github.com/cometbft/cometbft/api/cometbft/types/v2"
	abcitypes "github.com/cometbft/cometbft/v2/abci/types"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
	cmtypes "github.com/cometbft/cometbft/v2/types"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// BlockRecord is a structure that holds the details of a block, including its ID, transactions, events, and other related data.
// It is used to encode and decode block data in a msgpack format.
type BlockRecord struct {
	BlockID               cmtypes.BlockID
	Block                 *cmtypes.Block
	TxResults             []*abcitypes.ExecTxResult
	FinalizeBlockEvents   []abcitypes.Event
	ValidatorUpdates      []abcitypes.ValidatorUpdate
	ConsensusParamUpdates *cmtproto.ConsensusParams
	BlockResultsAppHash   []byte
}

// EncodeMsgpack encodes the Block into a msgpack format.
func (b *BlockRecord) EncodeMsgpack(enc *msgpack.Encoder) error {
	if b == nil {
		return fmt.Errorf("cannot encode nil BlockRecord")
	}

	if err := enc.EncodeArrayLen(7); err != nil {
		return fmt.Errorf("failed to encode array length")
	}

	if err := enc.Encode(b.BlockID); err != nil {
		return fmt.Errorf("failed to encode BlockID: %w", err)
	}

	var bp *cmtproto.Block
	if b.Block != nil {
		var err error
		bp, err = b.Block.ToProto()
		if err != nil {
			return fmt.Errorf("failed to convert Block to proto: %w", err)
		}
	}
	if err := enc.Encode(bp); err != nil {
		return fmt.Errorf("failed to encode Block: %w", err)
	}

	if err := enc.Encode(b.TxResults); err != nil {
		return fmt.Errorf("failed to encode TxResults: %w", err)
	}

	if err := enc.Encode(b.FinalizeBlockEvents); err != nil {
		return fmt.Errorf("failed to encode FinalizeBlockEvents: %w", err)
	}

	if err := enc.Encode(b.ValidatorUpdates); err != nil {
		return fmt.Errorf("failed to encode ValidatorUpdates: %w", err)
	}

	if err := enc.Encode(b.ConsensusParamUpdates); err != nil {
		return fmt.Errorf("failed to encode ConsensusParamUpdates: %w", err)
	}

	if err := enc.EncodeBytes(b.BlockResultsAppHash); err != nil {
		return fmt.Errorf("failed to encode BlockResultsAppHash: %w", err)
	}

	return nil
}

// DecodeMsgpack decodes the Block from a msgpack format.
func (b *BlockRecord) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return fmt.Errorf("failed to decode array length: %w", err)
	}

	var (
		bidp cmtproto.BlockID
		bp   *cmtproto.Block
	)
	if err := dec.DecodeMulti(
		&bidp,
		&bp,
		&b.TxResults,
		&b.FinalizeBlockEvents,
		&b.ValidatorUpdates,
		&b.ConsensusParamUpdates,
		&b.BlockResultsAppHash,
	); err != nil {
		return fmt.Errorf("failed to decode Block fields: %w", err)
	}

	bid, err := cmtypes.BlockIDFromProto(&bidp)
	if err != nil {
		return fmt.Errorf("failed to convert BlockID from proto: %w", err)
	}
	b.BlockID = *bid

	if bp != nil {
		if b.Block, err = cmtypes.BlockFromProto(bp); err != nil {
			return fmt.Errorf("failed to convert Block from proto: %w", err)
		}
	}

	return nil
}

// BlockRecordFromRPCResults creates a BlockRecord from the given ResultBlock and ResultBlockResults.
func BlockRecordFromRPCResults(block *coretypes.ResultBlock, blockResults *coretypes.ResultBlockResults) *BlockRecord {
	if block == nil || blockResults == nil {
		return nil
	}

	return &BlockRecord{
		BlockID:               block.BlockID,
		Block:                 block.Block,
		TxResults:             blockResults.TxResults,
		FinalizeBlockEvents:   blockResults.FinalizeBlockEvents,
		ValidatorUpdates:      blockResults.ValidatorUpdates,
		ConsensusParamUpdates: blockResults.ConsensusParamUpdates,
		BlockResultsAppHash:   blockResults.AppHash,
	}
}

// ToResultBlock converts the resultBlockProto back to a ResultBlock.
func (b *BlockRecord) ToResultBlock() (*coretypes.ResultBlock, error) {
	if b == nil {
		return nil, nil
	}

	return &coretypes.ResultBlock{
		BlockID: b.BlockID,
		Block:   b.Block,
	}, nil
}

// ToResultBlockResults converts the Block to a ResultBlockResults.
func (b *BlockRecord) ToResultBlockResults() (*coretypes.ResultBlockResults, error) {
	if b == nil {
		return nil, nil
	}
	return &coretypes.ResultBlockResults{
		Height:                b.Block.Height,
		TxResults:             b.TxResults,
		FinalizeBlockEvents:   b.FinalizeBlockEvents,
		ValidatorUpdates:      b.ValidatorUpdates,
		ConsensusParamUpdates: b.ConsensusParamUpdates,
		AppHash:               b.BlockResultsAppHash,
	}, nil
}
