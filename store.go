package cometdump

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/Masterminds/semver"
	"github.com/andybalholm/brotli"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
	"github.com/ehsanranjbar/cometdump/internal/chainutil"
	"github.com/ehsanranjbar/cometdump/internal/jobqueue"
	"github.com/vmihailenco/msgpack/v5"
)

// Store represents a directory where blocks are stored as chunks of data.
type Store struct {
	mu     sync.RWMutex
	dir    string
	chunks Chunks
}

// Open initializes a new Store at the specified path
// or returns an existing one. If the directory does not exist, it will be created.
// It also acquires a lock on the directory to prevent concurrent access.
func Open(path string) (*Store, error) {
	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(path, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", path, err)
		}
	} else if err != nil {
		return nil, err
	} else {
		if !stat.IsDir() {
			return nil, fmt.Errorf("path %s is not a directory", path)
		}
	}

	chunks, err := readChunksList(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks list: %w", err)
	}

	s := &Store{
		dir:    path,
		chunks: chunks,
	}
	return s, nil
}

// SyncConfig defines config for the Sync method.
type SyncConfig struct {
	// Remotes is a list of node RPC endpoints to connect to.
	Remotes []string
	// ExpandRemotes indicates whether to expand the remotes by querying the chain network info.
	ExpandRemotes bool
	// VersionConstraint is a semantic version constraint for app versions of the nodes.
	VersionConstraint *semver.Constraints
	// UseLatestVersion indicates whether to use the nodes with the latest application version.
	UseLatestVersion bool
	// ChunkSize is the number of blocks to put in each file/chunk.
	ChunkSize int
	// Height is the height up to which store should be synced.
	// If 0, it will fetch up to the latest block height.
	Height int64
	// FetchSize is the number of blocks to fetch in each RPC call.
	FetchSize int
	// NumWorkers is the number of concurrent workers to fetch blocks.
	NumWorkers int
	// OutputChan is a channel that can be optionally used to receive the BlockRecords as they are stored.
	OutputChan chan *BlockRecord
	// Logger is the logger to use for logging during the sync process.
	Logger *slog.Logger
}

// DefaultSyncConfig provides default options for the Sync method.
func DefaultSyncConfig(remotes ...string) SyncConfig {
	if len(remotes) == 0 {
		panic("at least one remote must be provided")
	}

	return SyncConfig{
		Remotes:          remotes,
		ExpandRemotes:    len(remotes) < 2,
		UseLatestVersion: true,
		ChunkSize:        10000,
		Height:           0,
		FetchSize:        100,
		NumWorkers:       4,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}
}

// WithExpandRemotes sets whether to expand the remotes by querying the chain network info.
func (c SyncConfig) WithExpandRemotes(expand bool) SyncConfig {
	c.ExpandRemotes = expand
	return c
}

// WithVersionConstraint sets a version constraint for the remotes.
func (c SyncConfig) WithVersionConstraint(constraint string) SyncConfig {
	if constraint == "" {
		c.VersionConstraint = nil
		return c
	}
	constraints, err := semver.NewConstraint(constraint)
	if err != nil {
		panic(fmt.Sprintf("invalid version constraint: %s", constraint))
	}
	c.VersionConstraint = constraints
	return c
}

// WithUseLatestVersion indicates whether to use the latest version of the remote.
func (c SyncConfig) WithUseLatestVersion(useLatest bool) SyncConfig {
	c.UseLatestVersion = useLatest
	return c
}

// WithHeight sets the height up to which blocks should be fetched.
func (c SyncConfig) WithHeight(height int64) SyncConfig {
	if height < 1 {
		panic("height must be a positive integer")
	}
	c.Height = height
	return c
}

// WithLogger sets the logger for the sync operation.
func (conf SyncConfig) WithLogger(logger *slog.Logger) SyncConfig {
	if logger == nil {
		panic("logger cannot be nil")
	}
	conf.Logger = logger
	return conf
}

// Sync fetches blocks up until the latest block height (or a specific height if provided)
// and stores them in the store directory.
func (s *Store) Sync(ctx context.Context, conf SyncConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger := conf.Logger

	logger.Info("Discovering nodes", "remotes", conf.Remotes)
	nodes, err := chainutil.DiscoverNodes(ctx, conf.Remotes, conf.ExpandRemotes, logger)
	if err != nil {
		return fmt.Errorf("failed to discover peers: %w", err)
	}
	if conf.VersionConstraint != nil {
		nodes = nodes.ConstrainByVersion(*conf.VersionConstraint)
	}
	if conf.UseLatestVersion {
		nodes = nodes.LatestVersion()
	}
	logger.Info("Functional remotes", "count", len(nodes))
	if len(nodes) == 0 {
		return fmt.Errorf("no functional remotes available after discovery and filtering")
	}

	targetHeight := conf.Height
	if targetHeight < 1 {
		targetHeight = nodes.LatestAvailableHeight()
	}
	lastStoredHeight := s.lastStoredHeight()
	if targetHeight <= lastStoredHeight {
		logger.Info("Store is already synced up to the given height, nothing to do",
			"chain_height", targetHeight,
			"last_stored_height", lastStoredHeight,
		)
		return nil
	}

	queueSize := conf.ChunkSize/conf.FetchSize + 1
	logger.Info("Spawning fetch workers",
		"num_workers", conf.NumWorkers, "queue_size", queueSize)
	jobQueue, resultsChan, cleanup := jobqueue.Launch(
		ctx,
		conf.NumWorkers,
		queueSize,
		doFetch(nodes))
	defer cleanup()

	currentHeight := lastStoredHeight + 1
	logger.Info("Syncing store", "from", currentHeight, "to", targetHeight)
	for currentHeight < targetHeight {
		queuedBlocks := queueFetchJobs(int64(conf.ChunkSize), int64(conf.FetchSize),
			currentHeight, targetHeight, jobQueue)
		blocks, blockResults, err := collectFetchResults(jobQueue, resultsChan, queuedBlocks, logger)
		if err != nil {
			return fmt.Errorf("failed to collect fetch results: %w", err)
		}
		records := blockRecordsFromRPCResults(blocks, blockResults)
		pushToChannel(records, conf.OutputChan)
		err = s.storeRecords(records, int64(conf.ChunkSize), logger)
		if err != nil {
			return fmt.Errorf("failed to store records: %w", err)
		}
		currentHeight += queuedBlocks
	}

	if conf.OutputChan != nil {
		close(conf.OutputChan)
	}

	return nil
}

func (s *Store) lastStoredHeight() int64 {
	return s.chunks.EndHeight()
}

type fetchJob struct {
	startHeight int64
	endHeight   int64
	retries     int
}

type fetchResult struct {
	blocks       []*coretypes.ResultBlock
	blockResults []*coretypes.ResultBlockResults
}

// NoNodesAvailableError is returned when no nodes are available for the given height range.
type NoNodesAvailableError struct {
	startHeight int64
	endHeight   int64
}

// Range returns the height range for which no nodes are available.
func (e *NoNodesAvailableError) Range() (int64, int64) {
	return e.startHeight, e.endHeight
}

func (e *NoNodesAvailableError) Error() string {
	return fmt.Sprintf("no nodes available for height range %d-%d", e.startHeight, e.endHeight)
}

// fetchBlocksError is returned when there is an error fetching blocks from the nodes.
type fetchBlocksError struct {
	remote string
	err    error
}

func (e *fetchBlocksError) Error() string {
	return fmt.Sprintf("failed to fetch blocks from %s: %v", e.remote, e.err)
}

func doFetch(nodes chainutil.Nodes) jobqueue.DoFunc[fetchJob, fetchResult] {
	return func(ctx context.Context, job *fetchJob) (*fetchResult, error) {
		nodes := nodes.ByHeightRange(job.startHeight, job.endHeight)
		if len(nodes) == 0 {
			return nil, &NoNodesAvailableError{
				startHeight: job.startHeight,
				endHeight:   job.endHeight,
			}
		}
		client := nodes.PickRandom().RPC
		blocks, blockResults, err := chainutil.FetchBlocks(ctx, client, job.startHeight, job.endHeight)
		if err != nil {
			return nil, &fetchBlocksError{
				remote: client.Remote(),
				err:    err,
			}
		}
		return &fetchResult{
			blocks:       blocks,
			blockResults: blockResults,
		}, nil
	}
}

func queueFetchJobs(
	chunkSize int64,
	fetchSize int64,
	currentHeight,
	targetHeight int64,
	jobQueue chan<- *fetchJob,
) int64 {
	var endHeight int64
	toHeight := ((currentHeight / chunkSize) + 1) * chunkSize
	for h := currentHeight; h < toHeight; h += fetchSize {
		startHeight := h
		endHeight = h + fetchSize - 1
		if endHeight > targetHeight {
			endHeight = targetHeight
		}

		job := &fetchJob{
			startHeight: startHeight,
			endHeight:   endHeight,
		}
		jobQueue <- job
	}

	return endHeight - currentHeight + 1
}

func collectFetchResults(
	jobQueue chan<- *fetchJob,
	resultsChan <-chan jobqueue.JobResult[fetchJob, fetchResult],
	resultsLimit int64,
	logger *slog.Logger,
) (
	blocks []*coretypes.ResultBlock,
	blockResults []*coretypes.ResultBlockResults,
	err error,
) {
	for jr := range resultsChan {
		if jr.Err != nil {
			switch e := jr.Err.(type) {
			case *NoNodesAvailableError:
				return nil, nil, e
			case *fetchBlocksError:
				logger.Warn("Failed to fetch blocks",
					"range", fmt.Sprintf("%d-%d", jr.Job.startHeight, jr.Job.endHeight),
					"retries", jr.Job.retries,
					"error", e.err,
					"remote", e.remote,
				)
				jr.Job.retries++
				jobQueue <- jr.Job // Requeue the job for retry
				continue
			}
		}
		blocks = append(blocks, jr.Result.blocks...)
		blockResults = append(blockResults, jr.Result.blockResults...)

		if len(blocks) >= int(resultsLimit) {
			break
		}
	}

	return blocks, blockResults, nil
}

func blockRecordsFromRPCResults(
	blocks []*coretypes.ResultBlock,
	blockResults []*coretypes.ResultBlockResults,
) []*BlockRecord {
	if len(blocks) != len(blockResults) {
		panic("blocks and block results must have the same length")
	}

	sortBlocks(blocks)
	sortBlockResults(blockResults)

	records := make([]*BlockRecord, len(blocks))
	for i, block := range blocks {
		records[i] = BlockRecordFromRPCResults(block, blockResults[i])
	}
	return records
}

func sortBlocks(blocks []*coretypes.ResultBlock) {
	slices.SortFunc(blocks, func(a, b *coretypes.ResultBlock) int {
		if a.Block.Height < b.Block.Height {
			return -1
		} else if a.Block.Height > b.Block.Height {
			return 1
		}
		return 0
	})
}

func sortBlockResults(blockResults []*coretypes.ResultBlockResults) {
	slices.SortFunc(blockResults, func(a, b *coretypes.ResultBlockResults) int {
		if a.Height < b.Height {
			return -1
		} else if a.Height > b.Height {
			return 1
		}
		return 0
	})
}

func pushToChannel[T any](records []T, outputChan chan<- T) {
	if outputChan == nil {
		return
	}
	for _, rec := range records {
		outputChan <- rec
	}
}

func (s *Store) storeRecords(recs []*BlockRecord, chunkSize int64, logger *slog.Logger) error {
	startHeight := recs[0].Block.Height
	endHeight := recs[len(recs)-1].Block.Height
	file, err := s.createChunkFile(startHeight, endHeight)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %w", err)
	}
	defer file.Close()

	wr := brotli.NewWriter(file)
	enc := msgpack.GetEncoder()
	defer msgpack.PutEncoder(enc)
	enc.Reset(wr)
	enc.UseCompactInts(true) // Use compact integers for better compression

	enc.EncodeArrayLen(len(recs)) // Each block and its results
	for _, rec := range recs {
		if err := enc.Encode(rec); err != nil {
			return fmt.Errorf("failed to encode BlockRecord: %w", err)
		}
	}
	err = wr.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush brotli writer: %w", err)
	}
	err = wr.Close()
	if err != nil {
		return fmt.Errorf("failed to close brotli writer: %w", err)
	}

	chunk := Chunk{
		FromHeight: startHeight,
		ToHeight:   endHeight,
	}
	s.insertChunk(chunk)

	logger.Info("Stored chunk", "filename", chunk.filename())

	return nil
}

func (s *Store) createChunkFile(startHeight, endHeight int64) (*os.File, error) {
	filename := fmt.Sprintf("%012d-%012d.msgpack.br", startHeight, endHeight)
	filePath := filepath.Join(s.dir, filename)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	return file, nil
}

func (s *Store) insertChunk(chunk Chunk) {
	idx, _ := slices.BinarySearchFunc(s.chunks, chunk, func(e, t Chunk) int {
		if e.FromHeight < t.FromHeight {
			return -1
		} else if e.FromHeight > t.FromHeight {
			return 1
		}
		return 0
	})
	s.chunks = slices.Insert(s.chunks, idx, chunk)
}

// Blocks returns an iterator which iterates over all BlockRecords in the store with order
func (s *Store) Blocks() iter.Seq2[*BlockRecord, error] {
	return func(yield func(*BlockRecord, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		for _, chunk := range s.chunks {
			iter, err := s.iterChunk(chunk, 0)
			if err != nil {
				err = fmt.Errorf("failed to iterate chunk %s: %w", chunk.filename(), err)
				yield(nil, err)
				return
			}

			for rec, err := range iter {
				if err != nil {
					err = fmt.Errorf("failed to read record from chunk %s: %w", chunk.filename(), err)
				}
				if !yield(rec, err) {
					return
				}
			}
		}
	}
}

func (s *Store) iterChunk(chunk Chunk, skip int) (iter.Seq2[*BlockRecord, error], error) {
	file, err := s.openChunk(chunk)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file %s: %w", chunk.filename(), err)
	}

	br := brotli.NewReader(file)
	dec := msgpack.GetDecoder()
	dec.Reset(br)

	l, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, fmt.Errorf("failed to decode array length: %w", err)
	}

	for i := 0; i < skip; i++ {
		if err := dec.Skip(); err != nil {
			return nil, fmt.Errorf("failed to skip record %d: %w", i, err)
		}
	}

	return func(yield func(*BlockRecord, error) bool) {
		defer file.Close()
		defer msgpack.PutDecoder(dec)

		for i := skip; i < l; i++ {
			var rec *BlockRecord
			err := dec.Decode(&rec)
			if err != nil {
				err = fmt.Errorf("failed to decode BlockRecord at index %d: %w", i, err)
			}
			if !yield(rec, err) {
				return
			}
		}
	}, nil
}

func (s *Store) openChunk(chunk Chunk) (*os.File, error) {
	filePath := filepath.Join(s.dir, chunk.filename())
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	return file, nil
}

// BlockAt retrieves a block record by its height from the store.
func (s *Store) BlockAt(height int64) (*BlockRecord, error) {
	if height < 1 {
		return nil, fmt.Errorf("height must be a positive integer, got %d", height)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	chunk, ok := s.chunks.FindForHeight(height)
	if !ok {
		return nil, fmt.Errorf("block at height %d not found", height)
	}
	skip := int(height - chunk.FromHeight)
	iter, err := s.iterChunk(chunk, skip)
	if err != nil {
		return nil, fmt.Errorf("failed to iterate chunk %s: %w", chunk.filename(), err)
	}
	for rec, err := range iter {
		if err != nil {
			return nil, fmt.Errorf("failed to get block at height %d: %w", height, err)
		}
		return rec, nil
	}

	return nil, fmt.Errorf("block at height %d not found", height)
}
