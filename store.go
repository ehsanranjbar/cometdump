package cometdump

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver"
	"github.com/andybalholm/brotli"
	coretypes "github.com/cometbft/cometbft/v2/rpc/core/types"
	"github.com/ehsanranjbar/cometdump/internal/chainutil"
	"github.com/ehsanranjbar/cometdump/internal/jobqueue"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	barStyle = mpb.BarStyle()
)

// Store represents a directory where blocks are stored as chunks of data.
type Store struct {
	mu     sync.RWMutex
	dir    string
	chunks chunks
	logger *slog.Logger
}

// OpenOptions defines options for opening a store.
type OpenOptions struct {
	// Path to the store directory.
	Path string
	// Logger is the logger to use for logging during store operations.
	Logger *slog.Logger
}

// DefaultOpenOptions provides default options for opening a store.
func DefaultOpenOptions(path string) OpenOptions {
	return OpenOptions{
		Path: path,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
}

// WithLogger sets the logger for the store operations.
func (o OpenOptions) WithLogger(logger *slog.Logger) OpenOptions {
	if logger == nil {
		panic("logger cannot be nil")
	}
	o.Logger = logger
	return o
}

// Open initializes a new Store at the specified path
// or returns an existing one. If the directory does not exist, it will be created.
// It also acquires a lock on the directory to prevent concurrent access.
func Open(opts OpenOptions) (*Store, error) {
	logger := opts.Logger

	stat, err := os.Stat(opts.Path)
	if errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(opts.Path, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", opts.Path, err)
		}
	} else if err != nil {
		return nil, err
	} else {
		if !stat.IsDir() {
			return nil, fmt.Errorf("path %s is not a directory", opts.Path)
		}
	}

	chunks, err := readChunksList(opts.Path, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks list: %w", err)
	}

	s := &Store{
		dir:    opts.Path,
		chunks: chunks,
		logger: logger,
	}
	logger.Info("Opened store",
		"path", opts.Path,
		"height_range", fmt.Sprintf("%d-%d", chunks.startHeight(), chunks.endHeight()),
		"chunks_count", len(chunks))

	return s, nil
}

// VerifyIntegrity checks the integrity of the chunks in the store by verifying their sha256 checksums.
func (s *Store) VerifyIntegrity(checksumFile string) error {
	checksums, err := loadChecksums(checksumFile)
	if err != nil {
		return fmt.Errorf("failed to load checksums: %w", err)
	}

	var failed bool
	for _, chk := range s.chunks {
		checksum, ok := checksums[chk]
		if !ok {
			s.logger.Warn("No checksum found for chunk", "chunk", chk.filename())
			continue
		}

		chunkPath := filepath.Join(s.dir, chk.filename())
		if err := verifyFileChecksum(chunkPath, checksum); err != nil {
			s.logger.Error("Checksum verification failed for chunk", "chunk", chk.filename(), "error", err)
			failed = true
		} else {
			s.logger.Debug("Checksum verified for chunk",
				"chunk", chk.filename(),
				"checksum", hex.EncodeToString(checksum),
			)
		}
	}

	if failed {
		return fmt.Errorf("some chunks failed checksum verification")
	} else {
		s.logger.Info("All chunks passed checksum verification")
	}
	return nil
}

func loadChecksums(checksumsFile string) (map[chunk][]byte, error) {
	file, err := os.Open(checksumsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open checksums file %s: %w", checksumsFile, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	checksums := make(map[chunk][]byte)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid checksum line: %s", line)
		}
		checksum := parts[0]
		chunkName := parts[1]
		chk, err := parseChunkFilename(chunkName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse chunk filename %s: %w", chunkName, err)
		}
		checksums[chk], err = hex.DecodeString(checksum)
		if err != nil {
			return nil, fmt.Errorf("failed to decode checksum %s: %w", checksum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read checksums file %s: %w", checksumsFile, err)
	}

	return checksums, nil
}

func verifyFileChecksum(filePath string, expectedChecksum []byte) error {
	actualChecksum, err := computeFileChecksum(filePath)
	if err != nil {
		return fmt.Errorf("failed to compute checksum for file %s: %w", filePath, err)
	}
	if !bytes.Equal(actualChecksum, expectedChecksum) {
		return fmt.Errorf("checksum mismatch for file %s: expected %x, got %x", filePath, expectedChecksum, actualChecksum)
	}
	return nil
}

func computeFileChecksum(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return nil, fmt.Errorf("failed to compute checksum for file %s: %w", filePath, err)
	}
	return hasher.Sum(nil), nil
}

// SyncOptions defines options for the Sync method.
type SyncOptions struct {
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
	// TargetHeight is the height up to which store should be synced.
	// If 0, it will fetch up to the latest block height.
	TargetHeight int64
	// FetchSize is the number of blocks to fetch in each RPC call.
	FetchSize int
	// NumWorkers is the number of concurrent workers to fetch blocks.
	NumWorkers int
	// OutputChan is a channel that can be optionally used to receive the BlockRecords as they are stored.
	OutputChan chan<- *BlockRecord
	// ProgressBar is the progress bar to use for displaying sync progress.
	ProgressBar *mpb.Progress
	// Logger is the logger to use for logging during the sync process.
	Logger *slog.Logger
}

// DefaultSyncOptions provides default options for the Sync method.
func DefaultSyncOptions(remotes ...string) SyncOptions {
	if len(remotes) == 0 {
		panic("at least one remote must be provided")
	}

	return SyncOptions{
		Remotes:          remotes,
		ExpandRemotes:    len(remotes) < 2,
		UseLatestVersion: true,
		ChunkSize:        10000,
		TargetHeight:     0,
		FetchSize:        100,
		NumWorkers:       4,
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		})),
	}
}

// WithExpandRemotes sets whether to expand the remotes by querying the chain network info.
func (opts SyncOptions) WithExpandRemotes(expand bool) SyncOptions {
	opts.ExpandRemotes = expand
	return opts
}

// WithVersionConstraint sets a version constraint for the remotes.
func (opts SyncOptions) WithVersionConstraint(constraint string) SyncOptions {
	if constraint == "" {
		opts.VersionConstraint = nil
		return opts
	}
	constraints, err := semver.NewConstraint(constraint)
	if err != nil {
		panic(fmt.Sprintf("invalid version constraint: %s", constraint))
	}
	opts.VersionConstraint = constraints
	return opts
}

// WithUseLatestVersion indicates whether to use the latest version of the remote.
func (opts SyncOptions) WithUseLatestVersion(useLatest bool) SyncOptions {
	opts.UseLatestVersion = useLatest
	return opts
}

// WithChunkSize sets the number of blocks to put in each file/chunk.
func (opts SyncOptions) WithChunkSize(size int) SyncOptions {
	if size < 1 {
		panic("chunk size must be a positive integer")
	}
	opts.ChunkSize = size
	return opts
}

// WithTargetHeight sets the height up to which blocks should be fetched.
func (opts SyncOptions) WithTargetHeight(height int64) SyncOptions {
	if height < 0 {
		panic("height must be a non-negative integer")
	}
	opts.TargetHeight = height
	return opts
}

// WithFetchSize sets the number of blocks to fetch in each RPC call.
func (opts SyncOptions) WithFetchSize(size int) SyncOptions {
	if size < 1 {
		panic("fetch size must be a positive integer")
	}
	opts.FetchSize = size
	return opts
}

// WithNumWorkers sets the number of concurrent workers to fetch blocks.
func (opts SyncOptions) WithNumWorkers(num int) SyncOptions {
	if num < 1 {
		panic("number of workers must be a positive integer")
	}
	opts.NumWorkers = num
	return opts
}

// WithOutputChan sets the channel to which BlockRecords will be sent as they are stored.
// If nil, no records will be sent to a channel.
// The channel will be closed automatically after the sync operation is complete.
func (opts SyncOptions) WithOutputChan(outputChan chan<- *BlockRecord) SyncOptions {
	opts.OutputChan = outputChan
	return opts
}

// WithProgressBar sets the progress bar to use for displaying sync progress.
func (opts SyncOptions) WithProgressBar(pBar *mpb.Progress) SyncOptions {
	opts.ProgressBar = pBar
	return opts
}

// WithLogger sets the logger for the sync operation.
func (opts SyncOptions) WithLogger(logger *slog.Logger) SyncOptions {
	if logger == nil {
		panic("logger cannot be nil")
	}
	opts.Logger = logger
	return opts
}

// Sync fetches blocks up until the latest block height (or a specific height if provided)
// and stores them in the store directory.
func (s *Store) Sync(ctx context.Context, opts SyncOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if opts.OutputChan != nil {
		defer close(opts.OutputChan)
	}

	logger := opts.Logger

	logger.Info("Discovering nodes", "remotes", opts.Remotes)
	nodes, err := chainutil.DiscoverNodes(ctx, opts.Remotes, opts.ExpandRemotes, logger)
	if ctx.Err() != nil {
		return fmt.Errorf("sync canceled: %w", ctx.Err())
	}
	if err != nil {
		return fmt.Errorf("failed to discover peers: %w", err)
	}
	if opts.VersionConstraint != nil {
		nodes = nodes.ConstrainByVersion(*opts.VersionConstraint)
	}
	if opts.UseLatestVersion {
		nodes = nodes.LatestVersion()
	}
	logger.Info("Functional remotes", "count", len(nodes))
	if len(nodes) == 0 {
		return fmt.Errorf("no functional remotes available after discovery and filtering")
	}

	targetHeight := opts.TargetHeight
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

	queueSize := opts.ChunkSize/opts.FetchSize + 1
	logger.Info("Spawning fetch workers",
		"num_workers", opts.NumWorkers, "queue_size", queueSize)
	jobQueue, resultsChan, cleanup := jobqueue.Launch(
		ctx,
		opts.NumWorkers,
		queueSize,
		doFetch(nodes))
	defer cleanup()

	currentHeight := lastStoredHeight + 1
	logger.Info("Syncing store", "from", currentHeight, "to", targetHeight)
	syncProgress := buildSyncProgressBar(opts.ProgressBar, targetHeight, currentHeight)
	for currentHeight < targetHeight {
		now := time.Now()
		queuedBlocks := queueFetchJobs(int64(opts.ChunkSize), int64(opts.FetchSize),
			currentHeight, targetHeight, jobQueue)
		blocks, blockResults, err := collectFetchResults(ctx, jobQueue, resultsChan, queuedBlocks, opts.ProgressBar, logger)
		if err != nil {
			return fmt.Errorf("failed to collect fetch results: %w", err)
		}
		records := blockRecordsFromRPCResults(blocks, blockResults)
		pushToChannel(records, opts.OutputChan)
		chk, err := s.storeRecords(records)
		if err != nil {
			return fmt.Errorf("failed to store records: %w", err)
		}
		s.insertChunk(chk)

		if syncProgress != nil {
			syncProgress.EwmaIncrInt64(chk.length(), time.Since(now))
		}
		s.logger.Info("Stored chunk", "filename", chk.filename(),
			"height_range", fmt.Sprintf("%d-%d", chk.fromHeight, chk.toHeight))

		currentHeight += queuedBlocks
	}

	return nil
}

func (s *Store) lastStoredHeight() int64 {
	return s.chunks.endHeight()
}

type fetchJob struct {
	startHeight int64
	endHeight   int64
	retries     int
}

type fetchResult struct {
	blocks       []*coretypes.ResultBlock
	blockResults []*coretypes.ResultBlockResults
	timeElapsed  time.Duration
}

// NoNodesAvailableForRangeError is returned when no nodes are available for the given height range.
type NoNodesAvailableForRangeError struct {
	startHeight int64
	endHeight   int64
}

// Range returns the height range for which no nodes are available.
func (e *NoNodesAvailableForRangeError) Range() (int64, int64) {
	return e.startHeight, e.endHeight
}

func (e *NoNodesAvailableForRangeError) Error() string {
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
		now := time.Now()
		nodes := nodes.ByHeightRange(job.startHeight, job.endHeight)
		if len(nodes) == 0 {
			return nil, &NoNodesAvailableForRangeError{
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
			timeElapsed:  time.Since(now),
		}, nil
	}
}

func buildSyncProgressBar(progress *mpb.Progress, targetHeight, currentHeight int64) *mpb.Bar {
	if progress == nil {
		return nil
	}

	pbar := progress.New(
		targetHeight,
		barStyle,
		mpb.BarPriority(1),
		mpb.PrependDecorators(
			decor.CountersNoUnit("[%d / %d", decor.WCSyncSpaceR),
			decor.Name(" | ", decor.WCSyncWidth),
			decor.NewPercentage("%d]", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncWidth),
		),
	)
	pbar.SetCurrent(currentHeight - 1) // Start from the last stored height

	return pbar
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
	ctx context.Context,
	jobQueue chan<- *fetchJob,
	resultsChan <-chan jobqueue.JobResult[fetchJob, fetchResult],
	resultsLimit int64,
	progress *mpb.Progress,
	logger *slog.Logger,
) (
	blocks []*coretypes.ResultBlock,
	blockResults []*coretypes.ResultBlockResults,
	err error,
) {
	var pbar *mpb.Bar
	if progress != nil {
		pbar = buildFetchProgressBar(progress, resultsLimit)
	}

	for {
		select {
		case jr := <-resultsChan:
			// Job queue closed, no more jobs to process
			if jr.Err != nil {
				switch e := jr.Err.(type) {
				case *NoNodesAvailableForRangeError:
					return nil, nil, e
				case *fetchBlocksError:
					logger.Warn("Failed to fetch blocks",
						"height_range", fmt.Sprintf("%d-%d", jr.Job.startHeight, jr.Job.endHeight),
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

			if pbar != nil {
				pbar.EwmaIncrBy(len(jr.Result.blocks), jr.Result.timeElapsed)
			}

			if len(blocks) >= int(resultsLimit) {
				return blocks, blockResults, nil
			}
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("sync cancelled: %w", ctx.Err())
		}
	}
}

func buildFetchProgressBar(progress *mpb.Progress, total int64) *mpb.Bar {
	if progress == nil {
		return nil
	}

	return progress.New(
		total,
		barStyle,
		mpb.BarPriority(0),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.CountersNoUnit("[%d / %d", decor.WCSyncSpaceR),
			decor.Name(" | ", decor.WCSyncWidth),
			decor.NewPercentage("%d]", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.AverageSpeed(nil, "%0.f blocks/s", decor.WCSyncWidth),
		),
	)
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

func (s *Store) storeRecords(recs []*BlockRecord) (chunk, error) {
	startHeight := recs[0].Block.Height
	endHeight := recs[len(recs)-1].Block.Height
	chk := newChunk(startHeight, endHeight)
	file, err := s.createChunkFile(chk)
	if err != nil {
		return chunk{}, fmt.Errorf("failed to create chunk file: %w", err)
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
			return chunk{}, fmt.Errorf("failed to encode BlockRecord: %w", err)
		}
	}
	err = wr.Flush()
	if err != nil {
		return chunk{}, fmt.Errorf("failed to flush brotli writer: %w", err)
	}
	err = wr.Close()
	if err != nil {
		return chunk{}, fmt.Errorf("failed to close brotli writer: %w", err)
	}

	return chk, nil
}

func (s *Store) createChunkFile(chk chunk) (*os.File, error) {
	filePath := filepath.Join(s.dir, chk.filename())
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	return file, nil
}

func (s *Store) insertChunk(chk chunk) {
	idx, _ := slices.BinarySearchFunc(s.chunks, chk, chunksCmpFunc)
	s.chunks = slices.Insert(s.chunks, idx, chk)
}

// Blocks returns an iterator over block records in the store.
// It accepts optional start and end heights to limit the range of blocks returned.
// If no heights are provided, it defaults to the entire range of stored blocks.
// The iterator yields BlockRecord objects and any errors encountered during iteration.
func (s *Store) Blocks(args ...int64) iter.Seq2[*BlockRecord, error] {
	startHeight := int64(1)
	if len(args) > 0 {
		startHeight = args[0]
	}
	endHeight := s.lastStoredHeight()
	if len(args) > 1 {
		endHeight = args[1]
	}
	if startHeight < 1 || endHeight < 1 || startHeight > endHeight {
		panic(fmt.Errorf("invalid height range: %d-%d", startHeight, endHeight))
	}

	return func(yield func(*BlockRecord, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		for _, chk := range s.chunks {
			if chk.toHeight < startHeight {
				continue
			}
			if chk.fromHeight > endHeight {
				return
			}

			skip := 0
			if chk.fromHeight < startHeight && chk.toHeight >= startHeight {
				skip = int(startHeight - chk.fromHeight)
			}

			iter, err := s.iterChunk(chk, skip)
			if err != nil {
				err = fmt.Errorf("failed to iterate chunk %s: %w", chk.filename(), err)
				yield(nil, err)
				return
			}

			for rec, err := range iter {
				if err != nil {
					err = fmt.Errorf("failed to read record from chunk %s: %w", chk.filename(), err)
				}
				if rec.Block.Height > endHeight || !yield(rec, err) {
					return
				}
			}
		}
	}
}

func (s *Store) iterChunk(chk chunk, skip int) (iter.Seq2[*BlockRecord, error], error) {
	file, err := s.openChunk(chk)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file %s: %w", chk.filename(), err)
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

func (s *Store) openChunk(chk chunk) (*os.File, error) {
	filePath := filepath.Join(s.dir, chk.filename())
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

	chk, ok := s.chunks.findForHeight(height)
	if !ok {
		return nil, fmt.Errorf("block at height %d not found", height)
	}
	skip := int(height - chk.fromHeight)
	iter, err := s.iterChunk(chk, skip)
	if err != nil {
		return nil, fmt.Errorf("failed to iterate chunk %s: %w", chk.filename(), err)
	}
	for rec, err := range iter {
		if err != nil {
			return nil, fmt.Errorf("failed to get block at height %d: %w", height, err)
		}
		return rec, nil
	}

	return nil, fmt.Errorf("block at height %d not found", height)
}

// Normalize normalizes the chunks into a new chunks list of specified size.
func (s *Store) Normalize(chunkSize int64) error {
	if chunkSize < 1 {
		return fmt.Errorf("chunk size must be a positive integer, got %d", chunkSize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.chunks) == 0 {
		return nil // Nothing to normalize
	}

	buf := make([]*BlockRecord, 0, chunkSize)
	newChunks := make(chunks, 0, (s.chunks.endHeight()-s.chunks.startHeight()+1)/int64(chunkSize))
	removalList := make([]chunk, 0)
	for i, chk := range s.chunks {
		// Skip chunks that are either exactly of the specified size or the last chunk that is smaller than the size.
		if len(buf) == 0 && (chk.length() == chunkSize || (i == len(s.chunks)-1 && chk.length() <= chunkSize)) {
			newChunks = append(newChunks, chk)
			continue
		}

		iter, err := s.iterChunk(chk, 0)
		if err != nil {
			return fmt.Errorf("failed to iterate chunk %s: %w", chk.filename(), err)
		}
		for rec, err := range iter {
			if err != nil {
				return fmt.Errorf("failed to read record from chunk %s: %w", chk.filename(), err)
			}
			buf = append(buf, rec)
			if len(buf) == int(chunkSize) {
				newChunk, err := s.storeRecords(buf)
				if err != nil {
					return fmt.Errorf("failed to store records for chunk %s: %w", chk.filename(), err)
				}
				newChunks = append(newChunks, newChunk)
				s.logger.Info("Stored chunk", "filename", newChunk.filename(),
					"height_range", fmt.Sprintf("%d-%d", newChunk.fromHeight, newChunk.toHeight))
				buf = buf[:0] // Reset buffer after storing

				err = s.dropChunks(removalList...)
				if err != nil {
					s.logger.Error("Failed to drop old chunks", "error", err)
				}
				removalList = removalList[:0] // Reset removal list after dropping
			}
		}

		removalList = append(removalList, chk) // Mark chunk for removal
	}

	if len(buf) > 0 {
		// Store any remaining records that didn't fill a complete chunk
		newChunk, err := s.storeRecords(buf)
		if err != nil {
			return fmt.Errorf("failed to store remaining records: %w", err)
		}
		newChunks = append(newChunks, newChunk)
		s.logger.Info("Stored chunk", "filename", newChunk.filename(),
			"height_range", fmt.Sprintf("%d-%d", newChunk.fromHeight, newChunk.toHeight))
	}

	// Drop remaining marked chunks
	err := s.dropChunks(removalList...)
	if err != nil {
		s.logger.Error("Failed to drop old chunks", "error", err)
	}
	s.chunks = newChunks
	s.chunks.sort() // Ensure chunks are sorted after normalization

	return nil
}

func (s *Store) dropChunks(chunks ...chunk) error {
	for _, chk := range chunks {
		if err := s.dropChunk(chk); err != nil {
			return fmt.Errorf("failed to drop chunk %s: %w", chk.filename(), err)
		}

		if s.logger != nil {
			s.logger.Info("Dropped chunk", "filename", chk.filename(),
				"height_range", fmt.Sprintf("%d-%d", chk.fromHeight, chk.toHeight))
		}
	}
	return nil
}

func (s *Store) dropChunk(chk chunk) error {
	filePath := filepath.Join(s.dir, chk.filename())
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to remove chunk file %s: %w", filePath, err)
	}

	idx, ok := slices.BinarySearchFunc(s.chunks, chk, chunksCmpFunc)
	if !ok {
		return fmt.Errorf("chunk %s not found in store", chk.filename())
	}
	s.chunks = slices.Delete(s.chunks, idx, idx)

	return nil
}
