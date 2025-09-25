package cometdump

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/vbauerster/mpb/v8"
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
	var (
		file io.ReadCloser
		err  error
	)
	if _, err = url.Parse(checksumsFile); err == nil {
		resp, err := http.Get(checksumsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch checksums file from URL %s: %w", checksumsFile, err)
		}
		defer resp.Body.Close()
		file = resp.Body
	} else {
		file, err = os.Open(checksumsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to open checksums file %s: %w", checksumsFile, err)
		}
		defer file.Close()
	}

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
