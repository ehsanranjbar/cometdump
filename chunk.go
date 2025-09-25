package cometdump

import (
	"fmt"
	"log/slog"
	"os"
	"slices"
)

const chunkFilenameFormat = "%012d-%012d.msgpack.br"

type chunks []Chunk

func readChunksList(dir string, logger *slog.Logger) (chunks, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}
	var chunks chunks
	for _, file := range files {
		if !file.Type().IsRegular() {
			continue
		}

		c, err := parseChunkFilename(file.Name())
		if err != nil {
			continue
		}
		if !c.isValid() {
			logger.Warn("Invalid chunk file found, skipping", "file", file.Name())
			continue
		}

		logger.Debug("Discovered chunk file", "file", file.Name(),
			"height_range", fmt.Sprintf("%d-%d", c.FromHeight, c.ToHeight), "length", c.length())
		chunks = append(chunks, c)
	}
	chunks.sort()

	if err := chunks.validate(); err != nil {
		return nil, err
	}

	return chunks, nil
}

func parseChunkFilename(filename string) (Chunk, error) {
	var fromHeight, toHeight int64
	if _, err := fmt.Sscanf(filename, chunkFilenameFormat, &fromHeight, &toHeight); err != nil {
		return Chunk{}, fmt.Errorf("failed to parse chunk filename %s: %w", filename, err)
	}
	return newChunk(fromHeight, toHeight), nil
}

func (chks chunks) startHeight() int64 {
	if len(chks) == 0 {
		return 0
	}
	return chks[0].FromHeight
}

func (chks chunks) endHeight() int64 {
	if len(chks) == 0 {
		return 0
	}
	return chks[len(chks)-1].ToHeight
}

func (chks chunks) findForHeight(height int64) (Chunk, bool) {
	idx, ok := slices.BinarySearchFunc(chks, newChunk(height, height), compareChunks)
	if !ok {
		return Chunk{}, false
	}
	return chks[idx], true
}

func compareChunks(a, b Chunk) int {
	if a.ToHeight < b.FromHeight {
		return -1
	} else if a.FromHeight > b.ToHeight {
		return 1
	}
	return 0
}

func (chks chunks) sort() {
	slices.SortFunc(chks, chunksCmpFunc)
}

func chunksCmpFunc(a, b Chunk) int {
	if a.FromHeight < b.FromHeight {
		return -1
	} else if a.FromHeight > b.FromHeight {
		return 1
	} else if a.ToHeight < b.ToHeight {
		return -1
	} else if a.ToHeight > b.ToHeight {
		return 1
	}
	return 0
}

func (chks chunks) validate() error {
	for i := 0; i < len(chks)-1; i++ {
		if chks[i].ToHeight >= chks[i+1].FromHeight {
			return fmt.Errorf("chunks %d-%d and %d-%d overlap",
				chks[i].FromHeight, chks[i].ToHeight, chks[i+1].FromHeight, chks[i+1].ToHeight)
		}
	}
	return nil
}

// Chunk represents a range of block heights stored in a single chunk file.
type Chunk struct {
	FromHeight int64
	ToHeight   int64
}

func newChunk(fromHeight, toHeight int64) Chunk {
	return Chunk{FromHeight: fromHeight, ToHeight: toHeight}
}

func (c Chunk) isValid() bool {
	return c.FromHeight > 0 && c.ToHeight >= c.FromHeight
}

func (c Chunk) filename() string {
	return fmt.Sprintf(chunkFilenameFormat, c.FromHeight, c.ToHeight)
}

func (c Chunk) length() int64 {
	return c.ToHeight - c.FromHeight + 1
}
