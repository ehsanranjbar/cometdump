package cometdump

import (
	"fmt"
	"os"
	"slices"
)

const chunkFilenameFormat = "%012d-%012d.msgpack.br"

// Chunks is a slice of Chunk objects.
type Chunks []Chunk

// StartHeight returns the start height of the first chunk in the list.
func (Chunks Chunks) StartHeight() int64 {
	if len(Chunks) == 0 {
		return 0
	}
	return Chunks[0].FromHeight
}

// EndHeight returns the end height of the last chunk in the list.
func (Chunks Chunks) EndHeight() int64 {
	if len(Chunks) == 0 {
		return 0
	}
	return Chunks[len(Chunks)-1].ToHeight
}

// FindForHeight returns the chunk that contains the given height.
// If no chunk is found, it returns an error.
func (chunks Chunks) FindForHeight(height int64) (Chunk, bool) {
	idx, ok := slices.BinarySearchFunc(chunks, height, func(c Chunk, h int64) int {
		if c.FromHeight <= h && c.ToHeight >= h {
			return 0
		} else if c.FromHeight > h {
			return 1
		}
		return -1
	})
	if !ok {
		return Chunk{}, false
	}
	return chunks[idx], true
}

func (chunks Chunks) sort() {
	slices.SortFunc(chunks, func(a, b Chunk) int {
		if a.FromHeight < b.FromHeight {
			return -1
		} else if a.FromHeight > b.FromHeight {
			return 1
		}
		return 0
	})
}

func (chunks Chunks) validate() error {
	for i := 0; i < len(chunks)-1; i++ {
		if chunks[i].ToHeight >= chunks[i+1].FromHeight {
			return fmt.Errorf("chunks %d-%d and %d-%d overlap",
				chunks[i].FromHeight, chunks[i].ToHeight, chunks[i+1].FromHeight, chunks[i+1].ToHeight)
		}
	}
	return nil
}

// Chunk represents a range of blocks in the store.
// It contains the start and end heights of the chunk.
type Chunk struct {
	FromHeight int64
	ToHeight   int64
}

func newChunk(fromHeight, toHeight int64) Chunk {
	return Chunk{FromHeight: fromHeight, ToHeight: toHeight}
}

// String implements the Stringer interface.
func (c Chunk) String() string {
	return fmt.Sprintf("%d-%d", c.FromHeight, c.ToHeight)
}

// Len returns the length of the chunk, which is the number of blocks it contains.
func (c Chunk) Len() int64 {
	if c.ToHeight < c.FromHeight {
		return 0
	}
	return c.ToHeight - c.FromHeight + 1
}

func (c Chunk) isValid() bool {
	return c.FromHeight > 0 && c.ToHeight >= c.FromHeight
}

func (c Chunk) filename() string {
	return fmt.Sprintf(chunkFilenameFormat, c.FromHeight, c.ToHeight)
}

func readChunksList(dir string) (Chunks, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}
	var chunks Chunks
	for _, file := range files {
		if !file.Type().IsRegular() {
			continue
		}

		var c Chunk
		if _, err := fmt.Sscanf(file.Name(), chunkFilenameFormat, &c.FromHeight, &c.ToHeight); err != nil {
			continue
		}
		if !c.isValid() {
			return nil, fmt.Errorf("invalid chunk file %s: %d-%d", file.Name(), c.FromHeight, c.ToHeight)
		}
		chunks = append(chunks, c)
	}
	chunks.sort()

	if err := chunks.validate(); err != nil {
		return nil, err
	}

	return chunks, nil
}
