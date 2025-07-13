package cometdump

import (
	"fmt"
	"os"
	"slices"
)

const chunkFilenameFormat = "%012d-%012d.msgpack.br"

type chunks []chunk

func readChunksList(dir string) (chunks, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}
	var chunks chunks
	for _, file := range files {
		if !file.Type().IsRegular() {
			continue
		}

		var c chunk
		if _, err := fmt.Sscanf(file.Name(), chunkFilenameFormat, &c.fromHeight, &c.toHeight); err != nil {
			continue
		}
		if !c.isValid() {
			return nil, fmt.Errorf("invalid chunk file %s: %d-%d", file.Name(), c.fromHeight, c.toHeight)
		}
		chunks = append(chunks, c)
	}
	chunks.sort()

	if err := chunks.validate(); err != nil {
		return nil, err
	}

	return chunks, nil
}

func (chks chunks) startHeight() int64 {
	if len(chks) == 0 {
		return 0
	}
	return chks[0].fromHeight
}

func (chks chunks) endHeight() int64 {
	if len(chks) == 0 {
		return 0
	}
	return chks[len(chks)-1].toHeight
}

func (chks chunks) findForHeight(height int64) (chunk, bool) {
	idx, ok := slices.BinarySearchFunc(chks, newChunk(height, height), compareChunks)
	if !ok {
		return chunk{}, false
	}
	return chks[idx], true
}

func compareChunks(a, b chunk) int {
	if a.toHeight < b.fromHeight {
		return -1
	} else if a.fromHeight > b.toHeight {
		return 1
	}
	return 0
}

func (chks chunks) sort() {
	slices.SortFunc(chks, func(a, b chunk) int {
		if a.fromHeight < b.fromHeight {
			return -1
		} else if a.fromHeight > b.fromHeight {
			return 1
		}
		return 0
	})
}

func (chks chunks) validate() error {
	for i := 0; i < len(chks)-1; i++ {
		if chks[i].toHeight >= chks[i+1].fromHeight {
			return fmt.Errorf("chunks %d-%d and %d-%d overlap",
				chks[i].fromHeight, chks[i].toHeight, chks[i+1].fromHeight, chks[i+1].toHeight)
		}
	}
	return nil
}

type chunk struct {
	fromHeight int64
	toHeight   int64
}

func newChunk(fromHeight, toHeight int64) chunk {
	return chunk{fromHeight: fromHeight, toHeight: toHeight}
}

func (c chunk) isValid() bool {
	return c.fromHeight > 0 && c.toHeight >= c.fromHeight
}

func (c chunk) filename() string {
	return fmt.Sprintf(chunkFilenameFormat, c.fromHeight, c.toHeight)
}
