// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains the generic multi-block file types and methods

import "github.com/aristanetworks/quantumfs"
import "encoding/json"
import "errors"

// These variables are always correct. Where the datastore value length disagrees,
// this structure is correct.
type MultiBlockContainer struct {
	BlockSize      uint32
	LastBlockBytes uint32
	Blocks         []quantumfs.ObjectKey
}

type MultiBlockFile struct {
	file       *File
	metadata   MultiBlockContainer
	dataBlocks map[int]quantumfs.Buffer
	maxBlocks  int
}

func newMultiBlockAccessor(c *ctx, key quantumfs.ObjectKey,
	maxBlocks int) *MultiBlockFile {

	var rtn MultiBlockFile
	initMultiBlockAccessor(&rtn, maxBlocks)

	buffer := c.dataStore.Get(&c.Ctx, key)
	if buffer == nil {
		c.elog("Unable to fetch metadata for new file creation")
		panic("Unable to fetch metadata for new file creation")
	}

	if err := json.Unmarshal(buffer.Get(), &rtn.metadata); err != nil {
		panic("Couldn't decode MultiBlockContainer object")
	}

	return &rtn
}

func initMultiBlockAccessor(multiBlock *MultiBlockFile, maxBlocks int) {
	multiBlock.maxBlocks = maxBlocks
	multiBlock.dataBlocks = make(map[int]quantumfs.Buffer)
}

func (fi *MultiBlockFile) expandTo(length int) {
	if length > fi.maxBlocks {
		panic("Invalid new length set to expandTo for file accessor")
	}

	newLength := make([]quantumfs.ObjectKey, length-len(fi.metadata.Blocks))
	for i := 0; i < len(newLength); i++ {
		newLength[i] = quantumfs.EmptyBlockKey
	}
	fi.metadata.Blocks = append(fi.metadata.Blocks, newLength...)
}

func (fi *MultiBlockFile) retrieveDataBlock(c *ctx, blockIdx int) quantumfs.Buffer {
	block, exists := fi.dataBlocks[blockIdx]
	if !exists {
		block = c.dataStore.Get(&c.Ctx, fi.metadata.Blocks[blockIdx])
		fi.dataBlocks[blockIdx] = block
	}

	return block
}

func (fi *MultiBlockFile) readBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if offset >= uint64(fi.metadata.BlockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	// If we read too far then there's nothing to return
	if blockIdx >= len(fi.metadata.Blocks) {
		return 0, nil
	}

	expectedSize := fi.metadata.BlockSize
	if blockIdx == len(fi.metadata.Blocks)-1 {
		// This is the last block, so it may not be filled
		expectedSize = fi.metadata.LastBlockBytes
	}

	block := fi.retrieveDataBlock(c, blockIdx)
	block.SetSize(int(expectedSize))

	copied := block.Read(buf, uint32(offset))
	return copied, nil
}

func (fi *MultiBlockFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > fi.maxBlocks {
		return 0, errors.New("BlockIdx exceeds bounds for file accessor")
	}

	if offset >= uint64(fi.metadata.BlockSize) {
		return 0, errors.New("Attempt to write past end of block")
	}

	// Ensure we expand the file to fit the blockIdx
	if blockIdx >= len(fi.metadata.Blocks) {
		fi.expandTo(blockIdx + 1)
	}

	block := fi.retrieveDataBlock(c, blockIdx)

	copied := block.Write(&c.Ctx, buf, uint32(offset))
	if copied > 0 {
		if blockIdx == len(fi.metadata.Blocks)-1 {
			fi.metadata.LastBlockBytes = uint32(block.Size())
		}
		fi.file.setDirty(true)
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data, %d, %d", copied, len(buf))
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *MultiBlockFile) fileLength() uint64 {
	return (uint64(fi.metadata.BlockSize) * uint64(len(fi.metadata.Blocks)-1)) +
		uint64(fi.metadata.LastBlockBytes)
}

func (fi *MultiBlockFile) blockIdxInfo(absOffset uint64) (int, uint64) {
	blkIdx := absOffset / uint64(fi.metadata.BlockSize)
	remainingOffset := absOffset % uint64(fi.metadata.BlockSize)

	return int(blkIdx), remainingOffset
}

func (fi *MultiBlockFile) sync(c *ctx) quantumfs.ObjectKey {
	for i, block := range fi.dataBlocks {
		key, err := block.Key(&c.Ctx)
		if err != nil {
			panic("TODO Failed to update datablock")
		}
		fi.metadata.Blocks[i] = key
	}

	bytes, err := json.Marshal(fi.metadata)
	if err != nil {
		panic("Unable to marshal file metadata")
	}

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	key, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new file metadata")
	}

	return key
}

func (fi *MultiBlockFile) truncate(c *ctx, newLengthBytes uint64) error {
	newEndBlkIdx := (newLengthBytes - 1) / uint64(fi.metadata.BlockSize)
	newNumBlocks := newEndBlkIdx + 1
	lastBlockLen := newLengthBytes -
		(newEndBlkIdx * uint64(fi.metadata.BlockSize))

	// Handle the special zero length case
	if newLengthBytes == 0 {
		newEndBlkIdx = 0
		newNumBlocks = 0
		lastBlockLen = 0
	}

	// If we're increasing the length, we need to update the block num
	expandingFile := false
	if newNumBlocks > uint64(len(fi.metadata.Blocks)) {
		fi.expandTo(int(newNumBlocks))
		expandingFile = true
	}

	// Allow increasing just the last block
	if (newNumBlocks == uint64(len(fi.metadata.Blocks)) &&
		lastBlockLen > uint64(fi.metadata.LastBlockBytes)) ||
		expandingFile {

		fi.metadata.LastBlockBytes = uint32(lastBlockLen)
		return nil
	}

	// Truncate the new last block
	block := fi.retrieveDataBlock(c, int(newEndBlkIdx))
	block.SetSize(int(lastBlockLen))
	fi.metadata.LastBlockBytes = uint32(lastBlockLen)

	return nil
}

func (fi *MultiBlockFile) setFile(file *File) {
	fi.file = file
}
