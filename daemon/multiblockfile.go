// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains the generic multi-block file types and methods

import "github.com/aristanetworks/quantumfs"
import "errors"

// These variables are always correct. Where the datastore value length disagrees,
// this structure is correct.
type MultiBlockContainer struct {
	BlockSize      uint32
	LastBlockBytes uint32
	Blocks         []quantumfs.ObjectKey
}

type MultiBlockFile struct {
	metadata  MultiBlockContainer
	toSync    map[int]quantumfs.Buffer
	maxBlocks int
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

	store := buffer.AsMultiBlockFile()

	rtn.metadata.BlockSize = store.BlockSize()
	rtn.metadata.LastBlockBytes = store.SizeOfLastBlock()
	rtn.metadata.Blocks = store.ListOfBlocks()

	return &rtn
}

func initMultiBlockAccessor(multiBlock *MultiBlockFile, maxBlocks int) {
	multiBlock.maxBlocks = maxBlocks
	multiBlock.toSync = make(map[int]quantumfs.Buffer)
	multiBlock.metadata.BlockSize = uint32(quantumfs.MaxBlockSize)
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
	c.vlog("MultiBlockFile::retrieveDataBlock Enter %d", blockIdx)
	defer c.vlog("MultiBlockFile::retrieveDataBlock Exit")
	block, exists := fi.toSync[blockIdx]
	if !exists {
		// Because multiblock files can be so large and consume many blocks,
		// we anticipate filling blocks fully and don't want to have to grow
		// individual blocks ever. This gives huge performance gains.
		return c.dataStore.Get(&c.Ctx, fi.metadata.Blocks[blockIdx])
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

	if offset >= uint64(expectedSize) {
		return 0, nil
	}

	block := fi.retrieveDataBlock(c, blockIdx)

	// Copy only what we have, and then zero out the rest
	copied := 0
	if offset < uint64(block.Size()) {
		copied = block.Read(buf, uint32(offset))
	}
	remainingLen := int(expectedSize) - (int(offset) + copied)

	for i := 0; i < remainingLen; i++ {
		// Stop if buf isn't big enough to hold all the data
		if copied >= len(buf) {
			break
		}

		buf[copied] = 0
		copied++
	}

	return copied, nil
}

func (fi *MultiBlockFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > fi.maxBlocks {
		c.elog("BlockIdx exceeds bounds for accessor: %d", blockIdx)
		return 0, errors.New("BlockIdx exceeds bounds for file accessor")
	}

	if offset >= uint64(fi.metadata.BlockSize) {
		c.elog("Attempt to write past end of block, %d", offset)
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

		// Ensure we note the block for syncing
		fi.toSync[blockIdx] = block

		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data, %d, %d", copied, len(buf))
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *MultiBlockFile) fileLength() uint64 {
	return (uint64(fi.metadata.BlockSize) * uint64(len(fi.metadata.Blocks)-1)) +
		uint64(fi.metadata.LastBlockBytes)
}

func (fi *MultiBlockFile) blockIdxInfo(c *ctx, absOffset uint64) (int, uint64) {
	blkIdx := absOffset / uint64(fi.metadata.BlockSize)
	remainingOffset := absOffset % uint64(fi.metadata.BlockSize)

	return int(blkIdx), remainingOffset
}

func (fi *MultiBlockFile) sync(c *ctx) quantumfs.ObjectKey {
	c.vlog("MultiBlockFile::sync Enter")
	defer c.vlog("MultiBlockFile::sync Exit")

	for i, block := range fi.toSync {
		c.vlog("Syncing block %d", i)
		key, err := block.Key(&c.Ctx)
		if err != nil {
			panic("TODO Failed to update datablock")
		}
		fi.metadata.Blocks[i] = key
		delete(fi.toSync, i)
	}

	store := quantumfs.NewMultiBlockFile(fi.maxBlocks)
	store.SetBlockSize(fi.metadata.BlockSize)
	store.SetNumberOfBlocks(len(fi.metadata.Blocks))
	store.SetSizeOfLastBlock(fi.metadata.LastBlockBytes)
	store.SetListOfBlocks(fi.metadata.Blocks)

	bytes := store.Bytes()

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
		fi.toSync = make(map[int]quantumfs.Buffer)
		fi.metadata.Blocks = make([]quantumfs.ObjectKey, 0)
		fi.metadata.LastBlockBytes = 0
		return nil
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

	// If we're decreasing length, we need to throw away toSync
	for i := newEndBlkIdx + 1; i < uint64(len(fi.metadata.Blocks)); i++ {
		delete(fi.toSync, int(i))
	}
	fi.metadata.Blocks = fi.metadata.Blocks[:newEndBlkIdx+1]

	// Truncate the new last block
	block := fi.retrieveDataBlock(c, int(newEndBlkIdx))
	block.SetSize(int(lastBlockLen))
	fi.metadata.LastBlockBytes = uint32(lastBlockLen)

	return nil
}
