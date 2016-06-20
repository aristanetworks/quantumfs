// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains the generic multi-block file types and methods

import "arista.com/quantumfs"
import "encoding/json"
import "errors"

// These variables are always correct. Where the datastore value length disagrees,
// this structure is correct.
type MultiBlockContainer struct {
	blockSize      uint32
	lastBlockBytes uint32
	blocks         []quantumfs.ObjectKey
}

type MultiBlockFile struct {
	data		MultiBlockContainer
	maxBlocks	int
}

func newMultiBlockAccessor(c *ctx, key quantumfs.ObjectKey,
	maxBlocks int) *MultiBlockFile {

	var rtn MultiBlockFile
	rtn.maxBlocks = maxBlocks
	rtn.data.blockSize = quantumfs.MaxBlockSize

	buffer := DataStore.Get(c, key)
	if buffer == nil {
		c.elog("Unable to fetch metadata for new file creation")
		// Assume that the file is empty
		rtn.data.lastBlockBytes = 0
		return &rtn
	}

	if err := json.Unmarshal(buffer.Get(), &rtn.data); err != nil {
		panic("Couldn't decode MultiBlockContainer object")
	}

	return &rtn
}

func (fi *MultiBlockFile) expandTo(length int) {
	if length > fi.maxBlocks {
		panic("Invalid new length set to expandTo for file accessor")
	}

	newLength := make([]quantumfs.ObjectKey, length-len(fi.data.blocks))
	for i := 0; i < len(newLength); i++ {
		newLength[i] = quantumfs.EmptyBlockKey
	}
	fi.data.blocks = append(fi.data.blocks, newLength...)
}

func (fi *MultiBlockFile) readBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if offset >= uint64(fi.data.blockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	// If we read too far then there's nothing to return
	if blockIdx >= len(fi.data.blocks) {
		return 0, nil
	}

	expectedSize := fi.data.blockSize
	if blockIdx == len(fi.data.blocks)-1 {
		// This is the last block, so it may not be filled
		expectedSize = fi.data.lastBlockBytes
	}
	// Grab the data
	data := fetchDataSized(c, fi.data.blocks[blockIdx], int(expectedSize))

	copied := copy(buf, data.Get()[offset:])
	return copied, nil
}

func (fi *MultiBlockFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > fi.maxBlocks {
		return 0, errors.New("BlockIdx exceeds bounds for file accessor")
	}

	if offset >= uint64(fi.data.blockSize) {
		return 0, errors.New("Attempt to write past end of block")
	}

	// Ensure we expand the file to fit the blockIdx
	if blockIdx >= len(fi.data.blocks) {
		fi.expandTo(blockIdx + 1)
	}

	// Grab the data
	data := DataStore.Get(c, fi.data.blocks[blockIdx])
	if data == nil {
		c.elog("Unable to fetch data for block %s", fi.data.blocks[blockIdx])
		return 0, errors.New("Unable to fetch block data")
	}

	copied := data.Write(buf, uint32(offset))
	if copied > 0 {
		newFileKey, err := pushData(c, data)
		if err != nil {
			c.elog("Write failed")
			return 0, errors.New("Unable to write to block")
		}
		//store the key
		fi.data.blocks[blockIdx] = *newFileKey
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data, %d, %d", copied, len(buf))
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *MultiBlockFile) fileLength() uint64 {
	return (uint64(fi.data.blockSize) * uint64(len(fi.data.blocks)-1)) +
		uint64(fi.data.lastBlockBytes)
}

func (fi *MultiBlockFile) blockIdxInfo(absOffset uint64) (int, uint64) {
	blkIdx := absOffset / uint64(fi.data.blockSize)
	remainingOffset := absOffset % uint64(fi.data.blockSize)

	return int(blkIdx), remainingOffset
}

func (fi *MultiBlockFile) writeToStore(c *ctx) quantumfs.ObjectKey {
	bytes, err := json.Marshal(fi.data)
	if err != nil {
		panic("Unable to marshal file metadata")
	}

	var buffer quantumfs.Buffer
	buffer.Set(bytes)

	newFileKey := buffer.Key(quantumfs.KeyTypeMetadata)
	if err := c.durableStore.Set(newFileKey, &buffer); err != nil {
		panic("Failed to upload new file metadata")
	}

	return newFileKey
}

func (fi *MultiBlockFile) truncate(c *ctx, newLengthBytes uint64) error {
	newEndBlkIdx := newLengthBytes / quantumfs.MaxBlockSize
	newLengthBytes = newLengthBytes % quantumfs.MaxBlockSize

	// If we're increasing the length, we can just update
	if newEndBlkIdx >= uint64(len(fi.data.blocks)) {
		fi.expandTo(int(newEndBlkIdx + 1))
		return nil
	}

	// Allow increasing just the last block
	if newEndBlkIdx == uint64(len(fi.data.blocks)-1) &&
		newLengthBytes > uint64(fi.data.lastBlockBytes) {

		fi.data.lastBlockBytes = uint32(newLengthBytes)
		return nil
	}

	// Truncate the new last block
	data := fetchDataSized(c, fi.data.blocks[newEndBlkIdx], int(newLengthBytes))
	if data == nil {
		c.elog("Unable to fetch existing data for block")
		return errors.New("Unable to fetch existing data")
	}

	newFileKey, err := pushData(c, data)
	if err != nil {
		c.elog("Block data write failed")
		return errors.New("Unable to write to block")
	}

	// Now that everything has succeeded and is in the datastore, update metadata
	fi.data.blocks[newEndBlkIdx] = *newFileKey
	fi.data.blocks = fi.data.blocks[:newEndBlkIdx+1]
	fi.data.lastBlockBytes = uint32(newLengthBytes)

	return nil
}
