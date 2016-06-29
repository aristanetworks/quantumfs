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
	data      MultiBlockContainer
	maxBlocks int
}

func newMultiBlockAccessor(c *ctx, key quantumfs.ObjectKey,
	maxBlocks int) *MultiBlockFile {

	var rtn MultiBlockFile
	rtn.maxBlocks = maxBlocks

	buffer := DataStore.Get(c, key)
	if buffer == nil {
		c.elog("Unable to fetch metadata for new file creation")
		panic("Unable to fetch metadata for new file creation")
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

	newLength := make([]quantumfs.ObjectKey, length-len(fi.data.Blocks))
	for i := 0; i < len(newLength); i++ {
		newLength[i] = quantumfs.EmptyBlockKey
	}
	fi.data.Blocks = append(fi.data.Blocks, newLength...)
}

func (fi *MultiBlockFile) readBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if offset >= uint64(fi.data.BlockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	// If we read too far then there's nothing to return
	if blockIdx >= len(fi.data.Blocks) {
		return 0, nil
	}

	expectedSize := fi.data.BlockSize
	if blockIdx == len(fi.data.Blocks)-1 {
		// This is the last block, so it may not be filled
		expectedSize = fi.data.LastBlockBytes
	}
	// Grab the data
	data := fetchDataSized(c, fi.data.Blocks[blockIdx], int(expectedSize))

	copied := copy(buf, data.Get()[offset:])
	return copied, nil
}

func (fi *MultiBlockFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > fi.maxBlocks {
		return 0, errors.New("BlockIdx exceeds bounds for file accessor")
	}

	if offset >= uint64(fi.data.BlockSize) {
		return 0, errors.New("Attempt to write past end of block")
	}

	// Ensure we expand the file to fit the blockIdx
	if blockIdx >= len(fi.data.Blocks) {
		fi.expandTo(blockIdx + 1)
	}

	// Grab the data
	data := DataStore.Get(c, fi.data.Blocks[blockIdx])
	if data == nil {
		c.elog("Unable to fetch data for block %s", fi.data.Blocks[blockIdx])
		return 0, errors.New("Unable to fetch block data")
	}

	copied := data.Write(buf, uint32(offset))
	if copied > 0 {
		newFileKey, err := pushData(c, data)
		if err != nil {
			c.elog("Write failed")
			return 0, errors.New("Unable to write to block")
		}
		//store the key and update the metadata
		fi.data.Blocks[blockIdx] = newFileKey
		if blockIdx == len(fi.data.Blocks)-1 {
			fi.data.LastBlockBytes = uint32(len(data.Get()))
		}
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data, %d, %d", copied, len(buf))
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *MultiBlockFile) fileLength() uint64 {
	return (uint64(fi.data.BlockSize) * uint64(len(fi.data.Blocks)-1)) +
		uint64(fi.data.LastBlockBytes)
}

func (fi *MultiBlockFile) blockIdxInfo(absOffset uint64) (int, uint64) {
	blkIdx := absOffset / uint64(fi.data.BlockSize)
	remainingOffset := absOffset % uint64(fi.data.BlockSize)

	return int(blkIdx), remainingOffset
}

func (fi *MultiBlockFile) writeToStore(c *ctx) quantumfs.ObjectKey {
	bytes, err := json.Marshal(fi.data)
	if err != nil {
		panic("Unable to marshal file metadata")
	}

	buffer := quantumfs.NewBuffer(bytes, quantumfs.KeyTypeMetadata)

	if err := c.durableStore.Set(buffer.Key(), &buffer); err != nil {
		panic("Failed to upload new file metadata")
	}

	return buffer.Key()
}

func (fi *MultiBlockFile) truncate(c *ctx, newLengthBytes uint64) error {
	newEndBlkIdx := (newLengthBytes - 1) / uint64(fi.data.BlockSize)
	newNumBlocks := newEndBlkIdx + 1
	lastBlockLen := newLengthBytes - (newEndBlkIdx * uint64(fi.data.BlockSize))

	// Handle the special zero length case
	if newLengthBytes == 0 {
		newEndBlkIdx = 0
		newNumBlocks = 0
		lastBlockLen = 0
	}

	// If we're increasing the length, we need to update the block num
	expandingFile := false
	if newNumBlocks > uint64(len(fi.data.Blocks)) {
		fi.expandTo(int(newNumBlocks))
		expandingFile = true
	}

	// Allow increasing just the last block
	if (newNumBlocks == uint64(len(fi.data.Blocks)) &&
		lastBlockLen > uint64(fi.data.LastBlockBytes)) ||
		expandingFile {

		fi.data.LastBlockBytes = uint32(lastBlockLen)
		return nil
	}

	// Truncate the new last block
	data := fetchDataSized(c, fi.data.Blocks[newEndBlkIdx], int(lastBlockLen))
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
	fi.data.Blocks[newEndBlkIdx] = newFileKey
	fi.data.Blocks = fi.data.Blocks[:newNumBlocks]
	fi.data.LastBlockBytes = uint32(lastBlockLen)

	return nil
}
