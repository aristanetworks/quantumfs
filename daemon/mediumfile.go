// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains medium file types and methods

import "arista.com/quantumfs"
import "encoding/json"
import "errors"

// These variables are always correct. Where the datastore value length disagrees,
// this structure is correct.
type MediumFile struct {
	blockSize      uint32
	lastBlockBytes uint32
	blocks         []quantumfs.ObjectKey
}

func newMediumAccessor(c *ctx, key quantumfs.ObjectKey) *MediumFile {
	var rtn MediumFile
	rtn.blockSize = quantumfs.MaxBlockSize

	buffer := DataStore.Get(c, key)
	if buffer == nil {
		c.elog("Unable to fetch metadata for new medium file creation")
		// Assume that the file is empty
		rtn.lastBlockBytes = 0
		return &rtn
	}

	if err := json.Unmarshal(buffer.Get(), &rtn); err != nil {
		panic("Couldn't decode MediumFile object")
	}

	return &rtn
}

func (fi *MediumFile) ExpandTo(length int) {
	newLength := make([]quantumfs.ObjectKey, length-len(fi.blocks))
	for i := 0; i < len(newLength); i++ {
		newLength[i] = quantumfs.EmptyBlockKey
	}
	fi.blocks = append(fi.blocks, newLength...)
}

func (fi *MediumFile) ReadBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if offset >= uint64(fi.blockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	// If we read too far then there's nothing to return
	if blockIdx >= len(fi.blocks) {
		return 0, nil
	}

	expectedSize := fi.blockSize
	if blockIdx == len(fi.blocks)-1 {
		// This is the last block, so it may not be filled
		expectedSize = fi.lastBlockBytes
	}
	// Grab the data
	data := fetchDataSized(c, fi.blocks[blockIdx], int(expectedSize))

	copied := copy(buf, data.Get()[offset:])
	return copied, nil
}

func (fi *MediumFile) WriteBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > quantumfs.MaxBlocksMediumFile {
		return 0, errors.New("BlockIdx exceeds bounds for medium file")
	}

	if offset >= uint64(fi.blockSize) {
		return 0, errors.New("Attempt to write past end of block")
	}

	// Ensure we expand the file to fit the blockIdx
	if blockIdx >= len(fi.blocks) {
		fi.ExpandTo(blockIdx + 1)
	}

	// Grab the data
	data := DataStore.Get(c, fi.blocks[blockIdx])
	if data == nil {
		c.elog("Unable to fetch data for block %s", fi.blocks[blockIdx])
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
		fi.blocks[blockIdx] = *newFileKey
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data")
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *MediumFile) GetFileLength() uint64 {
	return (uint64(fi.blockSize) * uint64(len(fi.blocks)-1)) +
		uint64(fi.lastBlockBytes)
}

func (fi *MediumFile) GetBlockLength() uint64 {
	return uint64(fi.blockSize)
}

func (fi *MediumFile) WriteToStore(c *ctx) quantumfs.ObjectKey {
	bytes, err := json.Marshal(fi)
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

func (fi *MediumFile) GetType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeMediumFile
}

func (fi *MediumFile) ConvertTo(c *ctx, newType quantumfs.ObjectType) BlockAccessor {

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}

func (fi *MediumFile) Truncate(c *ctx, newLengthBytes uint64) error {
	newEndBlkIdx := newLengthBytes / quantumfs.MaxBlockSize
	newLengthBytes = newLengthBytes % quantumfs.MaxBlockSize

	// If we're increasing the length, we can just update
	if newEndBlkIdx >= uint64(len(fi.blocks)) {
		fi.ExpandTo(int(newEndBlkIdx + 1))
		return nil
	}

	// Allow increasing just the last block
	if newEndBlkIdx == uint64(len(fi.blocks)-1) &&
		newLengthBytes > uint64(fi.lastBlockBytes) {

		fi.lastBlockBytes = uint32(newLengthBytes)
		return nil
	}

	// Truncate the new last block
	data := fetchDataSized(c, fi.blocks[newEndBlkIdx], int(newLengthBytes))
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
	fi.blocks[newEndBlkIdx] = *newFileKey
	fi.blocks = fi.blocks[:newEndBlkIdx+1]
	fi.lastBlockBytes = uint32(newLengthBytes)

	return nil
}
