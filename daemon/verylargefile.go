// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains very large file types and methods

import "github.com/aristanetworks/quantumfs"
import "encoding/json"

type VeryLargeFile struct {
	parts		[]LargeFile
}

type veryLargeStore struct {
	Keys		[]quantumfs.ObjectKey
}

const MaxParts = 48000

func newVeryLargeAccessor(c *ctx, key quantumfs.ObjectKey) *VeryLargeFile {
	var rtn VeryLargeFile

	buffer := DataStore.Get(c, key)
	if buffer == nil {
		c.elog("Unable to fetch metadata for new vl file creation")
		panic("Unable to fetch metadata for new vl file creation")
	}

	var store veryLargeStore
	if err := json.Unmarshal(buffer.Get(), &store); err != nil {
		panic("Couldn't decode veryLargeStore object")
	}

	for i := 0; i < len(store.Keys); i++ {
		rtn.parts = append(rtn.parts, *newLargeAccessor(c, store.Keys[i]))
	}

	return &rtn
}

func newVeryLargeShell(file *LargeFile) *VeryLargeFile {
	var rtn VeryLargeFile
	rtn.parts = make([]LargeFile, 1)
	rtn.parts[0] = *file

	return &rtn
}

func (fi *VeryLargeFile) readBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	partIdx := blockIdx / quantumfs.MaxBlocksLargeFile
	blockIdxRem := blockIdx % quantumfs.MaxBlocksLargeFile

	// If we try to read too far, there's nothing to read here
	if partIdx >= len(fi.parts) {
		return 0, nil
	}

	return fi.parts[partIdx].readBlock(c, blockIdxRem, offset, buf)
}

func (fi *VeryLargeFile) expandTo(lengthParts int) {
	if lengthParts > MaxParts {
		panic("Invalid new length set to expandTo for file accessor")
	}

	newLength := make([]LargeFile, lengthParts-len(fi.parts))
	for i := 0; i < len(newLength); i++ {
		newLength[i] = newLargeShell() 
	}
	fi.parts = append(fi.parts, newLength...)
}

func (fi *VeryLargeFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	partIdx := blockIdx / quantumfs.MaxBlocksLargeFile
	blockIdxRem := blockIdx % quantumfs.MaxBlocksLargeFile

	// Ensure we have a part to write to
	for len(fi.parts) <= partIdx {
		fi.expandTo(partIdx + 1)
	}

	return fi.parts[partIdx].writeBlock(c, blockIdxRem, offset, buf)
}

func (fi *VeryLargeFile) fileLength() uint64 {
	var length uint64

	// Count everything except the last block as being full
	for i := 0; i < len(fi.parts) - 1; i++ {
		length += uint64(fi.parts[i].data.BlockSize *
			quantumfs.MaxBlocksLargeFile)
	}

	// And add what's in the last block
	length += fi.parts[len(fi.parts) - 1].fileLength()

	return length
}

func (fi *VeryLargeFile) blockIdxInfo(absOffset uint64) (int, uint64) {
	// Variable multiblock data block sizes makes this function harder

	for i := 0; i < len(fi.parts); i++ {
		maxLengthBlock := uint64(fi.parts[i].data.BlockSize *
			quantumfs.MaxBlocksLargeFile)

		// If this block extends past the remaining offset, then this
		// is the block we're looking for
		if maxLengthBlock > absOffset {
			return i, absOffset
		}

		absOffset -= maxLengthBlock
	}

	// If we're reached here, we've gone through all of the existing blocks.
	// New blocks will be quantumfs.MaxBlockSize, so use that info to calculate
	// our return values
	maxLengthBlock := uint64(quantumfs.MaxBlockSize *
		quantumfs.MaxBlocksLargeFile)
	i := len(fi.parts)
	for {
		if maxLengthBlock > absOffset {
			return i, absOffset
		}
		absOffset -= maxLengthBlock
	}
}

func (fi *VeryLargeFile) writeToStore(c *ctx) quantumfs.ObjectKey {
	var store veryLargeStore

	for i := 0; i < len(fi.parts); i++ {
		newKey := fi.parts[i].writeToStore(c)

		store.Keys = append(store.Keys, newKey)
	}

	bytes, err := json.Marshal(store)
	if err != nil {
		panic("Unable to marshal very large file keys")
	}

	var buffer quantumfs.Buffer
	buffer.Set(bytes)

	newFileKey := buffer.Key(quantumfs.KeyTypeMetadata)
	if err := c.durableStore.Set(newFileKey, &buffer); err != nil {
		panic("Failed to upload new very large file keys")
	}

	return newFileKey
}

func (fi *VeryLargeFile) getType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeVeryLargeFile
}

func (fi *VeryLargeFile) convertTo(c *ctx, newType quantumfs.ObjectType) blockAccessor {
	if newType <= quantumfs.ObjectTypeVeryLargeFile {
		return fi
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}

func (fi *VeryLargeFile) truncate(c *ctx, newLengthBytes uint64) error {

	// If we're expanding the length, handle that first
	lastBlockIdx, lastBlockRem := fi.blockIdxInfo(newLengthBytes)
	newNumBlocks := lastBlockIdx + 1

	if newNumBlocks > len(fi.parts) {
		fi.expandTo(newNumBlocks)
	} else {
		fi.parts = fi.parts[:newNumBlocks]
	}

	return fi.parts[lastBlockIdx].truncate(c, lastBlockRem)
}
