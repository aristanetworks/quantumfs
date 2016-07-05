// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains very large file types and methods

import "github.com/aristanetworks/quantumfs"
import "encoding/json"

type VeryLargeFile struct {
	parts []LargeFile
}

type veryLargeStore struct {
	Keys []quantumfs.ObjectKey
}

// TODO: Increase this to 48000 when we switch away from json
const MaxParts = 22000

func newVeryLargeAccessor(c *ctx, key quantumfs.ObjectKey) *VeryLargeFile {
	var rtn VeryLargeFile

	buffer := c.dataStore.Get(&c.Ctx, key)
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

	numRead, err := fi.parts[partIdx].readBlock(c, blockIdxRem, offset, buf)

	// If this isn't the last block, ensure we read maximally
	if err == nil && partIdx < len(fi.parts)-1 {
		readQuota := uint32(len(buf))
		if fi.parts[partIdx].metadata.BlockSize < readQuota {
			readQuota = fi.parts[partIdx].metadata.BlockSize
		}

		if readQuota < uint32(numRead) {
			c.elog("Inconsistency, readBlock returned more than buf")
			panic("ReadBlock returned more than buf space")
		}
		padding := make([]byte, readQuota-uint32(numRead))
		copy(buf[numRead:], padding)
		numRead += len(padding)
	}

	return numRead, err
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
	for i := 0; i < len(fi.parts)-1; i++ {
		length += uint64(fi.parts[i].metadata.BlockSize) *
			uint64(quantumfs.MaxBlocksLargeFile)
	}

	// And add what's in the last block
	length += fi.parts[len(fi.parts)-1].fileLength()

	return length
}

func (fi *VeryLargeFile) blockIdxInfo(absOffset uint64) (int, uint64) {
	// Variable multiblock data block sizes makes this function harder

	for i := 0; i < len(fi.parts); i++ {
		maxLengthFile := uint64(fi.parts[i].metadata.BlockSize) *
			uint64(quantumfs.MaxBlocksLargeFile)

		// If this extends past the remaining offset, then this
		// is the file we're looking for
		if maxLengthFile > absOffset {
			blockIdx, offset := fi.parts[i].blockIdxInfo(absOffset)
			blockIdx += i * quantumfs.MaxBlocksLargeFile
			return blockIdx, offset
		}

		absOffset -= maxLengthFile
	}

	// If we're reached here, we've gone through all of the existing files.
	// New blocks will be quantumfs.MaxBlockSize, so use that info to calculate
	// our return values
	maxLengthFile := uint64(quantumfs.MaxBlockSize) *
		uint64(quantumfs.MaxBlocksLargeFile)
	for i := len(fi.parts); ; i++ {
		if maxLengthFile > absOffset {
			tmpLargeFile := newLargeShell()
			blockIdx, offset := tmpLargeFile.blockIdxInfo(absOffset)
			blockIdx += i * quantumfs.MaxBlocksLargeFile
			return blockIdx, offset
		}
		absOffset -= maxLengthFile
	}
}

func (fi *VeryLargeFile) sync(c *ctx) quantumfs.ObjectKey {
	var store veryLargeStore

	for i := 0; i < len(fi.parts); i++ {
		newKey := fi.parts[i].sync(c)

		store.Keys = append(store.Keys, newKey)
	}

	bytes, err := json.Marshal(store)
	if err != nil {
		panic("Unable to marshal very large file keys")
	}

	buffer := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)

	newFileKey, err := buffer.Key(&c.Ctx)
	if err != nil {
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
	if newLengthBytes == 0 {
		fi.parts = nil
		return nil
	}

	// If we're expanding the length, handle that first
	lastBlockIdx, lastBlockRem := fi.blockIdxInfo(newLengthBytes - 1)

	lastPartIdx := lastBlockIdx / quantumfs.MaxBlocksLargeFile
	newNumParts := lastPartIdx + 1

	if newNumParts > len(fi.parts) {
		fi.expandTo(newNumParts)
	} else {
		fi.parts = fi.parts[:newNumParts]
	}

	return fi.parts[lastPartIdx].truncate(c, lastBlockRem)
}
