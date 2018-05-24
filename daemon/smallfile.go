// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains small file types and methods

import (
	"errors"
	"math"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

type SmallFile struct {
	key  quantumfs.ObjectKey
	size int
	buf  quantumfs.Buffer
}

func newSmallAccessor(c *ctx, size uint64, key quantumfs.ObjectKey) *SmallFile {
	defer c.FuncIn("newSmallAccessor", "size %d", size).Out()

	return &SmallFile{
		key:  key,
		size: int(size),
		buf:  nil,
	}
}

func (fi *SmallFile) getBuffer(c *ctx) ImmutableBuffer {
	if fi.buf != nil {
		return fi.buf
	}

	buf := c.dataStore.Get(&c.Ctx, fi.key)
	utils.Assert(buf != nil, "Unable to get file buffer")

	if buf.Size() != fi.size {
		c.elog("getBuffer forced to be resized")
		mutable := MutableCopy(c, buf)
		mutable.SetSize(fi.size)
		fi.buf = mutable
		return fi.buf
	}
	return buf
}

func (fi *SmallFile) getBufferToDirty(c *ctx) quantumfs.Buffer {
	if fi.buf != nil {
		return fi.buf
	}

	buf := c.dataStore.Get(&c.Ctx, fi.key)
	utils.Assert(buf != nil, "Unable to get file buffer")

	fi.buf = MutableCopy(c, buf)
	if fi.buf.Size() != fi.size {
		fi.buf.SetSize(fi.size)
	}

	return fi.buf
}

func (fi *SmallFile) readBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
	error) {

	defer c.FuncIn("SmallFile::readBlock", "block %d offset %d", blockIdx,
		offset).Out()

	// Sanity checks
	if offset >= uint64(quantumfs.MaxBlockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	if blockIdx > 0 {
		return 0, nil
	}

	backingData := fi.getBuffer(c)
	// If we try to read too far, there's nothing to read here
	if offset > uint64(backingData.Size()) {
		return 0, nil
	}

	copied := backingData.Read(buf, uint32(offset))
	return copied, nil
}

func (fi *SmallFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	defer c.FuncIn("SmallFile::writeBlock", "block %d offset %d", blockIdx,
		offset).Out()

	// Sanity checks
	if blockIdx > 0 {
		return 0, errors.New("BlockIdx must be zero for small files")
	}

	if int(offset) >= quantumfs.MaxBlockSize {
		return 0, errors.New("Offset exceeds small file")
	}

	copied := fi.getBufferToDirty(c).Write(&c.Ctx, buf, uint32(offset))
	if copied > 0 {
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data")
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *SmallFile) fileLength(c *ctx) uint64 {
	return uint64(fi.getBuffer(c).Size())
}

func (fi *SmallFile) blockIdxInfo(c *ctx, absOffset uint64) (int, uint64) {
	defer c.FuncIn("SmallFile::blockIdxInfo", "offset %d", absOffset).Out()

	blkIdx := absOffset / uint64(quantumfs.MaxBlockSize)
	remainingOffset := absOffset % uint64(quantumfs.MaxBlockSize)

	return int(blkIdx), remainingOffset
}

func (fi *SmallFile) sync(c *ctx, pub publishFn) quantumfs.ObjectKey {
	defer c.funcIn("SmallFile::sync").Out()

	if fi.buf == nil {
		// Fully synced already
		return fi.key
	}

	// No metadata to marshal for small files
	buf := fi.buf
	fi.buf = nil
	key, err := pub(c, buf)
	if err != nil {
		panic(err.Error())
	}

	// Now that we've flushed our data to the datastore, drop our local buffer
	fi.key = key
	fi.size = buf.Size()

	return key
}

func (fi *SmallFile) reload(c *ctx, key quantumfs.ObjectKey) {
	defer c.funcIn("SmallFile::reload").Out()
	fi.key = key
	fi.buf = MutableCopy(c, c.dataStore.Get(&c.Ctx, fi.key))
	if fi.buf == nil {
		panic(key.String())
	}
	fi.size = fi.buf.Size()
}

func (fi *SmallFile) getType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeSmallFile
}

func (fi *SmallFile) convertToMultiBlock(c *ctx,
	input MultiBlockFile) MultiBlockFile {

	defer c.funcIn("SmallFile::convertToMultiBlock").Out()

	input.metadata.BlockSize = uint32(quantumfs.MaxBlockSize)

	numBlocks := int(math.Ceil(float64(fi.getBuffer(c).Size()) /
		float64(input.metadata.BlockSize)))
	input.expandTo(numBlocks)
	dataInPrevBlocks := 0
	if numBlocks > 0 {
		c.vlog("Syncing smallFile dataBlock")
		input.toSync[0] = fi.getBufferToDirty(c)
		dataInPrevBlocks = (numBlocks - 1) * int(input.metadata.BlockSize)
	}
	// last block (could be the only block) may be full or partial
	input.metadata.LastBlockBytes =
		uint32(fi.getBuffer(c).Size() - dataInPrevBlocks)

	return input
}

func (fi *SmallFile) convertTo(c *ctx, newType quantumfs.ObjectType) blockAccessor {
	defer c.FuncIn("SmallFile::convertTo", "newType %d", newType).Out()

	if newType == quantumfs.ObjectTypeSmallFile {
		return fi
	}

	if newType == quantumfs.ObjectTypeMediumFile {
		rtn := newMediumShell()

		rtn.MultiBlockFile = fi.convertToMultiBlock(c, rtn.MultiBlockFile)
		return &rtn
	}

	if newType == quantumfs.ObjectTypeLargeFile ||
		newType == quantumfs.ObjectTypeVeryLargeFile {

		lrg := newLargeShell()

		lrg.MultiBlockFile = fi.convertToMultiBlock(c, lrg.MultiBlockFile)
		if newType == quantumfs.ObjectTypeVeryLargeFile {
			return newVeryLargeShell(&lrg)
		}
		return &lrg
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}

func (fi *SmallFile) truncate(c *ctx, newLengthBytes uint64) fuse.Status {
	defer c.FuncIn("SmallFile::truncate", "new size %d", newLengthBytes).Out()
	fi.getBufferToDirty(c).SetSize(int(newLengthBytes))
	return fuse.OK
}
