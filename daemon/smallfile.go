// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains small file types and methods

import "github.com/aristanetworks/quantumfs"
import "errors"
import "math"

type SmallFile struct {
	buf quantumfs.Buffer
}

func newSmallAccessor(c *ctx, size uint64, key quantumfs.ObjectKey) *SmallFile {
	defer c.FuncIn("newSmallAccessor", "size %d", size).out()
	var rtn SmallFile
	rtn.buf = c.dataStore.Get(&c.Ctx, key)
	if rtn.buf != nil {
		rtn.buf.SetSize(int(size))
	}

	return &rtn
}

func (fi *SmallFile) readBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
	error) {

	defer c.FuncIn("SmallFile::readBlock", "block %d offset %d", blockIdx,
		offset).out()

	// Sanity checks
	if offset >= uint64(quantumfs.MaxBlockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	// If we try to read too far, there's nothing to read here
	if blockIdx > 0 || offset > uint64(fi.buf.Size()) {
		return 0, nil
	}

	copied := fi.buf.Read(buf, uint32(offset))
	return copied, nil
}

func (fi *SmallFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	defer c.FuncIn("SmallFile::writeBlock", "block %d offset %d", blockIdx,
		offset).out()

	// Sanity checks
	if blockIdx > 0 {
		return 0, errors.New("BlockIdx must be zero for small files")
	}

	if int(offset) >= quantumfs.MaxBlockSize {
		return 0, errors.New("Offset exceeds small file")
	}

	copied := fi.buf.Write(&c.Ctx, buf, uint32(offset))
	if copied > 0 {
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data")
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *SmallFile) fileLength() uint64 {
	return uint64(fi.buf.Size())
}

func (fi *SmallFile) blockIdxInfo(c *ctx, absOffset uint64) (int, uint64) {
	defer c.FuncIn("SmallFile::blockIdxInfo", "offset %d", absOffset).out()

	blkIdx := absOffset / uint64(quantumfs.MaxBlockSize)
	remainingOffset := absOffset % uint64(quantumfs.MaxBlockSize)

	return int(blkIdx), remainingOffset
}

func (fi *SmallFile) sync(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("SmallFile::sync").out()

	// No metadata to marshal for small files
	key, err := fi.buf.Key(&c.Ctx)
	if err != nil {
		panic(err.Error())
	}
	return key
}

func (fi *SmallFile) getType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeSmallFile
}

func (fi *SmallFile) convertToMultiBlock(c *ctx,
	input MultiBlockFile) MultiBlockFile {

	defer c.funcIn("SmallFile::convertToMultiBlock").out()

	input.metadata.BlockSize = uint32(quantumfs.MaxBlockSize)

	numBlocks := int(math.Ceil(float64(fi.buf.Size()) /
		float64(input.metadata.BlockSize)))
	input.expandTo(numBlocks)
	dataInPrevBlocks := 0
	if numBlocks > 0 {
		c.dlog("Syncing smallFile dataBlock")
		input.toSync[0] = fi.buf
		dataInPrevBlocks = (numBlocks - 1) * int(input.metadata.BlockSize)
	}
	// last block (could be the only block) may be full or partial
	input.metadata.LastBlockBytes =
		uint32(fi.buf.Size() - dataInPrevBlocks)

	return input
}

func (fi *SmallFile) convertTo(c *ctx, newType quantumfs.ObjectType) blockAccessor {
	defer c.FuncIn("SmallFile::convertTo", "newType %d", newType).out()

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

func (fi *SmallFile) truncate(c *ctx, newLengthBytes uint64) error {
	defer c.FuncIn("SmallFile::truncate", "new size %d", newLengthBytes).out()
	fi.buf.SetSize(int(newLengthBytes))
	return nil
}
