// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains small file types and methods

import "arista.com/quantumfs"
import "errors"
import "math"

type SmallFile struct {
	key   quantumfs.ObjectKey
	bytes uint64
}

func newSmallAccessor(c *ctx, size uint64, key quantumfs.ObjectKey) *SmallFile {
	var rtn SmallFile
	rtn.key = key
	rtn.bytes = size

	return &rtn
}

func (fi *SmallFile) readBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
	error) {

	// Sanity checks
	if offset >= uint64(quantumfs.MaxBlockSize) {
		return 0, errors.New("Attempt to read past end of block")
	}

	// If we try to read too far, there's nothing to read here
	if blockIdx > 0 {
		return 0, nil
	}

	expectedSize := fi.bytes
	data := fetchDataSized(c, fi.key, int(expectedSize))

	copied := copy(buf, data.Get()[offset:])
	return copied, nil
}

func (fi *SmallFile) writeBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > 0 {
		return 0, errors.New("BlockIdx must be zero for small files")
	}

	if uint32(offset) >= quantumfs.MaxBlockSize {
		return 0, errors.New("Offset exceeds small file")
	}

	// Grab the data
	data := DataStore.Get(c, fi.key)
	if data == nil {
		c.elog("Unable to fetch data for block")
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
		fi.bytes = uint64(len(data.Get()))
		fi.key = *newFileKey
		return int(copied), nil
	}

	c.elog("writeBlock attempt with zero data")
	return 0, errors.New("writeBlock attempt with zero data")
}

func (fi *SmallFile) fileLength() uint64 {
	return uint64(fi.bytes)
}

func (fi *SmallFile) blockIdxInfo(absOffset uint64) (int, uint64) {
	blkIdx := absOffset / quantumfs.MaxBlockSize
	remainingOffset := absOffset % quantumfs.MaxBlockSize

	return int(blkIdx), remainingOffset
}

func (fi *SmallFile) writeToStore(c *ctx) quantumfs.ObjectKey {
	// No metadata to marshal for small files
	return fi.key
}

func (fi *SmallFile) getType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeSmallFile
}

func (fi *SmallFile) convertToMultiBlock(input MultiBlockFile) MultiBlockFile {
	input.data.BlockSize = quantumfs.MaxBlockSize

	numBlocks := int(math.Ceil(float64(fi.bytes) /
		float64(input.data.BlockSize)))
	input.expandTo(numBlocks)
	if numBlocks > 0 {
		input.data.Blocks[0] = fi.key
	}
	input.data.LastBlockBytes = uint32(fi.bytes %
		uint64(input.data.BlockSize))

	return input
}

func (fi *SmallFile) convertTo(c *ctx, newType quantumfs.ObjectType) blockAccessor {
	if newType == quantumfs.ObjectTypeSmallFile {
		return fi
	}

	if newType == quantumfs.ObjectTypeMediumFile {
		rtn := newMediumShell()

		rtn.MultiBlockFile = fi.convertToMultiBlock(rtn.MultiBlockFile)
		return &rtn
	}

	if newType == quantumfs.ObjectTypeLargeFile {
		rtn := newLargeShell()

		rtn.MultiBlockFile = fi.convertToMultiBlock(rtn.MultiBlockFile)
		return &rtn
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}

func (fi *SmallFile) truncate(c *ctx, newLengthBytes uint64) error {

	// If we're increasing the length, then we can just update
	if newLengthBytes > fi.bytes {
		fi.bytes = newLengthBytes
		return nil
	}

	data := fetchDataSized(c, fi.key, int(newLengthBytes))
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
	fi.key = *newFileKey
	fi.bytes = newLengthBytes
	return nil
}
