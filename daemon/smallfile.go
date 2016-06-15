// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains small file types and methods

import "arista.com/quantumfs"
import "errors"
import "math"

type SmallFile struct {
	key quantumfs.ObjectKey
	bytes uint64
}

func newSmallAccessor(c *ctx, size uint64, key quantumfs.ObjectKey) *SmallFile {
	var rtn SmallFile
	rtn.key = key
	rtn.bytes = size

	return &rtn
}

func (fi *SmallFile) ReadBlock(c *ctx, blockIdx int, offset uint64, buf []byte) (int,
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

func (fi *SmallFile) WriteBlock(c *ctx, blockIdx int, offset uint64,
	buf []byte) (int, error) {

	// Sanity checks
	if blockIdx > 0 {
		return 0, errors.New("BlockIdx must be zero for small files")
	}

	if uint32(offset) + uint32(len(buf)) >= quantumfs.MaxBlockSize {
		return 0, errors.New("Offset and write amount exceeds small file")
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

func (fi *SmallFile) GetFileLength() uint64 {
	return uint64(fi.bytes)
}

func (fi *SmallFile) GetBlockLength() uint64 {
	return quantumfs.MaxBlockSize
}

func (fi *SmallFile) WriteToStore(c *ctx) quantumfs.ObjectKey {
	// No metadata to marshal for small files
	return fi.key
}

func (fi *SmallFile) GetType() quantumfs.ObjectType {
	return quantumfs.ObjectTypeSmallFile
}

func (fi *SmallFile) ConvertTo(c *ctx, newType quantumfs.ObjectType) BlockAccessor {
	if newType == quantumfs.ObjectTypeMediumFile {
		var rtn MediumFile
		rtn.blockSize = quantumfs.MaxBlockSize

		numBlocks := int(math.Ceil(float64(fi.bytes) /
			float64(rtn.blockSize)))
		rtn.blocks = make([]quantumfs.ObjectKey, numBlocks)
		rtn.blocks[0] = fi.key
		rtn.lastBlockBytes = uint32(fi.bytes % uint64(rtn.blockSize))

		return &rtn
	}

	c.elog("Unable to convert file accessor to type %d", newType)
	return nil
}

func (fi *SmallFile) Truncate(c *ctx, newLengthBytes uint64) error {

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
