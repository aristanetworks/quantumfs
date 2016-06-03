// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This contains medium file types and methods

type MediumFile struct {
	blockSize	uint32
	numBlocks	uint32
	lastBlockBytes	uint32
	blocks		[]ObjectKey
}

func (fi *MediumFile) writeBlock(c *ctx, blockIdx int, offset int,
	buf []byte) (*quantumfs.ObjectKey, int, error) {

	// Sanity checks
	if blockIdx > quantumfs.MaxBlocksMediumFile {
		return nil, 0, errors.New("BlockIdx exceeds bounds for medium file")
	}

	if offset >= fi.blockSize {
		return nil, 0, errors.New("Attempt to write past end of block")
	}

	// Ensure we expand the file to fit the blockIdx
	if blockIdx >= fi.numBlocks {
		newLength = make([]ObjectKey, blockIdx+1 - fi.numBlocks)
		for i := 0; i < len(newLength); i++ {
			newLength[i] = quantumfs.EmptyBlockKey
		}
		fi.numBlocks = blockIdx+1
		blocks = append(blocks, newLength...)
	}

	// Calculate how long this block is supposed to be
	targetSize = (fi.blockSize * (fi.numBlocks-1)) + lastBlockBytes

	// Grab the data
	data := fetchData(c, fi.blocks[blockIdx], targetSize)
	if data == nil {
		c.elog("Unable to fetch data for block")
		return nil, 0, errors.New("Unable to fetch block data")
	}

	if offset > len(data.Get()) {
		c.elog("Writing past the end of file is not supported yet")
		return nil, 0, errors.New("Writing past the end of file " +
			"is not supported yet")
	}

	copied := data.Write(buf, uint32(offset))
	if copied > 0 {
		var newFileKey *quantumfs.ObjectKey
		newFileKey, err = pushData(c, data)
		if err != nil {
			c.elog("Write failed")
			return nil, 0, errors.New("Unable to write to block")
		}
		return newFileKey, copied, nil
	}

	c.elog("writeBlock attempt with zero data")
	return nil, 0, errors.New("writeBlock attempt with zero data")
}
