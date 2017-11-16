// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import (
	"os"
	"sync/atomic"

	"github.com/aristanetworks/quantumfs"
)

func mbFileBlocksWriter(qctx *quantumfs.Ctx, file *os.File,
	readSize uint64,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, uint64, error) {

	keys, lastBlockSize, bytesWritten, err := writeFileBlocks(qctx, file,
		readSize, ds)
	if err != nil {
		return quantumfs.ZeroKey, 0, err
	}

	mbf := quantumfs.NewMultiBlockFile(len(keys))
	mbf.SetBlockSize(uint32(quantumfs.MaxBlockSize))
	mbf.SetNumberOfBlocks(len(keys))
	mbf.SetListOfBlocks(keys)
	mbf.SetSizeOfLastBlock(lastBlockSize)

	mbfKey, mbfErr := writeBlock(qctx, mbf.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if mbfErr != nil {
		return quantumfs.ZeroKey, 0, err
	}
	atomic.AddUint64(&MetadataBytesWritten, uint64(len(mbf.Bytes())))

	return mbfKey, bytesWritten, nil
}

// writes multi-block files of type Medium and Large
func mbFileWriter(qctx *quantumfs.Ctx, path string,
	finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, uint64, error) {

	file, oerr := os.Open(path)
	if oerr != nil {
		return quantumfs.ZeroKey, 0, oerr
	}
	defer file.Close()

	return mbFileBlocksWriter(qctx, file, uint64(finfo.Size()), ds)
}
