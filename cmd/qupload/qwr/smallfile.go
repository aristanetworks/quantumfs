// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import (
	"os"

	"github.com/aristanetworks/quantumfs"
)

func smallFileWriter(qctx *quantumfs.Ctx, path string,
	finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, uint64, uint64, error) {

	file, oerr := os.Open(path)
	if oerr != nil {
		return quantumfs.ZeroKey, 0, 0, oerr
	}
	defer file.Close()

	keys, _, bytesWritten, err := writeFileBlocks(qctx, file,
		uint64(finfo.Size()), ds)
	if err != nil {
		return quantumfs.ZeroKey, 0, 0, err
	}

	fileKey := quantumfs.EmptyBlockKey
	// zero length files are small files which don't
	// have any blocks, hence no keys
	if len(keys) > 0 {
		fileKey = keys[0]
	}

	return fileKey, bytesWritten, 0, err
}
