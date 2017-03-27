// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"

import "github.com/aristanetworks/quantumfs"

func smallFileWriter(path string,
	finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	file, oerr := os.Open(path)
	if oerr != nil {
		return quantumfs.ZeroKey, oerr
	}
	defer file.Close()

	keys, _, err := writeFileBlocks(file, uint64(finfo.Size()), ds)
	if err != nil {
		return quantumfs.ZeroKey, err
	}

	fileKey := quantumfs.EmptyBlockKey
	// zero length files are small files which don't
	// have any blocks, hence no keys
	if len(keys) > 0 {
		fileKey = keys[0]
	}

	return fileKey, err
}
