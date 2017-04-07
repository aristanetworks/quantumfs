// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"

import "github.com/aristanetworks/quantumfs"

func symlinkFileWriter(qctx *quantumfs.Ctx, path string, finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	pointedTo, err := os.Readlink(path)
	if err != nil {
		return quantumfs.ZeroKey, err
	}
	return writeBlock(qctx, []byte(pointedTo),
		quantumfs.KeyTypeMetadata, ds)
}
