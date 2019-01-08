// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package qwr

import (
	"os"

	"github.com/aristanetworks/quantumfs"
)

func symlinkFileWriter(qctx *quantumfs.Ctx, path string, finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, uint64, uint64, error) {

	pointedTo, err := os.Readlink(path)
	if err != nil {
		return quantumfs.ZeroKey, 0, 0, err
	}

	rtn, err := writeBlock(qctx, []byte(pointedTo),
		quantumfs.KeyTypeMetadata, ds)

	return rtn, 0, uint64(len(pointedTo)), err
}
