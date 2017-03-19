// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"

import "github.com/aristanetworks/quantumfs"

func symlinkFileWriter(path string,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	pointedTo, err := os.Readlink(path)
	if err != nil {
		return quantumfs.ZeroKey, err
	}
	return writeBlob([]byte(pointedTo), quantumfs.KeyTypeMetadata, ds)
}
