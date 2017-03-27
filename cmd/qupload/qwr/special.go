// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "encoding/binary"
import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func specialFileWriter(path string, finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	var hash [quantumfs.ObjectKeyLength - 1]byte

	// don't open special files
	stat := finfo.Sys().(*syscall.Stat_t)
	binary.LittleEndian.PutUint32(hash[0:4], stat.Mode)
	binary.LittleEndian.PutUint32(hash[4:8], uint32(stat.Rdev))

	// all the information is embedded into key
	// there are no blobs to be written here
	return quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, hash),
		nil
}
