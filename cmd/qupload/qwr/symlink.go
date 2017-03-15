// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func init() {
	symlinkIOHandler := &fileObjIOHandler{
		writer: symlinkWriter,
	}

	registerFileObjIOHandler(quantumfs.ObjectTypeSymlink,
		symlinkIOHandler)
}

func symlinkWriter(path string,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, error) {

	pointedTo, err := os.Readlink(path)
	if err != nil {
		return nil, err
	}

	data := []byte(pointedTo)
	key, bErr := writeBlob(data, quantumfs.KeyTypeMetadata, ds)
	if bErr != nil {
		return nil, bErr
	}

	stat := finfo.Sys().(*syscall.Stat_t)
	dirRecord := createNewDirRecord(finfo.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(len(data)),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		quantumfs.ObjectTypeSymlink, key)

	return dirRecord, nil
}
