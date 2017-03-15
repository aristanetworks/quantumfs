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

	registerFileObjIOHandler(quantumfs.ObjectTypeSpecial,
		symlinkIOHandler)
}

func symlinkWriter(file *os.File,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, *HardLinkInfo, error) {

	pointedTo, err := os.Readlink(file.Name())
	if err != nil {
		return nil, nil, err
	}

	key, bErr := writeBlob([]byte(pointedTo),
		quantumfs.KeyTypeMetadata, ds)
	if bErr != nil {
		return nil, nil, bErr
	}

	stat := finfo.Sys().(*syscall.Stat_t)
	dirRecord := createNewDirRecord(file.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(finfo.Size()),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		quantumfs.ObjectTypeSymlink, key)

	return dirRecord, nil, nil
}
