// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func init() {
	smallFileIOHandler := &fileObjIOHandler{
		writer: smallFileWriter,
	}

	registerFileObjIOHandler(quantumfs.ObjectTypeSmallFile,
		smallFileIOHandler)
}

func smallFileWriter(path string,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, error) {

	file, oerr := os.Open(path)
	if oerr != nil {
		return nil, oerr
	}
	defer file.Close()

	keys, _, err := writeFileBlocks(file, uint64(finfo.Size()), ds)
	if err != nil {
		return nil, err
	}

	stat := finfo.Sys().(*syscall.Stat_t)
	dirRecord := createNewDirRecord(finfo.Name(),
		stat.Mode, uint32(stat.Rdev), uint64(stat.Size),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Uid, stat.Uid),
		objType, keys[0])

	return dirRecord, nil
}
