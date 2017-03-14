// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "io"
import "io/ioutil"
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

func smallFileWriter(file io.Reader,
	finfo os.FileInfo,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, *HardLinkInfo, error) {

	stat := finfo.Sys().(*syscall.Stat_t)

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}

	key, berr := writeBlob(data, quantumfs.KeyTypeData, ds)
	if berr != nil {
		return nil, nil, berr
	}

	dirRecord := createNewDirRecord(finfo.Name(),
		stat.Mode, uint32(stat.Rdev), uint64(stat.Size),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Uid, stat.Uid),
		quantumfs.ObjectTypeSmallFile,
		key)

	return dirRecord, nil, nil
}
