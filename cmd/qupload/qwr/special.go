// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "encoding/binary"
import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func init() {
	splFileIOHandler := &fileObjIOHandler{
		writer: splFileWriter,
	}

	registerFileObjIOHandler(quantumfs.ObjectTypeSpecial,
		splFileIOHandler)
}

func splFileWriter(path string,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, error) {

	var hash [quantumfs.ObjectKeyLength - 1]byte

	// don't open special files
	stat := finfo.Sys().(*syscall.Stat_t)
	binary.LittleEndian.PutUint32(hash[0:4], stat.Mode)
	binary.LittleEndian.PutUint32(hash[4:8], uint32(stat.Rdev))

	key := quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded, hash)
	// all the information is embedded into key
	// there are no blobs to be written here

	dirRecord := createNewDirRecord(finfo.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(finfo.Size()),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		quantumfs.ObjectTypeSpecial, key)

	return dirRecord, nil
}
