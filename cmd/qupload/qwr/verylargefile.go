// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func init() {
	vlFileIOHandler := &fileObjIOHandler{
		writer: vlFileWriter,
	}

	registerFileObjIOHandler(quantumfs.ObjectTypeVeryLargeFile,
		vlFileIOHandler)
}

func vlFileWriter(file *os.File,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, *HardLinkInfo, error) {

	var mbfKeys []quantumfs.ObjectKey

	parts := uint64(finfo.Size()) / quantumfs.MaxLargeFileSize()
	if uint64(finfo.Size())%quantumfs.MaxLargeFileSize() > 0 {
		parts++
	}

	remainingSize := uint64(finfo.Size())
	for parts > 0 {
		var readSize uint64
		if remainingSize > quantumfs.MaxLargeFileSize() {
			readSize = quantumfs.MaxLargeFileSize()
		} else {
			readSize = remainingSize
		}

		mbfKey, err := mbFileBlocksWriter(file, readSize, ds)
		if err != nil {
			return nil, nil, err
		}
		mbfKeys = append(mbfKeys, mbfKey)
		remainingSize -= readSize

		parts--
	}

	vlf := quantumfs.NewVeryLargeFile()
	vlf.SetNumberOfParts(len(mbfKeys))
	for i := 0; i < len(mbfKeys); i++ {
		vlf.SetLargeFileKey(i, mbfKeys[i])
	}

	vlfKey, vlfErr := writeBlob(vlf.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if vlfErr != nil {
		return nil, nil, vlfErr
	}

	stat := finfo.Sys().(*syscall.Stat_t)
	dirRecord := createNewDirRecord(file.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(finfo.Size()),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		quantumfs.ObjectTypeVeryLargeFile, vlfKey)

	return dirRecord, nil, nil
}
