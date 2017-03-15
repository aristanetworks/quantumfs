// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func init() {
	mbFileIOHandler := &fileObjIOHandler{
		writer: mbFileWriter,
	}

	registerFileObjIOHandler(quantumfs.ObjectTypeMediumFile,
		mbFileIOHandler)
	registerFileObjIOHandler(quantumfs.ObjectTypeLargeFile,
		mbFileIOHandler)
}

func mbFileBlocksWriter(file *os.File,
	readSize uint64,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	keys, lastBlockSize, err := writeFileBlocks(file, readSize, ds)
	if err != nil {
		return quantumfs.ObjectKey{}, err
	}

	mbf := quantumfs.NewMultiBlockFile(len(keys))
	mbf.SetBlockSize(uint32(quantumfs.MaxBlockSize))
	mbf.SetNumberOfBlocks(len(keys))
	mbf.SetListOfBlocks(keys)
	mbf.SetSizeOfLastBlock(lastBlockSize)

	mbfKey, mbfErr := writeBlob(mbf.Bytes(), quantumfs.KeyTypeMetadata, ds)
	if mbfErr != nil {
		return quantumfs.ObjectKey{}, err
	}

	return mbfKey, nil
}

// writes multi-block files of type Medium and Large
func mbFileWriter(file *os.File,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (*quantumfs.DirectoryRecord, *HardLinkInfo, error) {

	mbfKey, err := mbFileBlocksWriter(file, uint64(finfo.Size()), ds)
	if err != nil {
		return nil, nil, err
	}

	stat := finfo.Sys().(*syscall.Stat_t)
	dirRecord := createNewDirRecord(file.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(finfo.Size()),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		objType, mbfKey)

	return dirRecord, nil, err
}
