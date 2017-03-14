// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "io"
import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

type HardLinkInfo struct {
	//TODO(krishna): setup fields

}

type fileObjWriter func(io.Reader, os.FileInfo,
	quantumfs.DataStore) (*quantumfs.DirectoryRecord, *HardLinkInfo, error)

type fileObjIOHandler struct {
	writer fileObjWriter
}

var fileObjIOHandlers map[quantumfs.ObjectType]*fileObjIOHandler

func registerFileObjIOHandler(objType quantumfs.ObjectType,
	handler *fileObjIOHandler) {

	fileObjIOHandlers[objType] = handler
}

func fileObjectType(finfo os.FileInfo) quantumfs.ObjectType {
	stat := finfo.Sys().(*syscall.Stat_t)

	switch {
	case stat.Nlink > 1:
		return quantumfs.ObjectTypeHardlink
	case utils.BitFlagsSet(uint(finfo.Mode()), uint(os.ModeSymlink)):
		return quantumfs.ObjectTypeSymlink
	case utils.BitFlagsSet(uint(finfo.Mode()), uint(os.ModeNamedPipe)) ||
		utils.BitFlagsSet(uint(finfo.Mode()), uint(os.ModeDevice)) ||
		utils.BitFlagsSet(uint(finfo.Mode()), uint(os.ModeSocket)):
		return quantumfs.ObjectTypeSpecial
	case uint64(finfo.Size()) <= quantumfs.MaxSmallFileSize():
		return quantumfs.ObjectTypeSmallFile
	case uint64(finfo.Size()) <= quantumfs.MaxMediumFileSize():
		return quantumfs.ObjectTypeMediumFile
	case uint64(finfo.Size()) <= quantumfs.MaxLargeFileSize():
		return quantumfs.ObjectTypeLargeFile
	}
	return quantumfs.ObjectTypeVeryLargeFile
}

func writer(objType quantumfs.ObjectType) (fileObjWriter, error) {
	handler, ok := fileObjIOHandlers[objType]
	if !ok {
		return nil, fmt.Errorf("Writer not found for file object type: %d\n",
			objType)
	}

	return handler.writer, nil
}

func WriteFile(ds quantumfs.DataStore,
	path string) (*quantumfs.DirectoryRecord, *HardLinkInfo, error) {

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("Open %s failed: %s\n",
			path, err)
	}
	defer file.Close()

	finfo, serr := os.Lstat(path)
	if serr != nil {
		return nil, nil, fmt.Errorf("Lstat %s failed: %s\n",
			path, err)
	}

	wr, wrerr := writer(fileObjectType(finfo))
	if wrerr != nil {
		return nil, nil, err
	}

	dirRecord, hlinkInfo, err := wr(file, finfo, ds)
	if err != nil {
		return nil, nil, fmt.Errorf("Writing %s failed: %s\n",
			path, err)
	}

	return dirRecord, hlinkInfo, nil
}
