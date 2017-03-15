// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

type fileObjWriter func(*os.File, os.FileInfo,
	quantumfs.ObjectType,
	quantumfs.DataStore) (*quantumfs.DirectoryRecord, *HardLinkInfo, error)

type fileObjIOHandler struct {
	writer fileObjWriter
}

var fileObjIOHandlers = make(map[quantumfs.ObjectType]*fileObjIOHandler)

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
	finfo os.FileInfo,
	path string) (*quantumfs.DirectoryRecord, *HardLinkInfo, error) {

	// process hardlinks first
	setHardLink := false
	stat := finfo.Sys().(*syscall.Stat_t)
	if finfo.Mode().IsRegular() && stat.Nlink > 1 {
		dirRecord, exists := HardLinkExists(finfo)
		if exists {
			// return a new thin record
			// representing the path for existing
			// hardlink
			return dirRecord, nil, nil
		}
		// this path is a hardlink
		// WriteFile should write the file and then
		// setup hardlink record
		setHardLink = true
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("Open %s failed: %s\n",
			path, err)
	}
	defer file.Close()

	objType := fileObjectType(finfo)
	wr, wrerr := writer(objType)
	if wrerr != nil {
		return nil, nil, err
	}

	dirRecord, hlinkInfo, err := wr(file, finfo, objType, ds)
	if err != nil {
		return nil, nil, fmt.Errorf("Writing %s failed: %s\n",
			path, err)
	}

	// write xattrs if any
	xattrsKey, xerr := WriteXAttrs(path, ds)
	if xerr != nil {
		return nil, nil, xerr
	}
	if !xattrsKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		dirRecord.SetExtendedAttributes(xattrsKey)
	}

	// initialize hardlink info from dirRecord
	if setHardLink {
		// new thin directory record is returned
		dirRecord = SetHardLink(dirRecord)
	}

	return dirRecord, hlinkInfo, nil
}

// caller ensures that file has at least readLen bytes without EOF
func writeFileBlocks(file *os.File, readLen uint64,
	ds quantumfs.DataStore) ([]quantumfs.ObjectKey, uint32, error) {

	var keys []quantumfs.ObjectKey

	// never attempt to read more than MaxBlobSize in each
	// iteration below. The backing array doesn't ever need
	// to increase beyond whats constructed here
	var chunk []byte
	if readLen > uint64(quantumfs.MaxBlockSize) {
		chunk = make([]byte, quantumfs.MaxBlockSize)
	} else {
		chunk = make([]byte, readLen)
	}

	for readLen > 0 {
		// ensures chunk is properly sized for the next read
		// while re-using the same backing array for the slice
		if readLen > uint64(quantumfs.MaxBlockSize) {
			chunk = chunk[:quantumfs.MaxBlockSize]
		} else {
			chunk = chunk[:readLen]
		}

		// this routine can be invoked multiple times for
		// same *os.File, the read continues from last read
		// offset
		bytesRead, err := file.Read(chunk)
		if err != nil {
			return nil, 0, err
		}
		if bytesRead != len(chunk) {
			return nil, 0, fmt.Errorf("Read %s failed due to partial read."+
				" ActualLen %d != ExpectedLen %d\n",
				file.Name(), bytesRead, len(chunk))
		}
		key, bErr := writeBlob(chunk, quantumfs.KeyTypeData, ds)
		if bErr != nil {
			return nil, 0, bErr
		}
		keys = append(keys, key)
		readLen -= uint64(len(chunk))
	}

	return keys, uint32(len(chunk)), nil
}
