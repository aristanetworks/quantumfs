// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "os"
import "syscall"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

type fileObjWriter func(string, os.FileInfo,
	quantumfs.ObjectType,
	quantumfs.DataStore) (*quantumfs.DirectoryRecord, error)

type fileObjIOHandler struct {
	writer fileObjWriter
}

var fileObjIOHandlers = make(map[quantumfs.ObjectType]*fileObjIOHandler)

func registerFileObjIOHandler(objType quantumfs.ObjectType,
	handler *fileObjIOHandler) {

	fileObjIOHandlers[objType] = handler
}

func fileObjectType(finfo os.FileInfo) quantumfs.ObjectType {
	switch {
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
	path string) (*quantumfs.DirectoryRecord, error) {

	stat := finfo.Sys().(*syscall.Stat_t)

	// process hardlink first
	setHardLink := false
	if finfo.Mode().IsRegular() && stat.Nlink > 1 {
		dirRecord, exists := HardLink(finfo)
		if exists {
			// return a new thin record
			// representing the path for existing
			// hardlink
			return dirRecord, nil
		}
		// this path is a hardlink
		// WriteFile should write the file and then
		// setup hardlink record
		setHardLink = true
	}

	// detect object type specific writer
	objType := fileObjectType(finfo)
	wr, wrerr := writer(objType)
	if wrerr != nil {
		return nil, wrerr
	}

	// use writer to write file blocks and file type
	// specific metadata
	dirRecord, werr := wr(path, finfo, objType, ds)
	if werr != nil {
		return nil, fmt.Errorf("Writing %s failed: %s\n",
			path, werr)
	}

	// TODO(krishna): create DirectoryRecords here
	// DirectoryRecord is setup in object type specific
	// writer to allow object type specific mods if needed
	// So far no such scenario has been seen

	// write xattrs if any
	xattrsKey, xerr := WriteXAttrs(path, ds)
	if xerr != nil {
		return nil, xerr
	}
	if !xattrsKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		dirRecord.SetExtendedAttributes(xattrsKey)
	}

	// initialize hardlink info from dirRecord
	// must be last step as it needs a fully setup
	// directory record
	if setHardLink {
		// instead of typical directory record use
		// a directory record which points to the
		// hardlink info
		dirRecord = SetHardLink(finfo, dirRecord)
	}

	return dirRecord, nil
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
