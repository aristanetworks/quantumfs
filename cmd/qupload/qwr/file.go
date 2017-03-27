// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "os"
import "sync/atomic"
import "syscall"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

var DataBytesWritten uint64
var MetadataBytesWritten uint64

type fileObjectWriter func(string, os.FileInfo,
	quantumfs.DataStore) (quantumfs.ObjectKey, error)

func fileObjectInfo(path string,
	finfo os.FileInfo) (quantumfs.ObjectType, fileObjectWriter, error) {

	mode := uint(finfo.Mode())
	size := uint64(finfo.Size())

	switch {
	case utils.BitFlagsSet(mode, uint(os.ModeSymlink)):
		return quantumfs.ObjectTypeSymlink, symlinkFileWriter, nil
	case utils.BitFlagsSet(mode, uint(os.ModeNamedPipe)) ||
		utils.BitFlagsSet(mode, uint(os.ModeDevice)) ||
		utils.BitFlagsSet(mode, uint(os.ModeSocket)):
		return quantumfs.ObjectTypeSpecial, specialFileWriter, nil
	case size <= quantumfs.MaxSmallFileSize():
		return quantumfs.ObjectTypeSmallFile, smallFileWriter, nil
	case size <= quantumfs.MaxMediumFileSize():
		return quantumfs.ObjectTypeMediumFile, mbFileWriter, nil
	case size <= quantumfs.MaxLargeFileSize():
		return quantumfs.ObjectTypeLargeFile, mbFileWriter, nil
	case size <= quantumfs.MaxVeryLargeFileSize():
		return quantumfs.ObjectTypeVeryLargeFile, vlFileWriter, nil
	}
	return quantumfs.ObjectTypeInvalid, nil,
		fmt.Errorf("Unsupported file object type for %q", path)
}

func WriteFile(ds quantumfs.DataStore,
	finfo os.FileInfo,
	path string) (quantumfs.DirectoryRecord, error) {

	stat := finfo.Sys().(*syscall.Stat_t)

	// process hardlink first since we can
	// skip the content write if the hardlink
	// content already exists
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
		// only one thread will write the file
		setHardLink = true
	}

	// detect object type specific writer
	objType, objWriter, err := fileObjectInfo(path, finfo)
	if err != nil {
		return nil, fmt.Errorf("WriteFile object type detect "+
			"failed: %v", err)
	}

	// use writer to write file blocks and file type
	// specific metadata
	fileKey, werr := objWriter(path, finfo, ds)
	if werr != nil {
		return nil, fmt.Errorf("WriteFile object writer %d "+
			"for %q failed: %v\n",
			objType, path, werr)
	}

	dirRecord := CreateNewDirRecord(finfo.Name(), stat.Mode,
		uint32(stat.Rdev), uint64(finfo.Size()),
		quantumfs.ObjectUid(stat.Uid, stat.Uid),
		quantumfs.ObjectGid(stat.Gid, stat.Gid),
		objType,
		// retain times from input files to maintain same blob
		// content for repeated writes
		quantumfs.NewTime(time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)),
		quantumfs.NewTime(time.Unix(stat.Ctim.Sec, stat.Ctim.Nsec)),
		fileKey)

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
	// directory record based on file content
	if setHardLink {
		// returned dir record
		dirRecord = SetHardLink(finfo,
			dirRecord.(*quantumfs.DirectRecord))
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
			return nil, 0,
				fmt.Errorf("writeFileBlocks: Read %s failed "+
					"due to partial read. "+
					"Actual %d != Expected %d\n",
					file.Name(), bytesRead, len(chunk))
		}
		key, bErr := writeBlob(chunk, quantumfs.KeyTypeData, ds)
		if bErr != nil {
			return nil, 0, bErr
		}
		atomic.AddUint64(&DataBytesWritten, uint64(len(chunk)))
		keys = append(keys, key)
		readLen -= uint64(len(chunk))
	}

	return keys, uint32(len(chunk)), nil
}
