// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "os"
import "sync/atomic"

import "github.com/aristanetworks/quantumfs"

func vlFileWriter(path string,
	finfo os.FileInfo,
	objType quantumfs.ObjectType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	var mbfKeys []quantumfs.ObjectKey

	file, oerr := os.Open(path)
	if oerr != nil {
		return quantumfs.ZeroKey, oerr
	}
	defer file.Close()

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
			return quantumfs.ZeroKey, err
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
		return quantumfs.ZeroKey, vlfErr
	}
	atomic.AddUint64(&MetadataBytesWritten, uint64(len(vlf.Bytes())))
	return vlfKey, vlfErr
}
