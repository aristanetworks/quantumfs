// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import (
	"os"
	"sync/atomic"

	"github.com/aristanetworks/quantumfs"
)

func vlFileWriter(qctx *quantumfs.Ctx, path string,
	finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, uint64, error) {

	var mbfKeys []quantumfs.ObjectKey

	file, oerr := os.Open(path)
	if oerr != nil {
		return quantumfs.ZeroKey, 0, oerr
	}
	defer file.Close()

	parts := uint64(finfo.Size()) / quantumfs.MaxLargeFileSize()
	if uint64(finfo.Size())%quantumfs.MaxLargeFileSize() > 0 {
		parts++
	}

	remainingSize := uint64(finfo.Size())
	totalWritten := uint64(0)
	for parts > 0 {
		var readSize uint64
		if remainingSize > quantumfs.MaxLargeFileSize() {
			readSize = quantumfs.MaxLargeFileSize()
		} else {
			readSize = remainingSize
		}

		mbfKey, bytesWritten, err := mbFileBlocksWriter(qctx, file, readSize,
			ds)
		if err != nil {
			return quantumfs.ZeroKey, 0, err
		}
		totalWritten += bytesWritten
		mbfKeys = append(mbfKeys, mbfKey)
		remainingSize -= readSize

		parts--
	}

	_, vlf := quantumfs.NewVeryLargeFile(len(mbfKeys))
	vlf.SetNumberOfParts(len(mbfKeys))
	for i := 0; i < len(mbfKeys); i++ {
		vlf.SetLargeFileKey(i, mbfKeys[i])
	}

	vlfKey, vlfErr := writeBlock(qctx, vlf.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if vlfErr != nil {
		return quantumfs.ZeroKey, 0, vlfErr
	}
	atomic.AddUint64(&MetadataBytesWritten, uint64(len(vlf.Bytes())))
	return vlfKey, totalWritten, vlfErr
}
