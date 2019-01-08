// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package qwr

import (
	"os"

	"github.com/aristanetworks/quantumfs"
)

func vlFileWriter(qctx *quantumfs.Ctx, path string,
	finfo os.FileInfo,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, uint64, uint64, error) {

	var mbfKeys []quantumfs.ObjectKey

	file, oerr := os.Open(path)
	if oerr != nil {
		return quantumfs.ZeroKey, 0, 0, oerr
	}
	defer file.Close()

	parts := uint64(finfo.Size()) / quantumfs.MaxLargeFileSize()
	if uint64(finfo.Size())%quantumfs.MaxLargeFileSize() > 0 {
		parts++
	}

	remainingSize := uint64(finfo.Size())
	totalData := uint64(0)
	totalMetadata := uint64(0)
	for parts > 0 {
		var readSize uint64
		if remainingSize > quantumfs.MaxLargeFileSize() {
			readSize = quantumfs.MaxLargeFileSize()
		} else {
			readSize = remainingSize
		}

		mbfKey, dataWritten, metadataWritten, err := mbFileBlocksWriter(qctx,
			file, readSize, ds)
		if err != nil {
			return quantumfs.ZeroKey, 0, 0, err
		}
		totalData += dataWritten
		totalMetadata += metadataWritten
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
		return quantumfs.ZeroKey, 0, 0, vlfErr
	}
	totalMetadata += uint64(len(vlf.Bytes()))

	return vlfKey, totalData, totalMetadata, vlfErr
}
