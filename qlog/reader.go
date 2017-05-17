// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// reader is a shared memory log parser for the qlog quantumfs subsystem
// It is used for tailing a qlog file and outputting it live

package qlog

import "fmt"
import "io"
import "os"
import "unsafe"


type Reader struct {
	qlogPath	string
}

func newReader(qlogFile string) *Reader {
	return &Reader {
		qlogPath:	qlogFile,
	}
}

func (read *Reader) ReadHeader() *MmapHeader {
	file, err := os.Open(read.qlogPath)
	if err != nil {
		panic(fmt.Sprintf("Unable to read from qlog file %s: %s",
			read.qlogPath, err))
	}

	headerLen := int(unsafe.Sizeof(MmapHeader{}))
	headerData := make([]byte, headerLen)
	_, err = io.ReadAtLeast(file, headerData, headerLen)
	if err != nil {
		panic(fmt.Sprintf("Unable to read header data from qlog file: %s",
			err))
	}

	return ExtractHeader(headerData)
}
