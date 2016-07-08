// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging shared memory support
import "os"
import "unsafe"
import "syscall"

// Circ buf size for approx 1 hour of LogEntries at 1000 a second
const mmapCircBufSize = 360000 * 24
// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024
// This must include the mmapHeader len
const mmapTotalSize = mmapCircBufSize + mmapStrMapSize + 8

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
type mmapHeader struct {
	circBufSize	uint32
	strMapSize	uint32
}

type SharedMemory struct {
	fd	*os.File
	buffer	*[mmapTotalSize]byte
}

// Average Log Entry should be ~24 bytes
type LogEntry struct {
	strIdx		uint16
	reqId		uint64
	timestamp	int64
	vars		[]interface{}
}

type CircMemLogs struct {
	buffer []byte
}

type LogStr struct {
	text		[30]byte
	logSubsystem	uint8
	logLevel	uint8
}

type IdStrMap struct {
	data	[]LogStr
}

func newSharedMemory(dir string, filename string) *SharedMemory {

	if dir == "" || filename == "" {
		return nil
	}

	// Create a file and its path to be mmap'd
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		panic("Unable to ensure shared memory log file path exists")
	}

	mapFile, err := os.Create(dir + "/" + filename)
	if mapFile == nil || err != nil {
		panic("Unable to create shared memory log file")
	}

	// Size the file to fit the shared memory requirements
	_, err = mapFile.Seek(mmapTotalSize-1, 0)
	if err != nil {
		panic("Unable to seek to shared memory end in file")
	}

	_, err = mapFile.Write([]byte(" "))
	if err != nil {
		panic("Unable to expand file to fit shared memory requirement")
	}

	// Map the file to memory
	mmap, err := syscall.Mmap(int(mapFile.Fd()), 0, mmapTotalSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)

	if err != nil {
		panic("Unable to map shared memory file for logging")
	}

	rtn := SharedMemory {
		fd:		mapFile,
		buffer:		(*[mmapTotalSize]byte)(unsafe.Pointer(&mmap[0])),
	}

	return &rtn
}

