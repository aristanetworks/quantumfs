// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging shared memory support
import "os"
import "unsafe"
import "sync"
import "syscall"

// Circ buf size for approx 1 hour of LogEntries at 1000 a second
const mmapCircBufSize = 360000 * 24
// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024
// This must include the mmapHeader len
const mmapTotalSize = mmapCircBufSize + mmapStrMapSize + mmapHeaderSize

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
const mmapHeaderSize = 16
type mmapHeader struct {
	circBufSize	uint32
	strMapSize	uint32

	// These variables indicate the front and end of the circular buffer
	cirBufFrontIdx	uint32
	cirBufEndIdx	uint32
}

type SharedMemory struct {
	fd		*os.File
	buffer		*[mmapTotalSize]byte
	circBuf		CircMemLogs
	strIdMap	IdStrMap
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

const logStrSize = 32
const logTextMax = 30
type LogStr struct {
	text		[logTextMax]byte
	logSubsystem	uint8
	logLevel	uint8
}

func newLogStr(idx LogSubsystem, level uint8, format string) LogStr {
	var rtn LogStr
	rtn.logSubsystem = uint8(idx)
	rtn.logLevel = level
	copyLen := len(format)
	if copyLen > logTextMax {
		copyLen = logTextMax
	}
	copy(rtn.text[:], format[:copyLen])

	return rtn
}

type IdStrMap struct {
	data		map[string]uint32
	mapLock		sync.RWMutex

	buffer		*[mmapStrMapSize/logStrSize]LogStr
	freeIdx		uint32
}

func newCircBuf(buf []byte) CircMemLogs {
	return CircMemLogs {
		buffer:	buf,
	}
}

func newIdStrMap(buf *[mmapTotalSize]byte, offset int) IdStrMap {
	var rtn IdStrMap
	rtn.data = make(map[string]uint32)
	rtn.freeIdx = 0
	rtn.buffer = (*[mmapStrMapSize/
		logStrSize]LogStr)(unsafe.Pointer(&buf[offset]))

	return rtn
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

	mapFile, err := os.OpenFile(dir + "/" + filename,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
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

	var rtn SharedMemory
	rtn.fd = mapFile
	rtn.buffer = (*[mmapTotalSize]byte)(unsafe.Pointer(&mmap[0]))
	offset := mmapHeaderSize
	rtn.circBuf = newCircBuf(rtn.buffer[offset:offset+mmapCircBufSize])
	offset += mmapCircBufSize
	rtn.strIdMap = newIdStrMap(rtn.buffer, offset)

	return &rtn
}

func (strMap *IdStrMap) mapGetLogIdx(format string) *uint32 {

	strMap.mapLock.RLock()
	defer strMap.mapLock.RUnlock()

	entry, ok := strMap.data[format]
	if ok {
		return &entry
	}

	return nil
}

func (strMap *IdStrMap) writeMapEntry(idx LogSubsystem, level uint8,
	format string) uint32{

	strMap.mapLock.Lock()
	defer strMap.mapLock.Unlock()

	// Re-check for existing key now that we have the log to avoid races
	existingId, ok := strMap.data[format]
	if ok {
		return existingId
	}

	newIdx := strMap.freeIdx
	strMap.freeIdx++

	strMap.data[format] = newIdx
	strMap.buffer[newIdx] = newLogStr(idx, level, format)

	return newIdx
}

func (strMap *IdStrMap) fetchLogIdx(idx LogSubsystem, level uint8,
	format string) uint32 {

	existingId := strMap.mapGetLogIdx(format)

	if existingId != nil {
		return *existingId
	}

	return strMap.writeMapEntry(idx, level, format)
}

func generateLogEntry(strMapId uint32, reqId uint64, timestamp int64,
	args ...interface{}) []byte {

	return nil
}

func writeLogEntry(data []byte) {

}

func (mem *SharedMemory) logEntry(idx LogSubsystem, reqId uint64, level uint8,
	timestamp int64, format string, args ...interface{}) {

	// Create the string map entry / fetch existing one
	strId := mem.strIdMap.fetchLogIdx(idx, level, format)

	// Generate the byte array packet
	data := generateLogEntry(strId, reqId, timestamp, args...)

	// Write it into the shared memory carefully
	writeLogEntry(data)
}
