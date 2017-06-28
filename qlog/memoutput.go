// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all logging shared memory support

import (
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// We need a static array sized upper bound on our memory. Increase this as needed.
const DefaultMmapSize = (360000 * 24) + mmapStrMapSize + unsafe.Sizeof(MmapHeader{})

// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
const QlogVersion = 4

// We use the upper-most bit of the length field to indicate the packet is ready,
// so the max packet length is 7 bits long
const MaxPacketLen = 32767

type MmapHeader struct {
	DaemonVersion [128]byte
	Version       uint32
	StrMapSize    uint32
	CircBuf       circBufHeader
}

type circBufHeader struct {
	Size uint64

	// This marks one past the end of the circular buffer
	PastEndIdx uint64
}

type SharedMemory struct {
	fd       *os.File
	mapSize  int
	circBuf  CircMemLogs
	strIdMap IdStrMap
	buffer   []byte

	// This is dangerous as Qlog also owns SharedMemory. SharedMemory must
	// ensure that any call it makes to Qlog doesn't result in infinite recursion
	errOut *Qlog

	// For testing only
	testDropStr string
	testMode    bool
}

type LogEntry struct {
	strIdx    uint16
	reqId     uint64
	timestamp int64
	vars      []interface{}
}

/*
The circular memory object contains a byte array that it writes data to in a
circular fashion. It is designed to be written to and read from at the same time
without any locks. To do so requires that readers and the writer adhere to the
following rules...

Writer:
- The CircMemLogs must only have one writer at a time, and must modify the front
	pointer if it needs to make space before writing any data. It needs to update
	the end pointer *after* all data is written, to prevent any reader from
	reading invalid data in a race

Readers:
- Must first copy the end pointer
- Must then copy the circular buffer in its entirety
- Must then copy the front pointer
- Must then only consider the data in the buffer between front (sampled after) and
	end (sampled before) as valid.
- Since the buffer is being written to, the pointers are constantly updated and data
	is constantly changing. If we read the end pointer *after* we've copied the
	buffer, it's possible that the end points to a further place than our copy
	of the buffer has been updated to. That's why we need to sample the end
	pointer first.
	If we read the front pointer *before* we've copied the data, it's possible
	that the packets near our front pointer were removed by the time that we got
	around to copying that section of the buffer. That's why we sample the front
	afterwards, since it most probably points to valid packets.
*/
type CircMemLogs struct {
	header *circBufHeader
	length uint64
	buffer []byte
}

func (circ *CircMemLogs) Size() int {
	return int(circ.length + uint64(unsafe.Sizeof(MmapHeader{})))
}

// Must only be called on a section of data where nobody else is writing to it
func (circ *CircMemLogs) wrapWrite_(idx uint64, data []byte) {
	numWrite := uint64(len(data))
	if idx+numWrite > circ.length {
		secondNum := (idx + numWrite) - circ.length
		numWrite -= secondNum
		copy(circ.buffer[0:secondNum], data[numWrite:])
	}

	copy(circ.buffer[idx:idx+numWrite], data[:numWrite])
}

func (circ *CircMemLogs) reserveMem(dataLen uint64) (dataStartIdx uint64) {
	dataEnd := atomic.AddUint64(&circ.header.PastEndIdx, uint64(dataLen))
	return (dataEnd - dataLen) % circ.length
}

// Note: in development code, you should never provide a True partialWrite
func (circ *CircMemLogs) writeData(data []byte, length int, partialWrite bool) {
	// For now, if the message is too long then just toss it
	if uint64(length) > circ.length {
		return
	}

	// We only want to use the lower 2 bytes, but need all 4 to use sync/atomic
	dataLen := uint32(length)
	dataRaw := (*[2]byte)(unsafe.Pointer(&dataLen))
	// Append the data field to the packet
	data = append(data, dataRaw[:]...)

	dataStart := circ.reserveMem(uint64(len(data)))

	// Now that we know we have space, write in the entry
	circ.wrapWrite_(dataStart, data)
	lenStart := (dataStart + uint64(dataLen)) % circ.length

	// For testing purposes only: if we need to generate some partially written
	// packets, then do so by not finishing this one.
	if partialWrite {
		return
	}

	// Now that the entry is written completely, mark the packet as safe to read,
	// but use an atomic operation to load to ensure a memory barrier
	dataLen &= ^uint32(entryCompleteBit)
	atomic.AddUint32(&dataLen, entryCompleteBit)
	circ.wrapWrite_(lenStart, (*dataRaw)[:])
}

const LogStrSize = 64
const logTextMax = 62

type LogStr struct {
	Text         [logTextMax]byte
	LogSubsystem uint8
	LogLevel     uint8
}

func checkRecursion(errorPrefix string, format string) {
	// Ensure log isn't ourselves
	if len(format) >= len(errorPrefix) &&
		errorPrefix == format[:len(errorPrefix)] {

		panic(fmt.Sprintf("Stuck in infinite recursion: %s", format))
	}
}

func newLogStr(idx LogSubsystem, level uint8, format string) (LogStr, error) {
	var err error
	var rtn LogStr
	rtn.LogSubsystem = uint8(idx)
	rtn.LogLevel = level
	copyLen := len(format)
	if copyLen > logTextMax {
		errorPrefix := "Log format string exceeds allowable length"
		checkRecursion(errorPrefix, format)

		err = errors.New(errorPrefix)
		copyLen = logTextMax
	}
	copy(rtn.Text[:], format[:copyLen])

	return rtn, err
}

type IdStrMap struct {
	data    map[string]uint16
	mapLock sync.RWMutex

	buffer  *[mmapStrMapSize / LogStrSize]LogStr
	freeIdx uint16
}

func newCircBuf(mapHeader *circBufHeader,
	mapBuffer []byte) CircMemLogs {

	rtn := CircMemLogs{
		header: mapHeader,
		length: uint64(len(mapBuffer)),
		buffer: mapBuffer,
	}

	rtn.header.Size = uint64(len(mapBuffer))

	return rtn
}

func newIdStrMap(buf []byte, offset int) IdStrMap {
	var rtn IdStrMap
	rtn.data = make(map[string]uint16)
	rtn.freeIdx = 0
	rtn.buffer = (*[mmapStrMapSize /
		LogStrSize]LogStr)(unsafe.Pointer(&buf[offset]))

	return rtn
}

func newSharedMemory(dir string, filename string, mmapTotalSize int,
	daemonVersion string, errOut *Qlog) *SharedMemory {

	if dir == "" || filename == "" {
		return nil
	}

	// Create a file and its path to be mmap'd
	err := os.MkdirAll(dir, 0777)
	if err != nil {
		panic(fmt.Sprintf("Unable to ensure log file path exists: %s", dir))
	}

	mapFile, err := os.OpenFile(dir+"/"+filename,
		os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if mapFile == nil || err != nil {
		panic(fmt.Sprintf("Unable to create shared memory log file: %s/%s",
			dir, filename))
	}

	circBufSize := mmapTotalSize - (mmapStrMapSize +
		int(unsafe.Sizeof(MmapHeader{})))
	// Size the file to fit the shared memory requirements
	_, err = mapFile.Seek(int64(mmapTotalSize-1), 0)
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

	// Make sure we touch every byte to ensure that the mmap isn't sparse
	for i := 0; i < mmapTotalSize; i++ {
		mmap[i] = 0
	}

	var rtn SharedMemory
	rtn.fd = mapFile
	rtn.mapSize = mmapTotalSize
	rtn.buffer = mmap
	header := (*MmapHeader)(unsafe.Pointer(&mmap[0]))
	header.Version = QlogVersion

	versionLen := len(daemonVersion)
	if versionLen > len(header.DaemonVersion) {
		versionLen = len(header.DaemonVersion)
	}
	copy(header.DaemonVersion[:], daemonVersion[:versionLen])
	if versionLen < len(header.DaemonVersion) {
		header.DaemonVersion[versionLen] = '\x00'
	}

	header.StrMapSize = mmapStrMapSize
	headerOffset := int(unsafe.Sizeof(MmapHeader{}))
	rtn.circBuf = newCircBuf(&header.CircBuf,
		mmap[headerOffset:headerOffset+circBufSize])
	rtn.strIdMap = newIdStrMap(mmap, headerOffset+circBufSize)
	rtn.errOut = errOut

	return &rtn
}

func (strMap *IdStrMap) mapGetLogIdx(format string) (idx uint16, valid bool) {

	strMap.mapLock.RLock()
	entry, ok := strMap.data[format]
	strMap.mapLock.RUnlock()

	if ok {
		return entry, true
	}

	return 0, false
}

func (strMap *IdStrMap) createLogIdx(idx LogSubsystem, level uint8,
	format string) (uint16, error) {

	strMap.mapLock.Lock()
	existingId, ok := strMap.data[format]
	// The vast majority of the time the string will already exist.
	// Optimize our locking for this scenario
	strMap.mapLock.Unlock()

	if ok {
		return existingId, nil
	}

	// Construct the new log string we *may* use outside of the critical region
	// because it can be slow to make
	newLog, err := newLogStr(idx, level, format)

	// we may need to add a new log. lock and check again
	strMap.mapLock.Lock()

	// Check again to avoid a race condition
	existingId, ok = strMap.data[format]
	if ok {
		strMap.mapLock.Unlock()
		return existingId, nil
	}

	// format string still doesn't exist, so create it with the same lock
	newIdx := strMap.freeIdx
	strMap.freeIdx++

	strMap.data[format] = newIdx
	strMap.buffer[newIdx] = newLog

	strMap.mapLock.Unlock()
	return newIdx, err
}

func (strMap *IdStrMap) fetchLogIdx(idx LogSubsystem, level uint8,
	format string) (uint16, error) {

	existingId, idValid := strMap.mapGetLogIdx(format)

	if idValid {
		return existingId, nil
	}

	return strMap.createLogIdx(idx, level, format)
}

const (
	TypeInt8Pointer   = 1
	TypeInt8          = 2
	TypeUint8Pointer  = 3
	TypeUint8         = 4
	TypeInt16Pointer  = 5
	TypeInt16         = 6
	TypeUint16Pointer = 7
	TypeUint16        = 8
	TypeInt32Pointer  = 9
	TypeInt32         = 10
	TypeUint32Pointer = 11
	TypeUint32        = 12
	TypeInt64Pointer  = 13
	TypeInt64         = 14
	TypeUint64Pointer = 15
	TypeUint64        = 16
	TypeString        = 17
	TypeByteArray     = 18
	TypeBoolean       = 19
)

type emptyInterface struct {
	type_ unsafe.Pointer
	value unsafe.Pointer
}

func interfaceAsUint8(intf interface{}) uint8 {
	ei := (*emptyInterface)(unsafe.Pointer(&intf))
	return (*(*uint8)(ei.value))
}

func interfaceAsUint16(intf interface{}) uint16 {
	ei := (*emptyInterface)(unsafe.Pointer(&intf))
	return (*(*uint16)(ei.value))
}

func interfaceAsUint32(intf interface{}) uint32 {
	ei := (*emptyInterface)(unsafe.Pointer(&intf))
	return (*(*uint32)(ei.value))
}

func interfaceAsUint64(intf interface{}) uint64 {
	ei := (*emptyInterface)(unsafe.Pointer(&intf))
	return (*(*uint64)(ei.value))
}

// Writes the data, with a type prefix field two bytes long, to output. We pass in a
// pointer to the output array instead of returning one to append so that we can
// take advantage of output having a larger capacity and reducing memmoves
func (mem *SharedMemory) binaryWrite(data interface{}, format string,
	output []byte, offset int) ([]byte, int) {

	dataType := reflect.TypeOf(data)
	dataKind := dataType.Kind()
	switch {
	case dataKind == reflect.Int8:
		offset = toBinaryUint16(output, offset, TypeInt8)
		offset = toBinaryUint8(output, offset, interfaceAsUint8(data))
	case dataKind == reflect.Uint8:
		offset = toBinaryUint16(output, offset, TypeUint8)
		offset = toBinaryUint8(output, offset, interfaceAsUint8(data))
	case dataKind == reflect.Int16:
		offset = toBinaryUint16(output, offset, TypeInt16)
		offset = toBinaryUint16(output, offset, interfaceAsUint16(data))
	case dataKind == reflect.Uint16:
		offset = toBinaryUint16(output, offset, TypeUint16)
		offset = toBinaryUint16(output, offset, interfaceAsUint16(data))
	case dataKind == reflect.Int32:
		offset = toBinaryUint16(output, offset, TypeInt32)
		offset = toBinaryUint32(output, offset, interfaceAsUint32(data))
	case dataKind == reflect.Uint32:
		offset = toBinaryUint16(output, offset, TypeUint32)
		offset = toBinaryUint32(output, offset, interfaceAsUint32(data))
	case dataKind == reflect.Int:
		offset = toBinaryUint16(output, offset, TypeInt64)
		offset = toBinaryUint64(output, offset, interfaceAsUint64(data))
	case dataKind == reflect.Uint:
		offset = toBinaryUint16(output, offset, TypeUint64)
		offset = toBinaryUint64(output, offset, interfaceAsUint64(data))
	case dataKind == reflect.Int64:
		offset = toBinaryUint16(output, offset, TypeInt64)
		offset = toBinaryUint64(output, offset, interfaceAsUint64(data))
	case dataKind == reflect.Uint64:
		offset = toBinaryUint16(output, offset, TypeUint64)
		offset = toBinaryUint64(output, offset, interfaceAsUint64(data))
	case dataKind == reflect.String:
		output, offset = writeArray(output, offset, format,
			[]byte(data.(string)), TypeString)
	case dataKind == reflect.Slice && dataType.Elem().Kind() == reflect.Uint8:
		output, offset = writeArray(output, offset, format, data.([]uint8),
			TypeByteArray)
	case dataKind == reflect.Bool:
		offset = toBinaryUint16(output, offset, TypeBoolean)
		if data.(bool) {
			offset = toBinaryUint8(output, offset, 1)
		} else {
			offset = toBinaryUint8(output, offset, 0)
		}
	default:
		errorPrefix := "ERROR: LogConverter needed for %s at %s: %v"

		unknownType := reflect.ValueOf(data).String()
		str := fmt.Sprintf(errorPrefix, unknownType, string(debug.Stack()),
			data)
		output, offset = writeArray(output, offset, errorPrefix+format,
			[]byte(str), TypeString)
	}

	return output, offset
}

func writeArray(output []byte, offset int, format string, data []byte,
	byteType uint16) ([]byte, int) {

	if len(data) > math.MaxUint16 {
		panic(fmt.Sprintf("String len > 65535 unsupported: "+
			"%s", format))
	}

	offset = toBinaryUint16(output, offset, byteType)
	offset = toBinaryUint16(output, offset, uint16(len(data)))

	if cap(output)-offset < len(data) {
		output = expandBuffer(output, len(data))
	}

	for i, v := range data {
		output[offset+i] = v
	}

	offset += len(data)

	return output, offset
}

// Don't use interfaces where possible because they're slow
func toBinaryUint8(buf []byte, offset int, input uint8) int {
	bufPtr := (*uint8)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 1
}

func toBinaryUint16(buf []byte, offset int, input uint16) int {
	bufPtr := (*uint16)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 2
}

func toBinaryUint32(buf []byte, offset int, input uint32) int {
	bufPtr := (*uint32)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 4
}

func toBinaryUint64(buf []byte, offset int, input uint64) int {
	bufPtr := (*uint64)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 8
}

func expandBuffer(buf []byte, howMuch int) []byte {
	if howMuch < 128 {
		howMuch = cap(buf)
	}
	tmp := make([]byte, cap(buf)+howMuch)
	copy(tmp, buf)
	return tmp
}

func (mem *SharedMemory) generateLogEntry(buf []byte, strMapId uint16, reqId uint64,
	timestamp int64, format string, args ...interface{}) ([]byte, int) {

	offset := 0

	// Two bytes prefix for the total packet length, before the num of args.
	// Write the log entry header with no prefixes
	offset = toBinaryUint16(buf, offset, uint16(len(args)))
	offset = toBinaryUint16(buf, offset, strMapId)
	offset = toBinaryUint64(buf, offset, reqId)
	offset = toBinaryUint64(buf, offset, uint64(timestamp))

	originalOffset := offset

	for i := 0; i < len(args); i++ {
		if cap(buf)-offset < 10 {
			buf = expandBuffer(buf, 10)
		}
		buf, offset = mem.binaryWrite(args[i], format, buf, offset)
	}

	// Make sure length isn't too long, excluding the size bytes
	if offset > MaxPacketLen {
		offset = originalOffset

		errorPrefix := "Log data exceeds allowable length at %s: %s"
		str := fmt.Sprintf(errorPrefix, string(debug.Stack()), format)
		buf, offset = mem.binaryWrite(str, format, buf, offset)
	}

	buf = buf[:offset]
	return buf, offset
}

func (mem *SharedMemory) Sync() int {
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&mem.buffer[0])), // *addr
		uintptr(mem.mapSize),                    // length
		uintptr(syscall.MS_SYNC))                //flags

	return int(err)
}

func (mem *SharedMemory) logEntry(idx LogSubsystem, reqId uint64, level uint8,
	timestamp int64, format string, args ...interface{}) {

	// Create the string map entry / fetch existing one
	strId, err := mem.strIdMap.fetchLogIdx(idx, level, format)
	if err != nil {
		mem.errOut.Log(LogQlog, reqId, 1, err.Error()+": %s\n", format)
		return
	}

	// Generate the byte array packet
	data, length := mem.generateLogEntry(strId, reqId, timestamp, format,
		args...)

	partialWrite := false
	if mem.testMode && len(mem.testDropStr) < len(format) &&
		strings.Compare(mem.testDropStr,
			format[:len(mem.testDropStr)]) == 0 {

		partialWrite = true
	}

	mem.circBuf.writeData(data, length, partialWrite)
}
