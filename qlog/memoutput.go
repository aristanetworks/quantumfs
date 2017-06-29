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
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/aristanetworks/quantumfs/utils"
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
func (circ *CircMemLogs) writePacket(partialWrite bool, format string,
	formatId uint16, reqId uint64, timestamp int64, length uint64,
	argKinds []reflect.Kind, args ...interface{}) {

	// Account for the packet length field
	packetLength := length + 2

	// For now, if the message is too long then just toss it
	if packetLength > circ.length {
		return
	}

	dataOffset := circ.reserveMem(packetLength)

	// Write the length field without the completion bit
	lenOffset := (dataOffset + length) % circ.length
	flagAndLength := length & ^uint64(entryCompleteBit)

	var offset uint64
	var buf []byte
	var fastpath bool
	if lenOffset > dataOffset {
		// Fast path, we don't need to wrap around the end of the buffer
		fastpath = true
		offset = dataOffset
		buf = circ.buffer
	} else {
		// Slow path, we need to wrap some of the data around the end of the
		// buffer. This is an uncommon case, so use a staging buffer.
		fastpath = false
		buf = make([]byte, packetLength)
		offset = 0
	}

	if fastpath {
		insertUint16(buf, lenOffset, uint16(flagAndLength))
	} else {
		insertUint16(buf, offset+length, uint16(flagAndLength))
	}

	// Write the entry header
	offset = insertUint16(buf, offset, uint16(len(args)))
	offset = insertUint16(buf, offset, formatId)
	offset = insertUint64(buf, offset, reqId)
	offset = insertUint64(buf, offset, uint64(timestamp))

	// Write the entry arguments
	for i, arg := range args {
		offset = writeArg(buf, offset, format, arg, argKinds[i])
	}

	if !fastpath {
		circ.wrapWrite_(dataOffset, buf)
	}

	// For testing purposes only: if we need to generate some partially written
	// packets, then do so by not finishing this one.
	if partialWrite {
		return
	}

	// Now that the entry is written completely, mark the packet as safe to read,
	// but use an atomic operation to ensure a compiler and memory barrier
	atomic.AddUint64(&flagAndLength, uint64(entryCompleteBit))

	if fastpath {
		insertUint16(buf, lenOffset, uint16(flagAndLength))
	} else {
		insertUint16(buf, 0+length, uint16(flagAndLength))
		circ.wrapWrite_(dataOffset, buf)
	}
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
	// The map from format strings to format index is a read heavy datastructure.
	// Once the daemon has warmed up it is unlikely a new format will be added.
	// Therefore we optimize heavily for the read-only case.
	//
	// Golang maps are multiple reader safe and consequently we can allow all the
	// readers access to the same map without a lock. However, we do sometimes
	// need to add new messages to the map. We handle that by atomically changing
	// a pointer to point to the most recent map. Read-only accesses will either
	// get the old map, or the new map with the additional entries, but in either
	// case the map is only visible after there will be no further writers.
	//
	// Now given a map may be fetched and retained for a period, we must also
	// prevent the garbage collector from reclaiming the map until all possible
	// readers have completed. We assume no logging operation will take more than
	// a second and wait that long before removing the final reference to the
	// previous map.
	//
	// This is the pointer to the most recent map and must be loaded using
	// atomic.LoadPointer().
	currentMapPtr unsafe.Pointer

	lock utils.DeferableMutex // Protects everything below here
	ids  map[string]uint16

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
	ids := make(map[string]uint16)
	rtn.ids = ids
	atomic.StorePointer(&rtn.currentMapPtr, unsafe.Pointer(&ids))
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

	ids := (*map[string]uint16)(strMap.currentMapPtr)
	entry, ok := (*ids)[format]

	if ok {
		return entry, true
	}

	return 0, false
}

func (strMap *IdStrMap) createLogIdx(idx LogSubsystem, level uint8,
	format string) (uint16, error) {

	defer strMap.lock.Lock().Unlock()

	if existingId, ok := strMap.ids[format]; ok {
		// Somebody beat us to adding this format, we have no work to do
		return existingId, nil
	}

	newLog, err := newLogStr(idx, level, format)

	newIdx := strMap.freeIdx
	strMap.freeIdx++

	// Copy the old map into the new one and add the additional format entry
	newMap := make(map[string]uint16, len(strMap.ids)+1)
	for k, v := range strMap.ids {
		newMap[k] = v
	}
	newMap[format] = newIdx

	strMap.buffer[newIdx] = newLog

	// Delay garbage collection of the previous map until all possible current
	// users are finished.
	go func(mapToClear map[string]uint16) {
		time.Sleep(1 * time.Second)

		// Waste time to avoid possible optimizations which eliminates the
		// reference to the map.
		for k, _ := range mapToClear {
			delete(mapToClear, k)
		}
	}(strMap.ids)

	// Now publish the new map, no writing may occur to this map after this
	// point.
	strMap.ids = newMap
	atomic.StorePointer(&strMap.currentMapPtr, unsafe.Pointer(&newMap))

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

func errorUnknownType(arg interface{}) (msgSize int,
	msg string) {

	str := fmt.Sprintf("ERROR: Unsupported qlog type %s",
		reflect.TypeOf(arg).String())

	return len(str), str
}

func errorPacketTooLong(format string) (msgSize int, msg string) {
	str := fmt.Sprintf("ERROR: Log data exceeds allowable length: %s", format)

	return len(str), str
}

func writeArg(buf []byte, offset uint64, format string, arg interface{},
	argKind reflect.Kind) uint64 {

	// The structure and sizes written here must match
	// SharedMemory.computePacketSize() to ensure the size is computed correctly.
	switch {
	case argKind == reflect.Int8:
		offset = insertUint16(buf, offset, TypeInt8)
		offset = insertUint8(buf, offset, interfaceAsUint8(arg))
	case argKind == reflect.Uint8:
		offset = insertUint16(buf, offset, TypeUint8)
		offset = insertUint8(buf, offset, interfaceAsUint8(arg))
	case argKind == reflect.Bool:
		offset = insertUint16(buf, offset, TypeBoolean)
		if arg.(bool) {
			offset = insertUint8(buf, offset, 1)
		} else {
			offset = insertUint8(buf, offset, 0)
		}
	case argKind == reflect.Int16:
		offset = insertUint16(buf, offset, TypeInt16)
		offset = insertUint16(buf, offset, interfaceAsUint16(arg))
	case argKind == reflect.Uint16:
		offset = insertUint16(buf, offset, TypeUint16)
		offset = insertUint16(buf, offset, interfaceAsUint16(arg))
	case argKind == reflect.Int32:
		offset = insertUint16(buf, offset, TypeInt32)
		offset = insertUint32(buf, offset, interfaceAsUint32(arg))
	case argKind == reflect.Uint32:
		offset = insertUint16(buf, offset, TypeUint32)
		offset = insertUint32(buf, offset, interfaceAsUint32(arg))
	case argKind == reflect.Int:
		offset = insertUint16(buf, offset, TypeInt64)
		offset = insertUint64(buf, offset, interfaceAsUint64(arg))
	case argKind == reflect.Uint:
		offset = insertUint16(buf, offset, TypeUint64)
		offset = insertUint64(buf, offset, interfaceAsUint64(arg))
	case argKind == reflect.Int64:
		offset = insertUint16(buf, offset, TypeInt64)
		offset = insertUint64(buf, offset, interfaceAsUint64(arg))
	case argKind == reflect.Uint64:
		offset = insertUint16(buf, offset, TypeUint64)
		offset = insertUint64(buf, offset, interfaceAsUint64(arg))
	case argKind == reflect.String:
		offset = writeArray(buf, offset, format, []byte(arg.(string)),
			TypeString)
	case argKind == sliceOfBytesKind:
		offset = writeArray(buf, offset, format, arg.([]uint8),
			TypeByteArray)
	default:
		_, msg := errorUnknownType(arg)
		offset = writeArray(buf, offset, format, []byte(msg), TypeString)
	}

	return offset
}

func writeArray(buf []byte, offset uint64, format string, data []byte,
	byteType uint16) uint64 {

	if len(data) > math.MaxUint16 {
		panic(fmt.Sprintf("String len > 65535 unsupported: "+
			"%s", format))
	}

	offset = insertUint16(buf, offset, byteType)
	offset = insertUint16(buf, offset, uint16(len(data)))

	for _, v := range data {
		offset = insertUint8(buf, offset, v)
	}

	return offset
}

func expandBuffer(buf []byte, howMuch int) []byte {
	if howMuch < 128 {
		howMuch = cap(buf)
	}
	tmp := make([]byte, cap(buf)+howMuch)
	copy(tmp, buf)
	return tmp
}

const sliceOfBytesKind = (reflect.Slice << 16) | reflect.Uint8

func (mem *SharedMemory) computePacketSize(format string, kinds []reflect.Kind,
	args ...interface{}) uint64 {

	// The structure of this method should match CircMemLogs.writePacket() and
	// CircMemLogs.writeArg() to ensure the size is computed correctly. This is
	// especially critical for the error strings.

	size := 0
	size += 2 // Number of arguments
	size += 2 // LogEntry.strIdx
	size += 8 // LogEntry.reqId
	size += 8 // LogEntry.timestamp

	for i, arg := range args {
		argType := reflect.TypeOf(arg)
		argKind := argType.Kind()

		kinds[i] = argKind

		size += 2 // Argument type, ie. TypeInt8

		switch {
		case argKind == reflect.Int8:
			fallthrough
		case argKind == reflect.Uint8:
			fallthrough
		case argKind == reflect.Bool:
			size += 1

		case argKind == reflect.Int16:
			fallthrough
		case argKind == reflect.Uint16:
			size += 2

		case argKind == reflect.Int32:
			fallthrough
		case argKind == reflect.Uint32:
			size += 4

		case argKind == reflect.Int:
			fallthrough
		case argKind == reflect.Uint:
			fallthrough
		case argKind == reflect.Int64:
			fallthrough
		case argKind == reflect.Uint64:
			size += 8

		case argKind == reflect.String:
			size += 2 // Length of string
			size += len([]byte(arg.(string)))

		case argKind == reflect.Slice && argType.Elem().Kind() == reflect.Uint8:
			// Store a compound kind because this is a compound type and
			// we don't want to carry/regenerate the reflect.Type object
			// for all the arguments in CircMemLogs.writeArg()
			kinds[i] = sliceOfBytesKind

			size += 2 // Length of slice
			size += len(arg.([]uint8))

		default:
			size += 2 // Length of error message
			l, _ := errorUnknownType(arg)
			size += l
		}
	}

	// Make sure length isn't too long, excluding the packet size bytes
	if size > MaxPacketLen {
		size = 2 // Length of error message
		l, _ := errorPacketTooLong(format)
		size += l
	}

	return uint64(size)
}

// Don't use interfaces where possible because they're slow
func insertUint8(buf []byte, offset uint64, input uint8) uint64 {
	bufPtr := (*uint8)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 1
}

func insertUint16(buf []byte, offset uint64, input uint16) uint64 {
	bufPtr := (*uint16)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 2
}

func insertUint32(buf []byte, offset uint64, input uint32) uint64 {
	bufPtr := (*uint32)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 4
}

func insertUint64(buf []byte, offset uint64, input uint64) uint64 {
	bufPtr := (*uint64)(unsafe.Pointer(&buf[offset]))
	*bufPtr = input
	return offset + 8
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
	formatId, err := mem.strIdMap.fetchLogIdx(idx, level, format)
	if err != nil {
		mem.errOut.Log(LogQlog, reqId, 1, err.Error()+": %s\n", format)
		return
	}

	argumentKinds := make([]reflect.Kind, len(args))
	packetSize := mem.computePacketSize(format, argumentKinds, args...)

	partialWrite := false
	if mem.testMode && len(mem.testDropStr) < len(format) &&
		strings.Compare(mem.testDropStr,
			format[:len(mem.testDropStr)]) == 0 {

		partialWrite = true
	}

	mem.circBuf.writePacket(partialWrite, format, formatId, reqId, timestamp,
		packetSize, argumentKinds, args...)
}
