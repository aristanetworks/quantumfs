// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging shared memory support
import "errors"
import "fmt"
import "math"
import "os"
import "reflect"
import "runtime/debug"
import "strings"
import "sync"
import "sync/atomic"
import "syscall"
import "unsafe"

// We need a static array sized upper bound on our memory. Increase this as needed.
const DefaultMmapSize = (360000 * 24) + mmapStrMapSize + unsafe.Sizeof(MmapHeader{})

// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
const QlogVersion = 3

// We use the upper-most bit of the length field to indicate the packet is ready,
// so the max packet length is 7 bits long
const MaxPacketLen = 32767

type MmapHeader struct {
	Version    uint32
	StrMapSize uint32
	CircBuf    circBufHeader
}

type circBufHeader struct {
	Size uint64

	// This marks one past the end of the circular buffer
	PastEndIdx uint64
}

type SharedMemory struct {
	fd       *os.File
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
	buffer []byte

	// Lock this for as short a period as possible
	writeMutex sync.Mutex
}

func (circ *CircMemLogs) Size() int {
	return len(circ.buffer) + int(unsafe.Sizeof(MmapHeader{}))
}

// Must only be called on a section of data where nobody else is writing to it
func (circ *CircMemLogs) wrapWrite_(idx uint64, data []byte) {
	numWrite := uint64(len(data))
	if idx+numWrite > uint64(len(circ.buffer)) {
		secondNum := (idx + numWrite) - uint64(len(circ.buffer))
		numWrite -= secondNum
		copy(circ.buffer[0:secondNum], data[numWrite:])
	}

	copy(circ.buffer[idx:idx+numWrite], data[:numWrite])
}

func (circ *CircMemLogs) reserveMem(dataLen uint64) (dataStartIdx uint64) {

	// Minimize the size of the critical section, don't use defer
	circ.writeMutex.Lock()

	dataStart := circ.header.PastEndIdx
	circ.header.PastEndIdx += uint64(dataLen)
	circ.header.PastEndIdx %= uint64(len(circ.buffer))

	circ.writeMutex.Unlock()

	return dataStart
}

// Note: in development code, you should never provide a True partialWrite
func (circ *CircMemLogs) writeData(data []byte, partialWrite bool) {
	// For now, if the message is too long then just toss it
	if len(data) > len(circ.buffer) {
		return
	}

	// we only want to use the lower 2 bytes, but need all 4 to use sync/atomic
	dataLen := uint32(len(data))
	dataRaw := (*[2]byte)(unsafe.Pointer(&dataLen))
	data = append(data, dataRaw[:]...)

	// add 2 for the length field
	dataStart := circ.reserveMem(uint64(len(data)))

	// Now that we know we have space, write in the entry
	circ.wrapWrite_(dataStart, data)
	lenStart := (dataStart + uint64(dataLen)) % uint64(len(circ.buffer))

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
	errOut *Qlog) *SharedMemory {

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
	rtn.buffer = mmap
	header := (*MmapHeader)(unsafe.Pointer(&mmap[0]))
	header.Version = QlogVersion
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

type LogPrimitive interface {
	// Return the type cast to a primitive, in interface form so that
	// type asserts will work
	Primitive() interface{}
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

// Writes the data, with a type prefix field two bytes long, to output. We pass in a
// pointer to the output array instead of returning one to append so that we can
// take advantage of output having a larger capacity and reducing memmoves
func (mem *SharedMemory) binaryWrite(data interface{}, format string,
	output *[]byte) {

	// Handle primitive aliases first
	if prim, ok := data.(LogPrimitive); ok {
		// This takes the alias and provides a base class via an interface{}
		// Without this, type casting will check against the alias instead
		// of the base type
		data = prim.Primitive()
	}

	switch v := data.(type) {
	case *int8:
		*output = append(*output, toBinaryUint16(TypeInt8Pointer)...)
		*output = append(*output, toBinaryUint8(uint8(*v))...)
	case int8:
		*output = append(*output, toBinaryUint16(TypeInt8)...)
		*output = append(*output, toBinaryUint8(uint8(v))...)
	case *uint8:
		*output = append(*output, toBinaryUint16(TypeUint8Pointer)...)
		*output = append(*output, toBinaryUint8(*v)...)
	case uint8:
		*output = append(*output, toBinaryUint16(TypeUint8)...)
		*output = append(*output, toBinaryUint8(v)...)
	case *int16:
		*output = append(*output, toBinaryUint16(TypeInt16Pointer)...)
		*output = append(*output, toBinaryUint16(uint16(*v))...)
	case int16:
		*output = append(*output, toBinaryUint16(TypeInt16)...)
		*output = append(*output, toBinaryUint16(uint16(v))...)
	case *uint16:
		*output = append(*output, toBinaryUint16(TypeUint16Pointer)...)
		*output = append(*output, toBinaryUint16(*v)...)
	case uint16:
		*output = append(*output, toBinaryUint16(TypeUint16)...)
		*output = append(*output, toBinaryUint16(v)...)
	case *int32:
		*output = append(*output, toBinaryUint16(TypeInt32Pointer)...)
		*output = append(*output, toBinaryUint32(uint32(*v))...)
	case int32:
		*output = append(*output, toBinaryUint16(TypeInt32)...)
		*output = append(*output, toBinaryUint32(uint32(v))...)
	case *uint32:
		*output = append(*output, toBinaryUint16(TypeUint32Pointer)...)
		*output = append(*output, toBinaryUint32(*v)...)
	case uint32:
		*output = append(*output, toBinaryUint16(TypeUint32)...)
		*output = append(*output, toBinaryUint32(v)...)
	case int:
		*output = append(*output, toBinaryUint16(TypeInt64)...)
		*output = append(*output, toBinaryUint64(uint64(v))...)
	case uint:
		*output = append(*output, toBinaryUint16(TypeUint64)...)
		*output = append(*output, toBinaryUint64(uint64(v))...)
	case *int64:
		*output = append(*output, toBinaryUint16(TypeInt64Pointer)...)
		*output = append(*output, toBinaryUint64(uint64(*v))...)
	case int64:
		*output = append(*output, toBinaryUint16(TypeInt64)...)
		*output = append(*output, toBinaryUint64(uint64(v))...)
	case *uint64:
		*output = append(*output, toBinaryUint16(TypeUint64Pointer)...)
		*output = append(*output, toBinaryUint64(*v)...)
	case uint64:
		*output = append(*output, toBinaryUint16(TypeUint64)...)
		*output = append(*output, toBinaryUint64(v)...)
	case string:
		writeArray(output, format, []byte(v), TypeString)
	case []byte:
		writeArray(output, format, v, TypeByteArray)
	case bool:
		*output = append(*output, toBinaryUint16(TypeBoolean)...)
		if v {
			*output = append(*output, toBinaryUint8(1)...)
		} else {
			*output = append(*output, toBinaryUint8(0)...)
		}
	default:
		errorPrefix := "ERROR: LogConverter needed for %s:\n%s\n"
		checkRecursion(errorPrefix, format)

		str := fmt.Sprintf("%v", data)
		writeArray(output, format, []byte(str), TypeString)
		mem.errOut.Log(LogQlog, QlogReqId, 1, errorPrefix,
			reflect.ValueOf(data).String(), string(debug.Stack()))
	}
}

func writeArray(output *[]byte, format string, data []byte, byteType uint16) {
	if len(data) > math.MaxUint16 {
		panic(fmt.Sprintf("String len > 65535 unsupported: "+
			"%s", format))
	}

	*output = append(*output, toBinaryUint16(byteType)...)

	*output = append(*output, toBinaryUint16(uint16(len(data)))...)

	*output = append(*output, data...)
}

// Don't use interfaces where possible because they're slow
func toBinaryUint8(input uint8) []byte {
	return (*[1]byte)(unsafe.Pointer(&input))[:]
}

func toBinaryUint16(input uint16) []byte {
	return (*[2]byte)(unsafe.Pointer(&input))[:]
}

func toBinaryUint32(input uint32) []byte {
	return (*[4]byte)(unsafe.Pointer(&input))[:]
}

func toBinaryUint64(input uint64) []byte {
	return (*[8]byte)(unsafe.Pointer(&input))[:]
}

func (mem *SharedMemory) generateLogEntry(strMapId uint16, reqId uint64,
	timestamp int64, format string, args ...interface{}) []byte {

	// Ensure we provide a sensible initial capacity
	buf := make([]byte, 0, 128)

	// Two bytes prefix for the total packet length, before the num of args.
	// Write the log entry header with no prefixes
	buf = append(buf, toBinaryUint16(uint16(len(args)))...)
	buf = append(buf, toBinaryUint16(strMapId)...)
	buf = append(buf, toBinaryUint64(reqId)...)
	buf = append(buf, toBinaryUint64(uint64(timestamp))...)

	for i := 0; i < len(args); i++ {
		mem.binaryWrite(args[i], format, &buf)
	}

	// Make sure length isn't too long, excluding the size bytes
	if len(buf) > MaxPacketLen {
		errorPrefix := "Log data exceeds allowable length: %s\n"
		checkRecursion(errorPrefix, format)

		mem.errOut.Log(LogQlog, reqId, 1, errorPrefix, format)

		buf = buf[:MaxPacketLen]
	}

	return buf
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
	data := mem.generateLogEntry(strId, reqId, timestamp, format, args...)

	partialWrite := false
	if mem.testMode && len(mem.testDropStr) < len(format) &&
		strings.Compare(mem.testDropStr,
			format[:len(mem.testDropStr)]) == 0 {

		partialWrite = true
	}

	mem.circBuf.writeData(data, partialWrite)
}
