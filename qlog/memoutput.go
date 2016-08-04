// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging shared memory support
import "bytes"
import "encoding/binary"
import "errors"
import "fmt"
import "io"
import "math"
import "os"
import "reflect"
import "runtime/debug"
import "sync"
import "syscall"
import "unsafe"

// We need a static array sized upper bound on our memory. Increase this as needed.
const DefaultMmapSize = (360000 * 24) + mmapStrMapSize + unsafe.Sizeof(MmapHeader{})

// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
const MmapHeaderVersion = 1

type MmapHeader struct {
	Version		uint32
	StrMapSize	uint32
	CircBuf		circBufHeader
}

type circBufHeader struct {
	Size uint32

	// This marks one past the end of the circular buffer
	PastEndIdx uint32
}

type SharedMemory struct {
	fd		*os.File
	circBuf		CircMemLogs
	strIdMap	IdStrMap
	buffer		[]byte

	// This is dangerous as Qlog also owns SharedMemory. SharedMemory must
	// ensure that any call it makes to Qlog doesn't result in infinite recursion
	errOut		*Qlog
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
	writeMutex	sync.Mutex
}

func (circ *CircMemLogs) Size() int {
	return len(circ.buffer) + int(unsafe.Sizeof(MmapHeader{}))
}

// Must only be called on a section of data where nobody else is writing to it
func (circ *CircMemLogs) wrapWrite_(idx uint32, data []byte) {
	numWrite := uint32(len(data))
	if idx+numWrite > uint32(len(circ.buffer)) {
		secondNum := (idx + numWrite) - uint32(len(circ.buffer))
		numWrite -= secondNum
		copy(circ.buffer[0:secondNum], data[numWrite:])
	}

	copy(circ.buffer[idx:idx+numWrite], data[:numWrite])
}

func (circ *CircMemLogs) writeData(data []byte) {
	// For now, if the message is too long then just toss it
	if len(data) > len(circ.buffer) {
		return
	}

	circBufLen := uint32(len(circ.buffer))
	dataLen := uint16(len(data))
	dataRaw := (*[2]byte)(unsafe.Pointer(&dataLen))
	var dataStart uint32
	{
		circ.writeMutex.Lock()
		defer circ.writeMutex.Unlock()

		dataStart = circ.header.PastEndIdx

		circ.header.PastEndIdx += uint32(dataLen)
		circ.wrapWrite_(uint32(circ.header.PastEndIdx % circBufLen),
			dataRaw[:])

		circ.header.PastEndIdx += 2
		circ.header.PastEndIdx %= circBufLen

	}

	// Now that we know we have space, write in the entry
	circ.wrapWrite_(uint32(dataStart), data)
}

const LogStrSize = 64
const logTextMax = 62

type LogStr struct {
	Text         [logTextMax]byte
	LogSubsystem uint8
	LogLevel     uint8
}

func newLogStr(idx LogSubsystem, level uint8, format string) (LogStr, error) {
	var err error
	var rtn LogStr
	rtn.LogSubsystem = uint8(idx)
	rtn.LogLevel = level
	copyLen := len(format)
	if copyLen > logTextMax {
		errorPrefix := "Log format string exceeds allowable length"

		// Ensure we're not already recursing
		if len(format) >= len(errorPrefix) &&
			errorPrefix == format[:len(errorPrefix)] {

			panic("Stuck in infinite recursion due to format length")
		}

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

	rtn.header.Size = uint32(len(mapBuffer))

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
	header.Version = MmapHeaderVersion
	header.StrMapSize = mmapStrMapSize
	headerOffset := int(unsafe.Sizeof(MmapHeader{}))
	rtn.circBuf = newCircBuf(&header.CircBuf,
		mmap[headerOffset:headerOffset+circBufSize])
	rtn.strIdMap = newIdStrMap(mmap, headerOffset+circBufSize)
	rtn.errOut = errOut

	return &rtn
}

func (strMap *IdStrMap) mapGetLogIdx(format string) *uint16 {

	strMap.mapLock.RLock()
	defer strMap.mapLock.RUnlock()

	entry, ok := strMap.data[format]
	if ok {
		return &entry
	}

	return nil
}

func (strMap *IdStrMap) writeMapEntry(idx LogSubsystem, level uint8,
	format string) (uint16, error) {

	strMap.mapLock.Lock()
	defer strMap.mapLock.Unlock()

	// Re-check for existing key now that we have the lock to avoid races
	existingId, ok := strMap.data[format]
	if ok {
		return existingId, nil
	}

	newIdx := strMap.freeIdx
	strMap.freeIdx++

	strMap.data[format] = newIdx
	var err error
	strMap.buffer[newIdx], err = newLogStr(idx, level, format)

	return newIdx, err
}

func (strMap *IdStrMap) fetchLogIdx(idx LogSubsystem, level uint8,
	format string) (uint16, error) {

	existingId := strMap.mapGetLogIdx(format)

	if existingId != nil {
		return *existingId, nil
	}

	return strMap.writeMapEntry(idx, level, format)
}

type LogPrimitive interface {
	// Return the type cast to a primitive, in interface form so that
	// type asserts will work
	Primitive() interface{}
}

const (
	TypeInt8Pointer = 1
	TypeInt8 = 2
	TypeUint8Pointer = 3
	TypeUint8 = 4
	TypeInt16Pointer = 5
	TypeInt16 = 6
	TypeUint16Pointer = 7
	TypeUint16 = 8
	TypeInt32Pointer = 9
	TypeInt32 = 10
	TypeUint32Pointer = 11
	TypeUint32 = 12
	TypeInt64Pointer = 13
	TypeInt64 = 14
	TypeUint64Pointer = 15
	TypeUint64 = 16
	TypeString = 17
	TypeByteArray = 18
)

// Writes the data, with a type prefix field two bytes long
func (mem *SharedMemory) binaryWrite(w io.Writer, input interface{}, format string) {
	data := input

	// Handle primitive aliases first
	if prim, ok := data.(LogPrimitive); ok {
		// This takes the alias and provides a base class via an interface{}
		// Without this, type casting will check against the alias instead
		// of the base type
		data = prim.Primitive()
	}

	needDataWrite := true
	var byteType uint16
	switch v := data.(type) {
	case *int8:
		byteType = TypeInt8Pointer
	case int8:
		byteType = TypeInt8
	case *uint8:
		byteType = TypeUint8Pointer
	case uint8:
		byteType = TypeUint8
	case *int16:
		byteType = TypeInt16Pointer
	case int16:
		byteType = TypeInt16
	case *uint16:
		byteType = TypeUint16Pointer
	case uint16:
		byteType = TypeUint16
	case *int32:
		byteType = TypeInt32Pointer
	case int32:
		byteType = TypeInt32
	case *uint32:
		byteType = TypeUint32Pointer
	case uint32:
		byteType = TypeUint32
	case int:
		byteType = TypeInt64
		data = interface{}(int64(v))
	case uint:
		byteType = TypeUint64
		data = interface{}(uint64(v))
	case *int64:
		byteType = TypeInt64Pointer
	case int64:
		byteType = TypeInt64
	case *uint64:
		byteType = TypeUint64Pointer
	case uint64:
		byteType = TypeUint64
	case string:
		writeArray(w, format, []byte(v), TypeString)
		return
	case []byte:
		writeArray(w, format, v, TypeByteArray)
		return
	default:
		needDataWrite = false
	}

	if needDataWrite {
		err := binary.Write(w, binary.LittleEndian, byteType)
		if err != nil {
			panic("Unable to write basic type to memory")
		}

		err = binary.Write(w, binary.LittleEndian, data)
		if err != nil {
			panic(fmt.Sprintf("Unable to write basic type data : %v",
				err))
		}
	} else {
		errorPrefix := "WARN: LogConverter needed for %s:\n%s\n"

		// Since we're going to do something recursive, ensure we're not
		// already recursing and something horrible is happening.
		if len(format) >= len(errorPrefix) &&
			errorPrefix == format[:len(errorPrefix)] {

			panic("Stuck in impossible infinite recursion")
		}

		str := fmt.Sprintf("%v", data)
		writeArray(w, format, []byte(str), 17)
		mem.errOut.Log(LogQlog, QlogReqId, 1, errorPrefix,
			reflect.ValueOf(data).String(), string(debug.Stack()))
	}
}

func writeArray(w io.Writer, format string, output []byte, byteType uint16) {
	if len(output) > math.MaxUint16 {
		panic(fmt.Sprintf("String len > 65535 unsupported: "+
			"%s", format))
	}
	err := binary.Write(w, binary.LittleEndian, byteType)
	if err != nil {
		panic("Unable to write array type to memory")
	}

	byteLen := uint16(len(output))
	err = binary.Write(w, binary.LittleEndian, byteLen)
	if err != nil {
		panic("Unable to write array length to memory")
	}

	err = binary.Write(w, binary.LittleEndian, output)
	if err != nil {
		panic(fmt.Sprintf("Unable to write array to mem: "+
			"%v", err))
	}
}

func (mem *SharedMemory) generateLogEntry(strMapId uint16, reqId uint64,
	timestamp int64, format string, args ...interface{}) []byte {

	buf := new(bytes.Buffer)
	// Two bytes prefix for the total packet length, before the num of args.
	// Write the log entry header with no prefixes
	binary.Write(buf, binary.LittleEndian, uint16(len(args)))
	binary.Write(buf, binary.LittleEndian, strMapId)
	binary.Write(buf, binary.LittleEndian, reqId)
	binary.Write(buf, binary.LittleEndian, timestamp)

	for i := 0; i < len(args); i++ {
		mem.binaryWrite(buf, args[i], format)
	}

	// Make sure length isn't too long, excluding the size bytes
	if buf.Len() > math.MaxUint16 {
		errorPrefix := "Log data exceeds allowable length: %s\n"

		// Ensure we're not already recursing
		if len(format) >= len(errorPrefix) &&
			errorPrefix == format[:len(errorPrefix)] {

			panic("Stuck in impossible infinite recursion from length")
		}

		mem.errOut.Log(LogQlog, reqId, 1, errorPrefix, format)

		buf.Truncate(math.MaxUint16)
	}

	return buf.Bytes()
}

func (mem *SharedMemory) logEntry(idx LogSubsystem, reqId uint64, level uint8,
	timestamp int64, format string, args ...interface{}) {

	// Create the string map entry / fetch existing one
	strId, err := mem.strIdMap.fetchLogIdx(idx, level, format)
	if err != nil {
		mem.errOut.Log(LogQlog, reqId, 1, err.Error() + ": %s\n", format)
		return
	}

	// Generate the byte array packet
	data := mem.generateLogEntry(strId, reqId, timestamp, format, args...)

	mem.circBuf.writeData(data)
}
