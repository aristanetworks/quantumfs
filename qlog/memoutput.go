// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging shared memory support
import "bytes"
import "encoding/binary"
import "fmt"
import "io"
import "math"
import "os"
import "reflect"
import "runtime/debug"
import "sync"
import "syscall"
import "unsafe"

const writerBufferBytes = 64 * 1024

// Circular buf size for approx 1 hour of LogEntries at 1000 a second
const mmapCircBufSize = 360000 * 24

// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024

// This must include the MmapHeader len
const mmapTotalSize = mmapCircBufSize + mmapStrMapSize + MmapHeaderSize

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
const MmapHeaderSize = 20
const MmapHeaderVersion = 1

type MmapHeader struct {
	Version		uint32
	StrMapSize	uint32
	CircBuf		circBufHeader
}

type circBufHeader struct {
	Size uint32

	// These variables indicate the front and end of the circular buffer
	FrontIdx   uint32
	PastEndIdx uint32
}

type SharedMemory struct {
	fd       *os.File
	buffer   *[mmapTotalSize]byte
	circBuf  CircMemLogs
	strIdMap IdStrMap
	errOut   *func(format string, args ...interface{}) (int, error)
}

// Average Log Entry should be ~24 bytes
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
	buffer *[mmapCircBufSize]byte
	writer chan []byte
}

func (circ *CircMemLogs) Size() int {
	return mmapCircBufSize + MmapHeaderSize
}

func wrapLen(frontIdx uint32, pastEndIdx uint32, dataLen uint32) uint32 {
	// Is the end ahead of us, or has it wrapped?
	if pastEndIdx >= frontIdx {
		return pastEndIdx - frontIdx
	} else {
		return (dataLen - frontIdx) + 1 + pastEndIdx
	}
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) curLen_() uint32 {
	return wrapLen(circ.header.FrontIdx, circ.header.PastEndIdx, mmapCircBufSize)
}

func WrapRead(idx uint32, num uint32, data []byte) []byte {
	return wrapRead(idx, num, data)
}

func wrapRead(idx uint32, num uint32, data []byte) []byte {
	rtn := make([]byte, num)

	if idx+num > uint32(len(data)) {
		secondNum := (idx + num) - uint32(len(data))
		num -= secondNum
		copy(rtn[num:], data[0:secondNum])
	}

	copy(rtn, data[idx:idx+num])

	return rtn
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) wrapRead_(idx uint32, num uint32) []byte {
	return wrapRead(idx, num, circ.buffer[:])
}

func WrapPlusEquals(lhs *uint32, addon uint32, bufLen int) {
	wrapPlusEquals(lhs, addon, bufLen)
}

func wrapPlusEquals(lhs *uint32, addon uint32, bufLen int) {
	if *lhs+addon < uint32(bufLen) {
		*lhs = *lhs + addon
		return
	}

	*lhs = (*lhs + addon) - uint32(bufLen)
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) wrapPlusEquals_(lhs *uint32, addon uint32) {
	wrapPlusEquals(lhs, addon, mmapCircBufSize)
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) wrapWrite_(idx uint32, data []byte) {
	numWrite := uint32(len(data))
	if idx+numWrite > mmapCircBufSize {
		secondNum := (idx + numWrite) - mmapCircBufSize
		numWrite -= secondNum
		copy(circ.buffer[0:secondNum], data[numWrite:])
	}

	copy(circ.buffer[idx:idx+numWrite], data[:numWrite])
}

func (circ *CircMemLogs) writeLogEntries() {
	// We need a dedicated thread writing to the buffer since memory copy can
	// be slow and we need to ensure the consistency of the header section
	for {
		data := <-circ.writer

		// For now, if the message is too long then just toss it
		if len(data) > mmapCircBufSize {
			continue
		}

		// Do we need to throw away some old entries to make space?
		newLen := uint32(len(data)) + circ.curLen_()
		for newLen > mmapCircBufSize {
			entryLen := circ.wrapRead_(circ.header.FrontIdx, 2)
			packetLen := (*uint16)(unsafe.Pointer(&entryLen[0]))
			circ.wrapPlusEquals_(&circ.header.FrontIdx,
				uint32(*packetLen))
			newLen -= uint32(*packetLen)
		}

		// Now that we know we have space, write in the entry
		circ.wrapWrite_(circ.header.PastEndIdx, data)

		// Lastly, now that the data is all there, move the end back
		circ.wrapPlusEquals_(&circ.header.PastEndIdx, uint32(len(data)))
	}
}

const LogStrSize = 64
const logTextMax = 62

type LogStr struct {
	Text         [logTextMax]byte
	LogSubsystem uint8
	LogLevel     uint8
}

func newLogStr(idx LogSubsystem, level uint8, format string) LogStr {
	var rtn LogStr
	rtn.LogSubsystem = uint8(idx)
	rtn.LogLevel = level
	copyLen := len(format)
	if copyLen > logTextMax {
		fmt.Printf("Log format string exceeds allowable length: %s\n",
			format)
		copyLen = logTextMax
	}
	copy(rtn.Text[:], format[:copyLen])

	return rtn
}

type IdStrMap struct {
	data    map[string]uint16
	mapLock sync.RWMutex

	buffer  *[mmapStrMapSize / LogStrSize]LogStr
	freeIdx uint16
}

func newCircBuf(mapHeader *circBufHeader,
	mapBuffer *[mmapCircBufSize]byte) CircMemLogs {

	rtn := CircMemLogs{
		header: mapHeader,
		buffer: mapBuffer,
		writer: make(chan []byte, writerBufferBytes),
	}

	rtn.header.Size = mmapCircBufSize

	// Launch the asynchronous shared memory writer
	go rtn.writeLogEntries()
	return rtn
}

func newIdStrMap(buf *[mmapTotalSize]byte, offset int) IdStrMap {
	var rtn IdStrMap
	rtn.data = make(map[string]uint16)
	rtn.freeIdx = 0
	rtn.buffer = (*[mmapStrMapSize /
		LogStrSize]LogStr)(unsafe.Pointer(&buf[offset]))

	return rtn
}

func newSharedMemory(dir string, filename string, errOut *func(format string,
	args ...interface{}) (int, error)) *SharedMemory {

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

	// Make sure we touch every byte to ensure that the mmap isn't sparse
	for i := 0; i < mmapTotalSize; i++ {
		mmap[i] = 0
	}

	var rtn SharedMemory
	rtn.fd = mapFile
	rtn.buffer = (*[mmapTotalSize]byte)(unsafe.Pointer(&mmap[0]))
	header := (*MmapHeader)(unsafe.Pointer(&rtn.buffer[0]))
	header.Version = MmapHeaderVersion
	header.StrMapSize = mmapStrMapSize
	rtn.circBuf = newCircBuf(&header.CircBuf,
		(*[mmapCircBufSize]byte)(unsafe.Pointer(
			&rtn.buffer[MmapHeaderSize])))
	rtn.strIdMap = newIdStrMap(rtn.buffer, rtn.circBuf.Size())
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
	format string) uint16 {

	strMap.mapLock.Lock()
	defer strMap.mapLock.Unlock()

	// Re-check for existing key now that we have the lock to avoid races
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
	format string) uint16 {

	existingId := strMap.mapGetLogIdx(format)

	if existingId != nil {
		return *existingId
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
	byteType := make([]byte, 2)
	switch v := data.(type) {
	case *int8:
		byteType[0] = TypeInt8Pointer
	case int8:
		byteType[0] = TypeInt8
	case *uint8:
		byteType[0] = TypeUint8Pointer
	case uint8:
		byteType[0] = TypeUint8
	case *int16:
		byteType[0] = TypeInt16Pointer
	case int16:
		byteType[0] = TypeInt16
	case *uint16:
		byteType[0] = TypeUint16Pointer
	case uint16:
		byteType[0] = TypeUint16
	case *int32:
		byteType[0] = TypeInt32Pointer
	case int32:
		byteType[0] = TypeInt32
	case *uint32:
		byteType[0] = TypeUint32Pointer
	case uint32:
		byteType[0] = TypeUint32
	case int:
		byteType[0] = TypeInt64
		data = interface{}(int64(v))
	case uint:
		byteType[0] = TypeUint64
		data = interface{}(uint64(v))
	case *int64:
		byteType[0] = TypeInt64Pointer
	case int64:
		byteType[0] = TypeInt64
	case *uint64:
		byteType[0] = TypeUint64Pointer
	case uint64:
		byteType[0] = TypeUint64
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
		str := fmt.Sprintf("%v", data)
		writeArray(w, format, []byte(str), 17)
		errorStr := fmt.Sprintf("WARN: LogConverter needed for %s:\n%s\n",
			reflect.ValueOf(data).String(), string(debug.Stack()))
		(*mem.errOut)("%s", errorStr)
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

	// Create the two byte packet length header at the front
	rtn := make([]byte, buf.Len()+2)
	copy(rtn[2:], buf.Bytes())
	if len(rtn) > math.MaxUint16 {
		fmt.Printf("Log data exceeds allowable length: %s\n",
			format)
		rtn = rtn[:math.MaxUint16]
	}
	lenBuf := new(bytes.Buffer)
	binary.Write(lenBuf, binary.LittleEndian, uint16(len(rtn)))
	copy(rtn[:2], lenBuf.Bytes())

	return rtn
}

func (mem *SharedMemory) logEntry(idx LogSubsystem, reqId uint64, level uint8,
	timestamp int64, format string, args ...interface{}) {

	// Create the string map entry / fetch existing one
	strId := mem.strIdMap.fetchLogIdx(idx, level, format)

	// Generate the byte array packet
	data := mem.generateLogEntry(strId, reqId, timestamp, format, args...)

	// Write it into the shared memory asynchronously
	mem.circBuf.writer <- data
}
