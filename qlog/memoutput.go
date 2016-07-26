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

// Circ buf size for approx 1 hour of LogEntries at 1000 a second
const mmapCircBufSize = 360000 * 24
// Strmap size allows for up to ~10000 unique logs
const mmapStrMapSize = 512 * 1024
// This must include the MmapHeader len
const mmapTotalSize = mmapCircBufSize + mmapStrMapSize + MmapHeaderSize

// This header will be at the beginning of the shared memory region, allowing
// this spec to change over time, but still ensuring a memory dump is self contained
const MmapHeaderSize = 16
type MmapHeader struct {
	StrMapSize	uint32
	CircBuf		circBufHeader
}

type circBufHeader struct {
	Size		uint32

	// These variables indicate the front and end of the circular buffer
	FrontIdx	uint32
	PastEndIdx	uint32
}

type SharedMemory struct {
	fd		*os.File
	buffer		*[mmapTotalSize]byte
	circBuf		CircMemLogs
	strIdMap	IdStrMap
	errOut		*func(format string, args ...interface{}) (int, error)
}

// Average Log Entry should be ~24 bytes
type LogEntry struct {
	strIdx		uint16
	reqId		uint64
	timestamp	int64
	vars		[]interface{}
}

type CircMemLogs struct {
	header		*circBufHeader
	buffer 		*[mmapCircBufSize]byte
	writer		chan []byte
}

func (circ *CircMemLogs) Size() int {
	return mmapCircBufSize + MmapHeaderSize
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) curLen_() uint32 {
	// Is the end ahead of us, or has it wrapped?
	if circ.header.PastEndIdx >= circ.header.FrontIdx {
		return circ.header.PastEndIdx - circ.header.FrontIdx
	} else {
		return (mmapCircBufSize - circ.header.FrontIdx) + 1 +
			circ.header.PastEndIdx
	}
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) wrapRead_(idx uint32, num uint32) []byte {
	rtn := make([]byte, num)

	if idx + num > mmapCircBufSize {
		secondNum := (idx + num) - mmapCircBufSize
		num -= secondNum
		copy(rtn[num:], circ.buffer[0:secondNum])
	}

	copy(rtn, circ.buffer[idx:idx+num])

	return rtn
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) wrapPlusEquals_(lhs *uint32, addon uint32) {
	if *lhs + addon < mmapCircBufSize {
		*lhs = *lhs + addon
		return
	}

	*lhs = (*lhs + addon) - mmapCircBufSize
}

// Must only be called from inside writeLogEntries
func (circ *CircMemLogs) wrapWrite_(idx uint32, data []byte) {
	numWrite := uint32(len(data))
	if idx + numWrite > mmapCircBufSize {
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
	Text		[logTextMax]byte
	LogSubsystem	uint8
	LogLevel	uint8
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
	data		map[string]uint16
	mapLock		sync.RWMutex

	buffer		*[mmapStrMapSize/LogStrSize]LogStr
	freeIdx		uint16
}

func newCircBuf(mapHeader *circBufHeader,
	mapBuffer *[mmapCircBufSize]byte) CircMemLogs {

	rtn := CircMemLogs {
		header:		mapHeader,
		buffer:		mapBuffer,
		writer:		make(chan []byte),
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
	rtn.buffer = (*[mmapStrMapSize/
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

	mapFile, err := os.OpenFile(dir + "/" + filename,
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

	var rtn SharedMemory
	rtn.fd = mapFile
	rtn.buffer = (*[mmapTotalSize]byte)(unsafe.Pointer(&mmap[0]))
	header := (*MmapHeader)(unsafe.Pointer(&rtn.buffer[0]))
	header.StrMapSize = mmapStrMapSize
	rtn.circBuf = newCircBuf(&header.CircBuf,
		(*[mmapCircBufSize]byte)(unsafe.Pointer(&rtn.buffer[MmapHeaderSize])))
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
			byteType[0] = 1
		case int8:
			byteType[0] = 2
		case *uint8:
			byteType[0] = 3
		case uint8:
			byteType[0] = 4
		case *int16:
			byteType[0] = 5
		case int16:
			byteType[0] = 6
		case *uint16:
			byteType[0] = 7
		case uint16:
			byteType[0] = 8
		case *int32:
			byteType[0] = 9
		case int32:
			byteType[0] = 10
		case *uint32:
			byteType[0] = 11
		case uint32:
			byteType[0] = 12
		case int:
			byteType[0] = 10
			data = interface{}(int32(v))
		case uint:
			byteType[0] = 12
			data = interface{}(uint32(v))
		case *int64:
			byteType[0] = 13
		case int64:
			byteType[0] = 14
		case *uint64:
			byteType[0] = 15
		case uint64:
			byteType[0] = 16
		case string:
			writeArray(w, format, []byte(v), 17)
			return
		case []byte:
			writeArray(w, format, v, 18)
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
		panic(fmt.Sprintf("String len > 65535 unsupported: " +
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
		panic(fmt.Sprintf("Unable to write array to mem: " +
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
	rtn := make([]byte, buf.Len() + 2)
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
