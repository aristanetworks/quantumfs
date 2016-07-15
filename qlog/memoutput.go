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
import "unsafe"
import "reflect"
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
	data		map[string]uint16
	mapLock		sync.RWMutex

	buffer		*[mmapStrMapSize/logStrSize]LogStr
	freeIdx		uint16
}

func newCircBuf(buf []byte) CircMemLogs {
	return CircMemLogs {
		buffer:	buf,
	}
}

func newIdStrMap(buf *[mmapTotalSize]byte, offset int) IdStrMap {
	var rtn IdStrMap
	rtn.data = make(map[string]uint16)
	rtn.freeIdx = 0
	rtn.buffer = (*[mmapStrMapSize/
		logStrSize]LogStr)(unsafe.Pointer(&buf[offset]))

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

type LogConverter interface {
	// Return the two byte type number
	ObjType() uint16

	// Return the object's data
	Data() []byte
}

func (mem *SharedMemory) binaryWrite(w io.Writer, input interface{}, format string) {
	data := input

	// Handle primitive aliases first
	if prim, ok := data.(LogPrimitive); ok {
		// This takes the alias and provides a base class via an interface{}
		// Without this, type casting will check against the alias instead
		// of the base type
		data = prim.Primitive()
	}

	handledWrite := true
	byteType := make([]byte, 2)
	switch v := data.(type) {
		case *int8:
			byteType[0] = 1
		case int8:
			byteType[0] = 2
		case []int8:
			byteType[0] = 3
		case *uint8:
			byteType[0] = 4
		case uint8:
			byteType[0] = 5
		case []uint8:
			byteType[0] = 6
		case *int16:
			byteType[0] = 7
		case int16:
			byteType[0] = 8
		case []int16:
			byteType[0] = 9
		case *uint16:
			byteType[0] = 10
		case uint16:
			byteType[0] = 11
		case []uint16:
			byteType[0] = 12
		case *int32:
			byteType[0] = 13
		case int32:
			byteType[0] = 14
		case []int32:
			byteType[0] = 15
		case *uint32:
			byteType[0] = 16
		case uint32:
			byteType[0] = 17
		case []uint32:
			byteType[0] = 18
		case int:
			byteType[0] = 14
			data = interface{}(int32(v))
		case uint:
			byteType[0] = 17
			data = interface{}(uint32(v))
		case *int64:
			byteType[0] = 19
		case int64:
			byteType[0] = 20
		case []int64:
			byteType[0] = 21
		case *uint64:
			byteType[0] = 22
		case uint64:
			byteType[0] = 23
		case []uint64:
			byteType[0] = 24
		default:
			handledWrite = false
	}

	if handledWrite {
		count, err := w.Write(byteType)
		if err != nil || count != len(byteType) {
			panic("Unable to write basic type to memory")
		}

		err = binary.Write(w, binary.LittleEndian, data)
		if err != nil {
			panic(fmt.Sprintf("Unable to write basic type data : %v",
				err))
		}
	} else if v, ok := data.(string); ok {
		writeString(w, format, v)
	} else if v, ok := data.(LogConverter); ok {
		writeData := append(make([]byte, 2), v.Data()...)
		*(*uint16)(unsafe.Pointer(&writeData[0])) = v.ObjType()
		count, err := w.Write(writeData)
		if err != nil || count != len(writeData) {
			panic(fmt.Sprintf("Unable to write type to memory: %s",
				reflect.ValueOf(data).String()))
		}
	} else {
		str := fmt.Sprintf("%v", data)
		writeString(w, format, str)
		errorStr := fmt.Sprintf("WARN: LogConverter needed for type %s\n",
			reflect.ValueOf(data).String())
		(*mem.errOut)("%s", errorStr)
	}
}

func writeString(w io.Writer, format string, output string) {
	if len(output) > math.MaxUint16 {
		panic(fmt.Sprintf("String len > 65535 unsupported: " +
			"%s", format))
	}
	byteType := make([]byte, 1)
	byteType[0] = 25
	count, err := w.Write(byteType)
	if err != nil || count != len(byteType) {
		panic("Unable to write string type to memory")
	}

	byteLen := make([]byte, 2)
	strLen := (*uint16)(unsafe.Pointer(&byteLen[0]))
	*strLen = uint16(len(output))
	count, err = w.Write(byteLen)
	if err != nil || count != len(byteLen) {
		panic("Unable to write string length to memory")
	}

	count, err = w.Write([]byte(output))
	if err != nil || count != len(output) {
		panic(fmt.Sprintf("Unable to write string to mem: " +
			"%v", err))
	}
}

func (mem *SharedMemory) generateLogEntry(strMapId uint16, reqId uint64,
	timestamp int64, format string, args ...interface{}) []byte {

	buf := new(bytes.Buffer)
	mem.binaryWrite(buf, strMapId, format)
	mem.binaryWrite(buf, reqId, format)
	mem.binaryWrite(buf, timestamp, format)

	for i := 0; i < len(args); i++ {
		mem.binaryWrite(buf, args[i], format)
	}

	// Create the two byte packet length header at the front
	rtn := make([]byte, buf.Len() + 2)
	copy(rtn[2:], buf.Bytes())
	if len(rtn) > math.MaxUint16 {
		rtn = rtn[:math.MaxUint16]
	}
	lenField := (*uint16)(unsafe.Pointer(&rtn[0]))
	*lenField = uint16(len(rtn))

	return rtn
}

func writeLogEntry(data []byte) {

}

func (mem *SharedMemory) logEntry(idx LogSubsystem, reqId uint64, level uint8,
	timestamp int64, format string, args ...interface{}) {

	// Create the string map entry / fetch existing one
	strId := mem.strIdMap.fetchLogIdx(idx, level, format)

	// Generate the byte array packet
	data := mem.generateLogEntry(strId, reqId, timestamp, format, args...)

	// Write it into the shared memory carefully
	writeLogEntry(data)
}
