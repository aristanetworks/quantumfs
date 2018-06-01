// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is a shared memory log parser for the qlog quantumfs subsystem.
// It is used for parsing a qlog file completely in a single pass
package qlog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

// consts
const defaultParseThreads = 30
const clippedMsg = "Packet has been clipped."

var clippedError error

func init() {
	clippedError = errors.New(clippedMsg)
}

func newLog(s LogSubsystem, r uint64, t int64, f string,
	args []interface{}) LogOutput {

	return LogOutput{
		Subsystem: s,
		ReqId:     r,
		T:         t,
		Format:    f,
		Args:      args,
	}
}

type WriteFn func(string, ...interface{}) (int, error)

func readOrPanic(offset int64, len int64, fd *os.File) []byte {
	raw := make([]byte, len)
	readCount := int64(0)
	for readCount < len {
		sz, err := fd.ReadAt(raw[readCount:], offset+readCount)
		if err != nil {
			panic("Unable to read log file data")
		}

		readCount += int64(sz)
	}

	return raw
}

func extractStrMapData(filepath string) []byte {
	// Extract just the strMapData, and avoid having to unecessarily read
	// the rest of the potentially giant log file
	file, err := os.Open(filepath)
	if err != nil {
		panic(fmt.Sprintf("Opening file strMapData file %s failed: %v",
			filepath, err))
	}
	defer file.Close()

	headerLen := int64(unsafe.Sizeof(mmapHeader{}))
	headerRaw := readOrPanic(0, headerLen, file)
	header := (*mmapHeader)(unsafe.Pointer(&headerRaw[0]))

	return readOrPanic(headerLen+int64(header.CircBuf.Size),
		int64(header.StrMapSize), file)
}

func grabMemory(filepath string) []byte {
	if filepath == "" {
		return nil
	}

	fileData, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(fmt.Sprintf("Unable to read shared memory file data from %s\n",
			filepath))
	}

	return fileData
}

func parseArg(idx *uint64, data []byte) (interface{}, error) {
	if len(data[*idx:]) < 2 {
		return nil, clippedError
	}
	var byteType uint16
	byteType = *(*uint16)(unsafe.Pointer(&data[*idx]))
	*idx += 2

	handledWrite := true
	var rtn reflect.Value
	switch byteType {
	case TypeInt8Pointer:
		var tmp *int8
		rtn = reflect.ValueOf(&tmp)
	case TypeInt8:
		var tmp int8
		rtn = reflect.ValueOf(&tmp)
	case TypeUint8Pointer:
		var tmp *uint8
		rtn = reflect.ValueOf(&tmp)
	case TypeUint8:
		var tmp uint8
		rtn = reflect.ValueOf(&tmp)
	case TypeInt16Pointer:
		var tmp *int16
		rtn = reflect.ValueOf(&tmp)
	case TypeInt16:
		var tmp int16
		rtn = reflect.ValueOf(&tmp)
	case TypeUint16Pointer:
		var tmp *uint16
		rtn = reflect.ValueOf(&tmp)
	case TypeUint16:
		var tmp uint16
		rtn = reflect.ValueOf(&tmp)
	case TypeInt32Pointer:
		var tmp *int32
		rtn = reflect.ValueOf(&tmp)
	case TypeInt32:
		var tmp int32
		rtn = reflect.ValueOf(&tmp)
	case TypeUint32Pointer:
		var tmp *uint32
		rtn = reflect.ValueOf(&tmp)
	case TypeUint32:
		var tmp uint32
		rtn = reflect.ValueOf(&tmp)
	case TypeInt64Pointer:
		var tmp *int64
		rtn = reflect.ValueOf(&tmp)
	case TypeInt64:
		var tmp int64
		rtn = reflect.ValueOf(&tmp)
	case TypeUint64Pointer:
		var tmp *uint64
		rtn = reflect.ValueOf(&tmp)
	case TypeUint64:
		var tmp uint64
		rtn = reflect.ValueOf(&tmp)
	case TypeBoolean:
		var tmp uint8
		rtn = reflect.ValueOf(&tmp)
	default:
		handledWrite = false
	}

	var err error
	if handledWrite {
		err = readPacket(idx, data, rtn)
		if err != nil {
			return nil, err
		}

		if byteType == TypeBoolean {
			val := rtn.Elem().Interface().(uint8)
			if val == 0 {
				return false, nil
			} else {
				return true, nil
			}
		}

		return rtn.Elem().Interface(), nil
	}

	if byteType == TypeString || byteType == TypeByteArray {
		if len(data[*idx:]) < 2 {
			return nil, clippedError
		}
		var strLen uint16
		strLen = *(*uint16)(unsafe.Pointer(&data[*idx]))
		*idx += 2

		var substr []byte
		if *idx < uint64(len(data)) {
			substr = data[*idx:]

			if strLen < uint16(len(substr)) {
				substr = substr[:strLen]
			}
		}
		*idx += uint64(len(substr))

		if byteType == TypeString {
			return string(substr), nil
		}

		// Otherwise, return []byte if type is TypeByteArray
		return substr, nil
	}

	return nil, errors.New(fmt.Sprintf("Unsupported field type %d\n", byteType))
}

func wrapRead(idx uint64, num uint64, data []byte) []byte {
	rtn := make([]byte, num)

	if idx+num > uint64(len(data)) {
		secondNum := (idx + num) - uint64(len(data))
		num -= secondNum
		copy(rtn[num:], data[0:secondNum])
	}

	copy(rtn, data[idx:idx+num])

	return rtn
}

func wrapPlusEquals(lhs *uint64, rhs uint64, bufLen uint64) {
	*lhs += rhs

	if *lhs > bufLen {
		*lhs -= bufLen
	}
}

func wrapMinus(lhs uint64, rhs uint64, bufLen uint64) uint64 {
	wrapMinusEquals(&lhs, rhs, bufLen)
	return lhs
}

func wrapMinusEquals(lhs *uint64, rhs uint64, bufLen uint64) {
	if *lhs < rhs {
		*lhs += uint64(bufLen)
	}

	*lhs -= rhs
}

// outputType is an instance of that same type as output, output *must* be a pointer
// to a variable of that type for the data to be placed into.
// PastIdx is the index of the element just *past* what we want to read
func ReadBack(pastIdx *uint64, data []byte, outputType interface{},
	output interface{}) {

	dataLen := uint64(reflect.TypeOf(outputType).Size())

	wrapMinusEquals(pastIdx, dataLen, uint64(len(data)))
	rawData := wrapRead(*pastIdx, dataLen, data)

	buf := bytes.NewReader(rawData)
	err := binary.Read(buf, binary.LittleEndian, output)
	if err != nil {
		panic("Unable to binary read from data")
	}
}

// Returns the number of bytes read
func readPacket(idx *uint64, data []byte, output reflect.Value) error {

	dataLen := uint64(output.Elem().Type().Size())

	if dataLen+*idx > uint64(len(data)) {
		return errors.New(fmt.Sprintf(clippedMsg+" (%d %d %d)",
			dataLen, *idx, len(data)))
	}

	buf := bytes.NewReader(data[*idx : *idx+dataLen])
	err := binary.Read(buf, binary.LittleEndian, output.Interface())
	if err != nil {
		return err
	}

	*idx += dataLen

	return nil
}

type LogStatus struct {
	shownHeader  bool
	pixWidth     int
	lastPixShown int
}

func processJobs(jobs <-chan logJob, wg *sync.WaitGroup) {
	for j := range jobs {
		packetData := j.packetData
		strMap := j.strMap
		out := j.out

		read := uint64(0)
		var numFields uint16
		var strMapId uint16
		var reqId uint64
		var timestamp int64

		var err error
		if err = readPacket(&read, packetData,
			reflect.ValueOf(&numFields)); err != nil {
		} else if err = readPacket(&read, packetData,
			reflect.ValueOf(&strMapId)); err != nil {
		} else if err = readPacket(&read, packetData,
			reflect.ValueOf(&reqId)); err != nil {
		} else if err = readPacket(&read, packetData,
			reflect.ValueOf(&timestamp)); err != nil {
		}

		args := make([]interface{}, numFields)
		for i := uint16(0); i < numFields; i++ {
			if err != nil {
				break
			}

			args[i], err = parseArg(&read, packetData)
		}

		if err != nil {
			// If the timestamp is zero, we will fill it in later with
			// the previous log's timestamp
			prefix := "ERROR"
			if err == clippedError {
				prefix = "WARN"
			}
			*out = newLog(LogQlog, QlogReqId, 0,
				prefix+": Packet read error (%s). "+
					"Dump of %d bytes:\n%x\n",
				[]interface{}{err, len(packetData),
					packetData})
			continue
		}

		// Grab the string and output
		if int(strMapId) >= len(strMap) {
			*out = newLog(LogQlog, QlogReqId, 0,
				"Not enough entries in "+
					"string map (%d %d)\n",
				[]interface{}{strMapId,
					len(strMap) / LogStrSize})
			continue
		}
		mapEntry := strMap[strMapId]
		logSubsystem := (LogSubsystem)(mapEntry.LogSubsystem)

		// Finally, print with the front attached like normal
		mapStr := string(mapEntry.Text[:])
		firstNullTerm := strings.Index(mapStr, "\x00")
		if firstNullTerm != -1 {
			mapStr = mapStr[:firstNullTerm]
		}

		*out = newLog(logSubsystem, reqId, timestamp,
			mapStr+"\n", args)
	}
	wg.Done()
}

type logJob struct {
	packetData []byte
	strMap     []logStr
	out        *LogOutput
}

func outputLogPtrs(pastEndIdx uint64, data []byte, strMap []logStr, maxWorkers int,
	printStatus bool) []*LogOutput {

	var logPtrs []*LogOutput
	readCount := uint64(0)

	jobs := make(chan logJob, 2)
	var wg sync.WaitGroup

	wg.Add(maxWorkers)
	for w := 0; w < maxWorkers; w++ {
		go processJobs(jobs, &wg)
	}

	if printStatus {
		fmt.Printf("Parsing logs in %d threads...\n",
			maxWorkers)
	}
	status := NewLogStatus(50)

	for readCount < uint64(len(data)) {
		var packetLen uint16
		ReadBack(&pastEndIdx, data, packetLen, &packetLen)

		// If we read a packet of zero length, that means our buffer wasn't
		// full and we've hit the unused area
		if packetLen == 0 {
			break
		}

		// If the packet's uppermost len bit isn't set, that means it's not
		// fully written and needs to be discarded
		completeEntry := (packetLen&EntryCompleteBit != 0)

		// Now clear the upper bit so it doesn't mess up our packetLen
		packetLen &= ^(uint16(EntryCompleteBit))

		// Prepare the pastEndIdx and readCount variables to allow us to skip
		wrapMinusEquals(&pastEndIdx, uint64(packetLen), uint64(len(data)))
		readCount += uint64(packetLen) + 2

		// Update a status bar if needed
		if printStatus {
			readCountClip := uint64(readCount)
			if readCountClip > uint64(len(data)) {
				readCountClip = uint64(len(data))
			}
			status.Process(float32(readCountClip) / float32(len(data)))
		}

		if readCount > uint64(len(data)) {
			// We've read everything, and this last packet isn't valid
			break
		}

		// At this point we know the packet is fully present, finished, and
		// the idx / readCounts have been updated in prep for the next entry
		if !completeEntry {
			newLine := newLog(LogQlog, QlogReqId, 0,
				"WARN: Dropping incomplete packet.\n", nil)
			logPtrs = append(logPtrs, &newLine)
			continue
		}

		// Read the packet data into a separate buffer
		packetData := wrapRead(pastEndIdx, uint64(packetLen), data)

		logPtrs = append(logPtrs, new(LogOutput))
		jobs <- logJob{
			packetData: packetData,
			strMap:     strMap,
			out:        logPtrs[len(logPtrs)-1],
		}
	}
	close(jobs)
	wg.Wait()
	if printStatus {
		status.Process(1)
	}

	return logPtrs
}
