// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package qlog

import "bytes"
import "encoding/binary"
import "errors"
import "fmt"
import "io/ioutil"
import "math"
import "os"
import "reflect"
import "sort"
import "strings"
import "time"
import "unsafe"

type LogOutput struct {
	subsystem	LogSubsystem
	reqId		uint64
	t		int64
	format		string
}

func newLog(s LogSubsystem, r uint64, t int64, f string) LogOutput {

	return LogOutput {
		subsystem: s,
		reqId: r,
		t: t,
		format: f,
	}
}

type SortString []string

func (s SortString) Len() int {
	return len(s)
}

func (s SortString) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortString) Less(i, j int) bool {
	return s[i] < s[j]
}

type SortByTime []LogOutput

func (s SortByTime) Len() int {
	return len(s)
}

func (s SortByTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortByTime) Less(i, j int) bool {
	return s[i].t < s[j].t
}

// Returns true if the log file string map given failed the logscan
func LogscanSkim(filepath string) bool {
	strMapData := extractStrMapData(filepath)

	// This takes too much time, so only count one string as failing
	if bytes.Contains(strMapData, []byte("ERROR")) {
		return true
	}

	return false
}

func ParseLogs(filepath string) string {
	return ParseLogsExt(filepath, 0)
}

func ParseLogsExt(filepath string, tabSpaces int) string {

	pastEndIdx, dataArray, strMapData := extractFields(filepath)

	// create a safer map to use
	strMap := make([]LogStr, len(strMapData)/LogStrSize)
	idx := 0
	for i := 0; i+LogStrSize <= len(strMapData); i += LogStrSize {
		mapEntry := (*LogStr)(unsafe.Pointer(&strMapData[i]))
		strMap[idx] = *mapEntry

		idx++
	}

	logs := outputLogs(pastEndIdx, dataArray, strMap, tabSpaces)

	indentMap := make(map[uint64]int)
	var rtn string
	// Now that we have the logs in correct order, we can indent them
	for i := 0; i < len(logs); i++ {
		// Add any indents necessary
		if tabSpaces > 0 {
			indents := indentMap[logs[i].reqId]

			if strings.Index(logs[i].format, FnExitStr) == 0 {
				indents--
			}

			// Add the spaces we need
			spaceStr := ""
			for i := 0; i < indents * tabSpaces; i++ {
				spaceStr += " "
			}

			if strings.Index(logs[i].format, FnEnterStr) == 0 {
				indents++
			}

			logs[i].format = spaceStr + logs[i].format
			indentMap[logs[i].reqId] = indents
		}

		// Convert timestamp back into something useful
		t := time.Unix(0, logs[i].t)

		rtn += formatString(logs[i].subsystem, logs[i].reqId, t,
			logs[i].format)
	}
	return rtn
}

func extractFields(filepath string) (pastEndIdx uint32, dataArray []byte,
	strMapData []byte) {

	data := grabMemory(filepath)
	header := (*MmapHeader)(unsafe.Pointer(&data[0]))

	if header.Version != QlogVersion {
		panic(fmt.Sprintf("Qlog version incompatible: got %d, need %d\n",
			header.Version, QlogVersion))
	}

	mmapHeaderSize := uint32(unsafe.Sizeof(MmapHeader{}))

	if uint32(len(data)) != (header.StrMapSize + header.CircBuf.Size +
		mmapHeaderSize) {
		fmt.Println("Data length inconsistent with expectations. %d",
			len(data))
	}

	return header.CircBuf.PastEndIdx,
		data[mmapHeaderSize : mmapHeaderSize+header.CircBuf.Size],
		data[mmapHeaderSize+header.CircBuf.Size:]
}

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
		panic("Unable to open log file for strMapData read")
	}
	defer file.Close()

	headerLen := int64(unsafe.Sizeof(MmapHeader{}))
	headerRaw := readOrPanic(0, headerLen, file)
	header := (*MmapHeader)(unsafe.Pointer(&headerRaw[0]))

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

func parseArg(idx *uint32, data []byte) (interface{}, error) {
	var byteType uint16
	err := readPacket(idx, data, reflect.ValueOf(&byteType))
	if err != nil {
		return nil, err
	}

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
	default:
		handledWrite = false
	}

	if handledWrite {
		err = readPacket(idx, data, rtn)
		if err != nil {
			return nil, err
		}
		return rtn.Elem().Interface(), nil
	}

	if byteType == TypeString || byteType == TypeByteArray {
		var strLen uint16
		err = readPacket(idx, data, reflect.ValueOf(&strLen))
		if err != nil {
			return nil, err
		}

		var rtnRaw [math.MaxUint16]byte
		err = readPacket(idx, data[:*idx+uint32(strLen)],
			reflect.ValueOf(&rtnRaw))
		if err != nil {
			return nil, err
		}

		if byteType == TypeString {
			return string(rtnRaw[:strLen]), nil
		}

		// Otherwise, return []byte if type is TypeByteArray
		return rtnRaw[:strLen], nil
	}

	return nil, errors.New(fmt.Sprintf("Unsupported field type %d\n", byteType))
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

func wrapMinusEquals(lhs *uint32, rhs uint32, bufLen int) {
	if *lhs < rhs {
		*lhs += uint32(bufLen)
	}

	*lhs -= rhs
}

// outputType is an instance of that same type as output, output *must* be a pointer
// to a variable of that type for the data to be placed into.
// PastIdx is the index of the element just *past* what we want to read
func readBack(pastIdx *uint32, data []byte, outputType interface{},
	output interface{}) {

	dataLen := uint32(reflect.TypeOf(outputType).Size())

	wrapMinusEquals(pastIdx, dataLen, len(data))
	rawData := wrapRead(*pastIdx, dataLen, data)

	buf := bytes.NewReader(rawData)
	err := binary.Read(buf, binary.LittleEndian, output)
	if err != nil {
		panic("Unable to binary read from data")
	}
}

// Returns the number of bytes read
func readPacket(idx *uint32, data []byte, output reflect.Value) error {

	dataLen := uint32(output.Elem().Type().Size())

	// If this is a string, then consume the rest of data provided
	if byteArray, ok := output.Interface().(*[math.MaxUint16]byte); ok {
		dataLen = uint32(len(data)) - *idx
		// Because binary.Read is dumb and can't read less than the given
		// array without EOFing *and* needs a fixed array, we have to do this
		var singleArray [1]byte
		for i := uint32(0); i < dataLen; i++ {
			buf := bytes.NewReader(data[*idx+i : *idx+i+1])
			err := binary.Read(buf, binary.LittleEndian, &singleArray)
			if err != nil {
				return err
			}
			(*byteArray)[i] = singleArray[0]
		}

		*idx += dataLen

		return nil
	}

	if dataLen+*idx > uint32(len(data)) {
		return errors.New(fmt.Sprintf("Packet has been clipped. (%d %d %d)",
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

func outputLogs(pastEndIdx uint32, data []byte, strMap []LogStr,
	tabSpaces int) []LogOutput {

	var rtn []LogOutput
	readCount := uint32(0)
	var lastTimestamp int64

	for readCount < uint32(len(data)) {
		var packetLen uint16
		readBack(&pastEndIdx, data, packetLen, &packetLen)

		// If we read a packet of zero length, that means our buffer wasn't
		// full and we've hit the unused area
		if packetLen == 0 {
			break
		}

		// If the packet's uppermost len bit isn't set, that means it's not
		// fully written and needs to be discarded
		completeEntry := (packetLen&entryCompleteBit != 0)

		// Now clear the upper bit so it doesn't mess up our packetLen
		packetLen &= ^(uint16(entryCompleteBit))

		// Prepare the pastEndIdx and readCount variables to allow us to skip
		wrapMinusEquals(&pastEndIdx, uint32(packetLen), len(data))
		readCount += uint32(packetLen) + 2

		if readCount > uint32(len(data)) {
			// We've read everything, and this last packet isn't valid
			break
		}

		// At this point we know the packet is fully present, finished, and
		// the idx / readCounts have been updated in prep for the next entry
		if !completeEntry {
			newLine := newLog(LogQlog, QlogReqId, lastTimestamp,
				"WARN: Dropping incomplete packet.\n")
			rtn = append(rtn, newLine)
			continue
		}

		// Read the packet data into a separate buffer
		packetData := wrapRead(pastEndIdx, uint32(packetLen), data)

		read := uint32(0)
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
		lastTimestamp = timestamp

		args := make([]interface{}, numFields)
		for i := uint16(0); i < numFields; i++ {
			if err != nil {
				break
			}

			args[i], err = parseArg(&read, packetData)
		}

		if err != nil {
			newLine := newLog(LogQlog, QlogReqId, lastTimestamp,
				fmt.Sprintf("ERROR: Packet read error (%s). "+
					"Dump of %d bytes:\n%x\n", err, packetLen,
					packetData))
			rtn = append(rtn, newLine)
			continue
		}

		// Grab the string and output
		if int(strMapId) > len(strMap) {
			newLine := newLog(LogQlog, QlogReqId, lastTimestamp,
				fmt.Sprintf("Not enough entries in "+
					"string map (%d %d)\n", strMapId,
					len(strMap)/LogStrSize))
			rtn = append(rtn, newLine)
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

		newLine := newLog(logSubsystem, reqId, timestamp,
			fmt.Sprintf(mapStr+"\n", args...))
		rtn = append(rtn, newLine)
	}

	sort.Sort(SortByTime(rtn))
	return rtn
}
