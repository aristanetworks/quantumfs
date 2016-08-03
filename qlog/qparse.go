// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package qlog

import "bytes"
import "encoding/binary"
import "errors"
import "fmt"
import "io"
import "io/ioutil"
import "math"
import "os"
import "reflect"
import "strings"
import "time"
import "unsafe"

func ParseLogs(filepath string) string {
	data := grabMemory(filepath)
	header := (*MmapHeader)(unsafe.Pointer(&data[0]))

	if header.Version != MmapHeaderVersion {
		panic(fmt.Sprintf("Qlog version incompatible: got %d, need %d\n",
			header.Version, MmapHeaderVersion))
	}

	mmapHeaderSize := uint32(unsafe.Sizeof(MmapHeader{}))

	if uint32(len(data)) != (header.StrMapSize + header.CircBuf.Size +
		mmapHeaderSize) {
		fmt.Println("Data length inconsistent with expectations. %d",
			len(data))
	}

	return outputLogs(header.CircBuf.FrontIdx, header.CircBuf.PastEndIdx,
		data[mmapHeaderSize:mmapHeaderSize+header.CircBuf.Size],
		data[mmapHeaderSize+header.CircBuf.Size:])
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

	mapFile, err := os.OpenFile(filepath, os.O_RDONLY, 0777)
	if mapFile == nil || err != nil {
		panic("Unable to open shared memory log file")
	}

	afterHeader := make([]byte, unsafe.Sizeof(MmapHeader{}))
	count := 0
	for count < len(afterHeader) {
		dataLen, err := mapFile.Read(afterHeader[count:])
		if err != nil && err != io.EOF {
			panic("Unable to re-read file header")
		}
		count += dataLen
	}

	// Replace the file data "frontIdx" field with the safest, newer value
	header := (*MmapHeader)(unsafe.Pointer(&fileData[0]))
	newerHeader := (*MmapHeader)(unsafe.Pointer(&afterHeader[0]))
	header.CircBuf.FrontIdx = newerHeader.CircBuf.FrontIdx
	
	mapFile.Close()

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

	panic(fmt.Sprintf("Unsupported field type %d\n", byteType))
}

// outputType is an instance of that same type as output, output *must* be a pointer
// to a variable of that type for the data to be placed into
func readInto(idx *uint32, data []byte, outputType interface{}, output interface{}) {
	dataLen := uint32(reflect.TypeOf(outputType).Size())
	rawData := wrapRead(*idx, dataLen, data)
	wrapPlusEquals(idx, dataLen, len(data))

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
		for i := uint32(0); i < dataLen; i++ {
			var singleArray [1]byte
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

func outputLogs(frontIdx uint32, pastEndIdx uint32, data []byte,
	strMapData []byte) string {

	var buffer bytes.Buffer

	for frontIdx != pastEndIdx {
		var packetLen uint16
		readInto(&frontIdx, data, packetLen, &packetLen)

		// Read the packet data into a separate buffer
		packetData := wrapRead(frontIdx, uint32(packetLen-2), data)
		wrapPlusEquals(&frontIdx, uint32(packetLen-2), len(data))

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

		args := make([]interface{}, numFields)
		for i := uint16(0); i < numFields; i++ {
			if err != nil {
				break
			}

			args[i], err = parseArg(&read, packetData)
		}

		if err != nil {
			fmt.Sprintf("WARN: Packet read error (%s). "+
				"Dump of %d bytes:\n%x\n", err, packetLen,
				packetData)
			continue
		}

		// Grab the string and output
		strMapIdx := uint32(strMapId) * LogStrSize
		if strMapIdx+LogStrSize > uint32(len(strMapData)) {
			buffer.WriteString(fmt.Sprintf("Not enough entries in " +
				"string map (%d %d)\n", strMapId,
				len(strMapData)/LogStrSize))
			continue
		}

		mapEntry := *(*LogStr)(unsafe.Pointer(&strMapData[strMapIdx]))
		logSubsystem := (LogSubsystem)(mapEntry.LogSubsystem)

		// Convert timestamp back into something useful
		t := time.Unix(0, timestamp)

		var front string
		if reqId < MinSpecialReqId {
			const frontFmt = "%s | %12s %7d: "
			front = fmt.Sprintf(frontFmt, t.Format(TimeFormat),
				logSubsystem, reqId)
		} else {
			const frontFmt = "%s | %12s % 7s: "
			front = fmt.Sprintf(frontFmt, t.Format(TimeFormat),
				logSubsystem, SpecialReq(reqId))
		}

		// Finally, print with the front attached like normal
		mapStr := string(mapEntry.Text[:])
		firstNullTerm := strings.Index(mapStr, "\x00")
		if firstNullTerm != -1 {
			mapStr = mapStr[:firstNullTerm]
		}
		buffer.WriteString(fmt.Sprintf(front+mapStr+"\n", args...))
	}

	return buffer.String()
}
