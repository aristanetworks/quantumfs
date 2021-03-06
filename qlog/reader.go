// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// reader is a shared memory log parser for the qlog quantumfs subsystem
// It is used for tailing a qlog file and outputting it live

package qlog

import (
	"bytes"
	"fmt"
	"os"
	"unsafe"
)

const (
	PacketHeaderLen = 20
)

type logStrTrim struct {
	Text         string
	LogSubsystem uint8
	LogLevel     uint8
}

type reader struct {
	file *os.File

	headerSize    uint64
	circBufSize   uint64
	daemonVersion string

	lastPastEndIdx uint64

	strMap         []logStrTrim
	strMapLastRead uint64
}

func (read *reader) readDataBlock(pos uint64, len uint64, outbuf []byte) {
	_, err := read.file.ReadAt(outbuf[:len], int64(pos+read.headerSize))
	if err != nil {
		panic(fmt.Sprintf("Unable to read data from qlog file: %s", err))
	}
}

func (read *reader) refreshStrMap() {
	fileOffset := read.headerSize + read.circBufSize
	if read.strMap == nil {
		read.strMap = make([]logStrTrim, 0)
	}

	buf := make([]byte, LogStrSize)
	for {
		_, err := read.file.ReadAt(buf,
			int64(fileOffset+read.strMapLastRead))
		if err != nil {
			fmt.Printf("Unable to read entries for strMap: %s\n", err)
			break
		}

		mapEntry := (*logStr)(unsafe.Pointer(&buf[0]))
		if mapEntry.Text[0] == '\x00' {
			// No more strMap entries filled, stop looping
			break
		}

		// trim and convert to a string to take the CPU hit early and once
		mapBytes := mapEntry.Text[:]
		firstNullTerm := bytes.IndexByte(mapBytes, '\x00')
		if firstNullTerm != -1 {
			mapBytes = mapBytes[:firstNullTerm]
		}

		var mapStr logStrTrim
		mapStr.LogSubsystem = mapEntry.LogSubsystem
		mapStr.LogLevel = mapEntry.LogLevel
		mapStr.Text = string(mapBytes) + "\n"

		read.strMap = append(read.strMap, mapStr)
		read.strMapLastRead += LogStrSize
	}
}

func (read *reader) readHeader() *mmapHeader {
	headerData := make([]byte, read.headerSize)
	_, err := read.file.ReadAt(headerData, 0)
	if err != nil {
		panic(fmt.Sprintf("Unable to read header data from qlog file: %s",
			err))
	}

	header, err := ExtractHeader(headerData)
	if err != nil {
		panic(err)
	}

	return header
}

func (read *reader) parseOld(pastEndIdx uint64) (logs []*LogOutput,
	haveReadTo uint64) {

	logs = make([]*LogOutput, 0, 1000000)

	distanceToEnd := read.circBufSize - 1
	lastReadyIdx := pastEndIdx
	readTo := pastEndIdx

	// to accommodate pastEndIdx moving while we read, we have to read packet
	// by packet and check the header every time we parse a log
	for {
		// Read the packet size from file
		lengthData := read.wrapRead(readTo-2, 2)
		packetLen := *(*uint16)(unsafe.Pointer(&lengthData[0]))
		packetLen &= ^(uint16(EntryCompleteBit))

		// If we reach a packet of insufficient length, then we're done
		if packetLen == 0 {
			break
		}
		// Add in the 2 bytes for the size field
		packetLen += 2

		// Now read the full packet so we can use readLogAt
		readFrom := wrapMinus(readTo, uint64(packetLen), read.circBufSize)
		packetData := read.wrapRead(readFrom, uint64(packetLen))
		readLen, logOutput, ready := read.readLogAt(packetData,
			uint64(packetLen))
		if readLen <= 2 {
			// hit an unset packet, so we're done
			break
		}

		// Place our marker at the beginning of the next packet
		wrapMinusEquals(&readTo, uint64(packetLen), read.circBufSize)

		// Read the header to see if the log end just passed us as we parsed
		freshHeader := read.readHeader()
		newDistanceToEnd := wrapMinus(readTo, freshHeader.CircBuf.endIndex(),
			read.circBufSize)

		// Distance to the moving end of logs should be decreasing. The
		// moment that it increases, it has passed us.
		if newDistanceToEnd > distanceToEnd {
			break
		}
		distanceToEnd = newDistanceToEnd

		if ready {
			logs = append(logs, logOutput)
		} else {
			logs = make([]*LogOutput, 0, 1000000)
			lastReadyIdx = readTo
			wrapMinusEquals(&lastReadyIdx, uint64(packetLen),
				read.circBufSize)
		}
	}

	// Reverse logs into chronological order
	logs = reverseLogSlice(logs)

	return logs, lastReadyIdx
}

func reverseLogSlice(in []*LogOutput) []*LogOutput {
	numEntries := len(in)
	backwardsLogs := in
	logs := make([]*LogOutput, numEntries)
	for i, val := range backwardsLogs {
		logs[numEntries-1-i] = val
	}

	return logs
}

func (read *reader) parse(readFrom uint64, readTo uint64) (logs []*LogOutput,
	haveReadTo uint64) {

	logs = make([]*LogOutput, 0, 1000)

	pastEndIdx := readTo
	readLen := wrapMinus(readTo, readFrom, read.circBufSize)

	// read all the data in one go to reduce number of reads
	data := read.wrapRead(readFrom, readLen)
	pastDataIdx := int64(len(data))

	for {
		readLen, logOutput, ready := read.readLogAt(data,
			uint64(pastDataIdx))
		if readLen == 0 {
			break
		}

		if !ready {
			// throw away the logs we've seen so far because of this hole
			logs = make([]*LogOutput, 0, 1000)
		}

		if int64(readLen) > pastDataIdx {
			errorLog := newLog(LogQlog, QlogReqId, 0,
				"ERROR: Packet over-read error", nil)
			logs = append(logs, &errorLog)
			break
		}

		pastDataIdx -= int64(readLen)

		if ready {
			logs = append(logs, logOutput)
		} else {
			pastEndIdx = readFrom
			wrapPlusEquals(&pastEndIdx, uint64(pastDataIdx),
				read.circBufSize)
		}

		if pastDataIdx <= 0 {
			break
		}
	}

	logs = reverseLogSlice(logs)

	return logs, pastEndIdx
}

func (read *reader) readLogAt(data []byte, pastEndIdx uint64) (uint64, *LogOutput,
	bool) {

	if pastEndIdx < 2 {
		fmt.Println("Partial packet - not enough data to even read size")
		return 0, nil, false
	}

	var packetLen uint16
	ReadBack(&pastEndIdx, data, packetLen, &packetLen)

	packetReady := ((packetLen & uint16(EntryCompleteBit)) != 0)
	packetLen &= ^(uint16(EntryCompleteBit))

	if uint64(len(data)) < pastEndIdx || pastEndIdx < uint64(packetLen) ||
		packetLen < PacketHeaderLen {

		// Not enough data to read packet
		return 0, nil, false
	}

	if !packetReady {
		// packet not ready yet
		return 2 + uint64(packetLen), nil, false
	}

	// now read the data
	packetData := data[pastEndIdx-uint64(packetLen) : pastEndIdx]
	output := read.dataToLog(packetData)
	return 2 + uint64(packetLen), output, true
}

func (read *reader) dataToLog(packetData []byte) *LogOutput {
	var numFields uint16
	var strMapId uint16
	var reqId uint64
	var timestamp int64
	var rtn LogOutput

	numFields = *(*uint16)(unsafe.Pointer(&packetData[0]))
	strMapId = *(*uint16)(unsafe.Pointer(&packetData[2]))
	reqId = *(*uint64)(unsafe.Pointer(&packetData[4]))
	timestamp = *(*int64)(unsafe.Pointer(&packetData[12]))

	numRead := uint64(PacketHeaderLen)

	var err error
	args := make([]interface{}, numFields)
	for i := uint16(0); i < numFields; i++ {
		if err != nil {
			break
		}

		args[i], err = parseArg(&numRead, packetData)
	}

	if err != nil {
		// If the timestamp is zero, we will fill it in later with
		// the previous log's timestamp
		rtn = newLog(LogQlog, QlogReqId, 0,
			"ERROR: Packet read error (%s). i"+
				"Dump of %d bytes:\n%x\n",
			[]interface{}{err, len(packetData),
				packetData})
		return &rtn
	}

	// Grab the string and output
	if int(strMapId) >= len(read.strMap) {
		read.refreshStrMap()

		if int(strMapId) >= len(read.strMap) {
			rtn = newLog(LogQlog, QlogReqId, 0,
				"Not enough entries in string map (%d %d)\n",
				[]interface{}{strMapId,
					len(read.strMap) / LogStrSize})
			return &rtn
		}
	}
	mapEntry := read.strMap[strMapId]
	logSubsystem := (LogSubsystem)(mapEntry.LogSubsystem)

	rtn = newLog(logSubsystem, reqId, timestamp, mapEntry.Text, args)
	return &rtn
}

func (read *reader) wrapRead(idx uint64, num uint64) []byte {
	rtn := make([]byte, num)

	if idx+num > read.circBufSize {
		secondNum := (idx + num) - read.circBufSize
		num -= secondNum

		read.readDataBlock(0, secondNum, rtn[num:])
	}

	read.readDataBlock(idx, num, rtn[:num])

	return rtn
}
