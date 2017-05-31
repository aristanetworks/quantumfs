// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// reader is a shared memory log parser for the qlog quantumfs subsystem
// It is used for tailing a qlog file and outputting it live

package qlog

import "fmt"
import "os"
import "reflect"
import "strings"
import "time"
import "unsafe"

type Reader struct {
	file *os.File

	headerSize  uint64
	circBufSize uint64

	lastPastEndIdx uint64

	strMap         []LogStr
	strMapLastRead uint64
}

func NewReader(qlogFile string) *Reader {
	rtn := Reader{
		headerSize: uint64(unsafe.Sizeof(MmapHeader{})),
	}

	file, err := os.Open(qlogFile)
	if err != nil {
		panic(fmt.Sprintf("Unable to read from qlog file %s: %s",
			qlogFile, err))
	}

	rtn.file = file
	header := rtn.ReadHeader()
	rtn.circBufSize = header.CircBuf.Size
	rtn.lastPastEndIdx = header.CircBuf.PastEndIdx
	return &rtn
}

func (read *Reader) readDataBlock(pos uint64, len uint64, outbuf []byte) {
	_, err := read.file.ReadAt(outbuf[:len], int64(pos+read.headerSize))
	if err != nil {
		panic(fmt.Sprintf("Unable to read data from qlog file: %s", err))
	}
}

func (read *Reader) RefreshStrMap() {
	fileOffset := read.headerSize + read.circBufSize
	if read.strMap == nil {
		read.strMap = make([]LogStr, 0)
	}

	buf := make([]byte, LogStrSize)
	for {
		_, err := read.file.ReadAt(buf,
			int64(fileOffset+read.strMapLastRead))
		if err != nil {
			fmt.Printf("Unable to read entries for strMap: %s\n", err)
			break
		}

		mapEntry := (*LogStr)(unsafe.Pointer(&buf[0]))
		if mapEntry.Text[0] == '\x00' {
			// No more strMap entries filled, stop looping
			break
		}

		read.strMap = append(read.strMap, *mapEntry)
		read.strMapLastRead += LogStrSize
	}
}

func (read *Reader) ReadHeader() *MmapHeader {
	headerData := make([]byte, read.headerSize)
	_, err := read.file.ReadAt(headerData, 0)
	if err != nil {
		panic(fmt.Sprintf("Unable to read header data from qlog file: %s",
			err))
	}

	return ExtractHeader(headerData)
}

func (read *Reader) ProcessLogs(fxn func(LogOutput)) {
	// Run indefinitely
	for {
		newLogs := read.readMore()
		for _, v := range newLogs {
			fxn(v)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (read *Reader) readMore() []LogOutput {
	freshHeader := read.ReadHeader()
	if freshHeader.CircBuf.PastEndIdx == read.lastPastEndIdx {
		return nil
	}

	rtn := make([]LogOutput, 0)

	pastEndIdx := freshHeader.CircBuf.PastEndIdx
	readLen := wrapMinus(pastEndIdx, read.lastPastEndIdx, read.circBufSize)

	// read all the data in one go to reduce number of reads
	data := read.wrapRead(read.lastPastEndIdx, readLen)
	pastDataIdx := int64(len(data))

	for {
		readLen, logOutput, ready := read.readLogAt(data,
			uint64(pastDataIdx))
		if readLen == 0 {
			fmt.Printf("ERROR: Read zero length - escaping\n")
			break
		}

		if !ready {
			// throw away the logs we've seen so far because of this hole
			rtn = make([]LogOutput, 0)
		}

		if int64(readLen) > pastDataIdx {
			errorLog := newLog(LogQlog, QlogReqId, 0,
				"ERROR: Packet over-read error", nil)
			rtn = append([]LogOutput{errorLog}, rtn...)
			break
		}

		pastDataIdx -= int64(readLen)

		if ready {
			rtn = append([]LogOutput{logOutput}, rtn...)
		} else {
			pastEndIdx = read.lastPastEndIdx
			wrapPlusEquals(&pastEndIdx, uint64(pastDataIdx),
				read.circBufSize)
		}

		if pastDataIdx <= 0 {
			break
		}
	}
	read.lastPastEndIdx = pastEndIdx

	return rtn
}

func (read *Reader) readLogAt(data []byte, pastEndIdx uint64) (uint64, LogOutput,
	bool) {

	if len(data) < 2 {
		fmt.Println("Partial packet - not enough data to even read size")
		return 0, LogOutput{}, false
	}

	var packetLen uint16
	readBack(&pastEndIdx, data, packetLen, &packetLen)

	packetReady := ((packetLen & uint16(entryCompleteBit)) != 0)
	packetLen &= ^(uint16(entryCompleteBit))

	if uint64(len(data)) < pastEndIdx || pastEndIdx < uint64(packetLen) {
		fmt.Printf("Not enough data to read packet %d %d\n", len(data),
			packetLen)
		return 0, LogOutput{}, false
	}

	if !packetReady {
		// packet not ready yet
		return 2 + uint64(packetLen), LogOutput{}, false
	}

	// now read the data
	packetData := data[pastEndIdx-uint64(packetLen) : pastEndIdx]
	return 2 + uint64(packetLen), read.dataToLog(packetData), true
}

func (read *Reader) dataToLog(packetData []byte) LogOutput {
	numRead := uint64(0)
	var numFields uint16
	var strMapId uint16
	var reqId uint64
	var timestamp int64

	var err error
	if err = readPacket(&numRead, packetData,
		reflect.ValueOf(&numFields)); err != nil {
	} else if err = readPacket(&numRead, packetData,
		reflect.ValueOf(&strMapId)); err != nil {
	} else if err = readPacket(&numRead, packetData,
		reflect.ValueOf(&reqId)); err != nil {
	} else if err = readPacket(&numRead, packetData,
		reflect.ValueOf(&timestamp)); err != nil {
	}

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
		return newLog(LogQlog, QlogReqId, 0,
			"ERROR: Packet read error (%s). i"+
				"Dump of %d bytes:\n%x\n",
			[]interface{}{err, len(packetData),
				packetData})
	}

	// Grab the string and output
	if int(strMapId) >= len(read.strMap) {
		read.RefreshStrMap()

		if int(strMapId) >= len(read.strMap) {
			return newLog(LogQlog, QlogReqId, 0,
				"Not enough entries in string map (%d %d)\n",
				[]interface{}{strMapId,
					len(read.strMap) / LogStrSize})
		}
	}
	mapEntry := read.strMap[strMapId]
	logSubsystem := (LogSubsystem)(mapEntry.LogSubsystem)

	// Finally, print with the front attached like normal
	mapStr := string(mapEntry.Text[:])
	firstNullTerm := strings.Index(mapStr, "\x00")
	if firstNullTerm != -1 {
		mapStr = mapStr[:firstNullTerm]
	}

	return newLog(logSubsystem, reqId, timestamp,
		mapStr+"\n", args)
}

func (read *Reader) wrapRead(idx uint64, num uint64) []byte {
	rtn := make([]byte, num)

	if idx+num > read.circBufSize {
		secondNum := (idx + num) - read.circBufSize
		num -= secondNum

		read.readDataBlock(0, secondNum, rtn[num:])
	}

	read.readDataBlock(idx, num, rtn[:num])

	return rtn
}
