// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// reader is a shared memory log parser for the qlog quantumfs subsystem
// It is used for tailing a qlog file and outputting it live

package qlog

import "encoding/binary"
import "bytes"
import "fmt"
import "os"
import "reflect"
import "unsafe"


type Reader struct {
	file            *os.File

	headerSize	uint64
	circBufSize	uint64

	lastPastEndIdx  uint64
}

func NewReader(qlogFile string) *Reader {
	rtn := Reader {
		headerSize:     uint64(unsafe.Sizeof(MmapHeader{})),
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
	num, err := read.file.ReadAt(outbuf[:len], int64(pos+read.headerSize))
	if uint64(num) != len {
		panic(fmt.Sprintf("Read length doesn't match: %d vs %d", len, num))
	}
	if err != nil {
		panic(fmt.Sprintf("Unable to read data from qlog file: %s", err))
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

func (read *Reader) ReadMore() []LogOutput {
	freshHeader := read.ReadHeader()
	if freshHeader.CircBuf.PastEndIdx == read.lastPastEndIdx {
		return nil
	}

	rtn := make([]LogOutput, 0)
	pastEndIdx := freshHeader.CircBuf.PastEndIdx
	rtnPastEndIdx := pastEndIdx
	remaining := wrapMinus(pastEndIdx, read.lastPastEndIdx, read.circBufSize)

	for {
		readLen, logOutput, ready := read.readLogAt(pastEndIdx)
		if readLen == 0 {
			fmt.Printf("ERROR: Read zero length - escaping\n")
			break
		}

		if !ready {
			// throw away the logs we've seen so far 'cause of this hole
			rtn = make([]LogOutput, 0)
		}

		if readLen > remaining {
			errorLog := newLog(LogQlog, QlogReqId, 0,
				"ERROR: Packet over-read error", nil)
			rtn = append([]LogOutput{errorLog}, rtn...)
			break
		}

		remaining -= readLen
		wrapMinusEquals(&pastEndIdx, readLen, read.circBufSize)

		if ready {
			rtn = append([]LogOutput{logOutput}, rtn...)
		} else {
			rtnPastEndIdx = pastEndIdx
		}

		if remaining == 0 {
			break
		}
	}
	read.lastPastEndIdx = rtnPastEndIdx

	return rtn
}

func (read *Reader) readLogAt(pastEndIdx uint64) (uint64, LogOutput, bool) {
	var packetLen uint16
	read.readBack(pastEndIdx, packetLen, &packetLen)

	packetReady := ((packetLen & uint16(entryCompleteBit)) != 0)
	packetLen &= ^(uint16(entryCompleteBit))

	if !packetReady {
		// packet not ready yet
		return 2+uint64(packetLen), LogOutput{}, false
	}

	return 2+uint64(packetLen), newLog(LogQlog, QlogReqId, 0, "PACKET %d", []interface{}{packetLen}), true
}

func (read *Reader) readBack(pastIdx uint64, outputType interface{},
	output interface{}) {

	dataLen := uint64(reflect.TypeOf(outputType).Size())

	wrapMinusEquals(&pastIdx, dataLen, read.circBufSize)
	rawData := read.wrapRead(pastIdx, dataLen)

	buf := bytes.NewReader(rawData)
	err := binary.Read(buf, binary.LittleEndian, output)
	if err != nil {
		panic("Unable to binary read from data")
	}
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
