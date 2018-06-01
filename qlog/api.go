// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all public functions and constants

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/aristanetworks/quantumfs/utils/dangerous"
)

type LogSubsystem uint8

const LogStrSize = 64

// Strmap size allows for up to ~10000 unique logs
const MmapStrMapSize = 512 * 1024

// We need a static array sized upper bound on our memory. Increase this as needed.
const DefaultMmapSize = (360000 * 24) + MmapStrMapSize

// These should be short to save space, visually different to aid in reading
const FnEnterStr = "---In "
const FnExitStr = "Out-- "

const (
	LogDaemon LogSubsystem = iota
	LogDatastore
	LogWorkspaceDb
	LogTest
	LogQlog
	LogQuark
	LogSpin
	LogTool
	logSubsystemMax = LogTool
)

const (
	MuxReqId uint64 = math.MaxUint64 - iota
	FlushReqId
	QlogReqId
	TestReqId
	RefreshReqId
	MinFixedReqId
)

const (
	MinSpecialReqId     = uint64(0xb) << 48
	FlusherRequestIdMin = uint64(0xb) << 48
	RefreshRequestIdMin = uint64(0xc) << 48
	ForgetRequstIdMin   = uint64(0xd) << 48
	UnusedRequestIdMin  = uint64(0xe) << 48
)

func (enum LogSubsystem) String() string {
	if 0 <= enum && enum <= logSubsystemMax {
		return logSubsystem[enum]
	}
	return ""
}

// Load desired log levels from the environment variable
func (q *Qlog) SetLogLevels(levels string) {
	// reset all levels
	defaultSetting := uint8(1)
	if levels == "*/*" {
		defaultSetting = ^uint8(0)
	}

	for i := 0; i <= int(logSubsystemMax); i++ {
		q.setLogLevelBitmask(LogSubsystem(i), defaultSetting)
	}

	bases := strings.Split(levels, ",")

	for i := range bases {
		cummulative := true
		tokens := strings.Split(bases[i], "/")
		if len(tokens) != 2 {
			tokens = strings.Split(bases[i], "|")
			cummulative = false
			if len(tokens) != 2 {
				continue
			}
		}

		// convert the string into an int
		var level int = 0
		var e error
		if tokens[1] == "*" {
			level = int(maxLogLevels)
			cummulative = true
		} else {
			var e error
			level, e = strconv.Atoi(tokens[1])
			if e != nil {
				continue
			}
		}

		// if it's cummulative, turn it into a cummulative mask
		if cummulative {
			if level >= int(maxLogLevels) {
				level = int(maxLogLevels - 1)
			}
			level = (1 << uint8(level+1)) - 1
		}

		var idx LogSubsystem
		idx, e = getSubsystem(tokens[0])
		if e != nil {
			continue
		}

		q.setLogLevelBitmask(idx, uint8(level))
	}
}

type Qlog struct {
	// This is the logging system level store. Increase size as the number of
	// LogSubsystems increases past your capacity
	LogLevels uint32

	// N.B. The format and args arguments are only valid until Write returns as
	// they are forced to be allocated on the stack.
	Write     func(format string, args ...interface{}) error
	logBuffer *sharedMemory

	// Maximum level to log to the qlog file
	maxLevel uint8
}

func PrintToStdout(format string, args ...interface{}) error {
	format += "\n"
	_, err := fmt.Printf(format, args...)
	return err
}

func NewQlog(ramfsPath string) (*Qlog, error) {
	return NewQlogExt(ramfsPath, uint64(DefaultMmapSize), "noVersion",
		PrintToStdout)
}

// N.B. The format and args arguments to the outLog are only valid
// until outLog returns as they are forced to be allocated on the stack.
// If a client wishes to read them after outLog returns, it must make a
// copy for itself.

func NewQlogExt(ramfsPath string, sharedMemLen uint64, daemonVersion string,
	outLog func(format string, args ...interface{}) error) (*Qlog, error) {

	if sharedMemLen == 0 {
		return nil, fmt.Errorf("Invalid shared memory length provided: %d\n",
			sharedMemLen)
	}

	q := Qlog{
		LogLevels: 0,
		Write:     outLog,
		maxLevel:  255,
	}
	var err error
	q.logBuffer, err = newSharedMemory(ramfsPath, defaultMmapFile,
		int(sharedMemLen), daemonVersion, &q)

	if err != nil {
		return nil, err
	}

	// check that our logLevel container is large enough for our subsystems
	if (uint8(logSubsystemMax) * maxLogLevels) >
		uint8(unsafe.Sizeof(q.LogLevels))*8 {

		return nil, fmt.Errorf("Log level structure not large enough " +
			"for given subsystems")
	}

	q.SetLogLevels(os.Getenv(logEnvTag))

	return &q, nil
}

func (q *Qlog) SetWriter(w func(format string, args ...interface{}) error) {
	q.Write = w
}

func (q *Qlog) SetMaxLevel(level uint8) {
	q.maxLevel = level
}

// Only for tests - causes all logs starting with prefix to be incompletely written
func (q *Qlog) EnterTestMode(dropPrefix string) {
	q.logBuffer.testDropStr = dropPrefix
	q.logBuffer.testMode = true
}

func (q *Qlog) Sync() int {
	return q.logBuffer.sync()
}

func (q *Qlog) Close() error {
	return q.logBuffer.close()
}

func (q *Qlog) Log(idx LogSubsystem, reqId uint64, level uint8, format string,
	args ...interface{}) {

	if level <= q.maxLevel {
		t := time.Now()
		q.Log_(t, idx, reqId, level, format, args...)
	}
}

// Should only be used by tests
func (q *Qlog) Log_(t time.Time, idx LogSubsystem, reqId uint64, level uint8,
	format string, args ...interface{}) {

	// Put into the shared circular buffer, UnixNano will work until year 2262
	unixNano := t.UnixNano()
	if q.logBuffer != nil {
		q.logBuffer.logEntry(idx, reqId, level, unixNano, format, args...)
	}

	if q.getLogLevel(idx, level) {
		argsCopy := make([]interface{}, 0, len(args))
		for _, arg := range args {
			argsCopy = append(argsCopy, dangerous.NoescapeInterface(arg))
		}
		q.Write(formatString(idx, reqId, t, format), argsCopy...)
	}
}

func NewReader(qlogFile string) *reader {
	rtn := reader{
		headerSize: uint64(unsafe.Sizeof(mmapHeader{})),
	}

	file, err := os.Open(qlogFile)
	if err != nil {
		panic(fmt.Sprintf("Unable to read from qlog file %s: %s",
			qlogFile, err))
	}

	rtn.file = file
	header := rtn.readHeader()
	rtn.circBufSize = header.CircBuf.Size

	rtn.daemonVersion = string(header.DaemonVersion[:])
	terminatorIdx := strings.Index(rtn.daemonVersion, "\x00")
	if terminatorIdx != -1 {
		rtn.daemonVersion = rtn.daemonVersion[:terminatorIdx]
	}

	rtn.lastPastEndIdx = header.CircBuf.endIndex()
	return &rtn
}

type LogProcessMode int

const (
	TailOnly LogProcessMode = iota
	ReadOnly
	ReadThenTail
)

func (read *reader) DaemonVersion() string {
	return read.daemonVersion
}

func (read *reader) ProcessLogs(mode LogProcessMode, fxn func(*LogOutput)) {
	if mode == ReadThenTail || mode == ReadOnly {
		freshHeader := read.readHeader()
		newLogs, newIdx := read.parseOld(freshHeader.CircBuf.endIndex())

		read.lastPastEndIdx = newIdx
		for _, v := range newLogs {
			fxn(v)
		}

		// we may be done
		if mode == ReadOnly {
			return
		}
	}

	// Run indefinitely
	for {
		freshHeader := read.readHeader()
		if freshHeader.CircBuf.endIndex() != read.lastPastEndIdx {
			newLogs, newPastEndIdx := read.parse(read.lastPastEndIdx,
				freshHeader.CircBuf.endIndex())

			read.lastPastEndIdx = newPastEndIdx
			for _, v := range newLogs {
				fxn(v)
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// Returns whether the strings are a function in/out log pair
func IsLogFnPair(formatIn string, formatOut string) bool {
	if strings.Index(formatIn, FnEnterStr) != 0 {
		return false
	}

	if strings.Index(formatOut, FnExitStr) != 0 {
		return false
	}

	formatIn = strings.Trim(formatIn, "\n ")
	formatOut = strings.Trim(formatOut, "\n ")

	minLength := len(formatIn) - len(FnEnterStr)
	outLength := len(formatOut) - len(FnExitStr)
	if outLength < minLength {
		minLength = outLength
	}

	tokenA := formatIn[len(FnEnterStr) : len(FnEnterStr)+minLength]
	tokenB := formatOut[len(FnExitStr) : len(FnExitStr)+minLength]

	if strings.Compare(tokenA, tokenB) != 0 {
		return false
	}

	return true
}

type LogOutput struct {
	Subsystem LogSubsystem
	ReqId     uint64
	T         int64
	Format    string
	Args      []interface{}
}

func ParseLogs(filepath string) string {
	rtn := ""

	ParseLogsExt(filepath, 0, defaultParseThreads, false,
		func(format string, args ...interface{}) (int, error) {
			rtn += fmt.Sprintf(format, args...)
			return len(format), nil
		})

	return rtn
}

func ParseLogsExt(filepath string, tabSpaces int, maxThreads int,
	statusBar bool, fn WriteFn) {

	pastEndIdx, dataArray, strMap := ExtractFields(filepath)

	logs := OutputLogsExt(pastEndIdx, dataArray, strMap, maxThreads, statusBar)
	FormatLogs(logs, tabSpaces, statusBar, fn)
}

// A faster parse without any flair: logs may be out of order, no tabbing. For use
// in the test suite predominantly. Returns LogOutput* which need to be Sprintf'd
func ParseLogsRaw(filepath string) []*LogOutput {

	pastEndIdx, dataArray, strMap := ExtractFields(filepath)

	return outputLogPtrs(pastEndIdx, dataArray, strMap, defaultParseThreads,
		false)
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

func (rawlog *LogOutput) ToString() string {
	t := time.Unix(0, rawlog.T)
	return fmt.Sprintf(formatString(rawlog.Subsystem, rawlog.ReqId, t,
		rawlog.Format), rawlog.Args...)
}

type sortString []string

func (s sortString) Len() int {
	return len(s)
}

func (s sortString) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortString) Less(i, j int) bool {
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
	return s[i].T < s[j].T
}

type SortByTimePtr []*LogOutput

func (s SortByTimePtr) Len() int {
	return len(s)
}

func (s SortByTimePtr) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortByTimePtr) Less(i, j int) bool {
	return s[i].T < s[j].T
}

func FormatLogs(logs []LogOutput, tabSpaces int, statusBar bool, fn WriteFn) {
	indentMap := make(map[uint64]int)
	// Now that we have the logs in correct order, we can indent them

	var status LogStatus
	if statusBar {
		fmt.Println("Formatting log output...")
		status = NewLogStatus(50)
	}
	for i := 0; i < len(logs); i++ {
		if statusBar {
			status.Process(float32(i) / float32(len(logs)))
		}

		// Add any indents necessary
		if tabSpaces > 0 {
			indents := indentMap[logs[i].ReqId]

			if strings.Index(logs[i].Format, FnExitStr) == 0 {
				indents--
			}

			// Add the spaces we need
			spaceStr := ""
			for j := 0; j < indents*tabSpaces; j++ {
				spaceStr += " "
			}

			if strings.Index(logs[i].Format, FnEnterStr) == 0 {
				indents++
			}

			logs[i].Format = spaceStr + logs[i].Format
			indentMap[logs[i].ReqId] = indents
		}

		// Convert timestamp back into something useful
		t := time.Unix(0, logs[i].T)

		outStr := formatString(logs[i].Subsystem, logs[i].ReqId, t,
			logs[i].Format)
		fn(outStr, logs[i].Args...)
	}
	if statusBar {
		status.Process(1)
	}
}

func ExtractHeader(data []byte) *mmapHeader {
	header := (*mmapHeader)(unsafe.Pointer(&data[0]))

	if header.Version != qlogVersion {
		panic(fmt.Sprintf("Qlog version incompatible: got %d, need %d\n",
			header.Version, qlogVersion))
	}

	return header
}

func ExtractFields(filepath string) (pastEndIdx uint64, dataArray []byte,
	strMapRtn []logStr) {

	data := grabMemory(filepath)
	header := ExtractHeader(data)

	mmapHeaderSize := uint64(unsafe.Sizeof(mmapHeader{}))

	if uint64(len(data)) != uint64(header.StrMapSize)+header.CircBuf.Size+
		mmapHeaderSize {
		fmt.Println("Data length inconsistent with expectations. ",
			len(data))
	}

	// create a safer map to use
	strMapData := data[mmapHeaderSize+header.CircBuf.Size:]
	strMap := make([]logStr, len(strMapData)/LogStrSize)
	idx := 0
	for i := 0; i+LogStrSize <= len(strMapData); i += LogStrSize {
		mapEntry := (*logStr)(unsafe.Pointer(&strMapData[i]))
		strMap[idx] = *mapEntry

		idx++
	}

	return header.CircBuf.endIndex(),
		data[mmapHeaderSize : mmapHeaderSize+header.CircBuf.Size], strMap
}

func NewLogStatus(displayWidth int) LogStatus {
	return LogStatus{
		shownHeader:  false,
		pixWidth:     displayWidth,
		lastPixShown: 0,
	}
}

func (l *LogStatus) Process(newPct float32) {
	if !l.shownHeader {
		leftHeader := "Processing: ||"
		nextHeader := "             |"
		fmt.Printf(leftHeader)
		for i := 0; i < l.pixWidth; i++ {
			fmt.Printf(" ")
		}
		fmt.Printf("||\n")
		fmt.Printf(nextHeader)
		l.shownHeader = true
	}

	// Calculate the amount of pixels to output in the loading bar
	pixDone := int(float32(l.pixWidth) * newPct)
	for i := l.lastPixShown + 1; i <= pixDone; i++ {
		// Each pixel in the bar is a period
		fmt.Printf(".")
	}

	if pixDone == l.pixWidth && pixDone != l.lastPixShown {
		fmt.Printf("| Done.\n")
	}

	l.lastPixShown = pixDone
}

func OutputLogs(pastEndIdx uint64, data []byte, strMap []logStr,
	maxWorkers int) []LogOutput {

	return OutputLogsExt(pastEndIdx, data, strMap, maxWorkers, false)
}

func OutputLogsExt(pastEndIdx uint64, data []byte, strMap []logStr, maxWorkers int,
	printStatus bool) []LogOutput {

	logPtrs := outputLogPtrs(pastEndIdx, data, strMap, maxWorkers, printStatus)

	// Go through the logs and fix any missing timestamps. Use the last entry's,
	// and de-pointer-ify them.
	rtn := make([]LogOutput, len(logPtrs))
	var lastTimestamp int64

	if printStatus {
		fmt.Println("Fixing missing timestamps...")
	}
	status := NewLogStatus(50)
	for i := 0; i < len(logPtrs); i++ {
		if printStatus {
			status.Process(float32(i) / float32(len(logPtrs)))
		}

		if logPtrs[i].T == 0 {
			logPtrs[i].T = lastTimestamp
		} else {
			lastTimestamp = logPtrs[i].T
		}

		rtn[i] = *logPtrs[i]
	}

	if printStatus {
		status.Process(1)
		fmt.Printf("Sorting parsed logs...")
	}

	sort.Sort(SortByTime(rtn))

	if printStatus {
		fmt.Printf("done\n")
	}

	return rtn
}
