// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package qlog provides a logging infrastructure with fast and compact writes to
// file and log levels.
package qlog

// This file contains all public functions and constants

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/aristanetworks/quantumfs/utils/dangerous"
)

// Constants

// WriteFn is a function that qlog will echo logs to if a given log's level
// is currently enabled
type WriteFn func(string, ...interface{}) (int, error)

// EntryCompleteBit is the bit flag used in each log packet header to indicate the
// log has been completely written and is ready to be read
const EntryCompleteBit = uint16(1 << 15)

// LogStrSize is the maximum length allowed for a format string in a log
const LogStrSize = 64

// MmapStrMapSize is the fixed size of the string map in the qlog. 500K allows
// for roughly 10000 unique format log strings
const MmapStrMapSize = 512 * 1024

// DefaultMmapSize is the default total shared memory size, which includes both
// the string map and the space for actual log packets.
const DefaultMmapSize = (360000 * 24) + MmapStrMapSize

// FnEnterStr is the recommended prefix to use when logging the entry to functions
const FnEnterStr = "---In "

// FnExitStr is the recommended prefix to use when logging the exit from functions
const FnExitStr = "Out-- "

// Global Functions

// ExtractHeader takes the data from a qlog file and extracts the header from it
func ExtractHeader(data []byte) (*mmapHeader, error) {
	header := (*mmapHeader)(unsafe.Pointer(&data[0]))

	if header.Version != qlogVersion {
		return nil, fmt.Errorf("Qlog version incompatible: "+
			"got %d, need %d\n", header.Version, qlogVersion)
	}

	return header, nil
}

// ExtractFields takes a qlog file path and extracts the components: end of log
// offset, circular log buffer, string map
func ExtractFields(filepath string) (pastEndIdx uint64, dataArray []byte,
	strMapRtn []logStr, err error) {

	data := grabMemory(filepath)

	header, err := ExtractHeader(data)
	if err != nil {
		return 0, []byte{}, []logStr{}, err
	}

	mmapHeaderSize := uint64(unsafe.Sizeof(mmapHeader{}))

	if uint64(len(data)) < mmapHeaderSize+header.CircBuf.Size {
		return 0, []byte{}, []logStr{}, fmt.Errorf("qlog truncated")
	}

	if uint64(len(data)) != uint64(header.StrMapSize)+header.CircBuf.Size+
		mmapHeaderSize {
		fmt.Println("Data length inconsistent with expectations. ",
			len(data))
	}

	// create a safer map to use
	strMapData := data[mmapHeaderSize+header.CircBuf.Size:]
	strMap := make([]logStr, len(strMapData)/LogStrSize)
	visitStrMap(strMapData, func(idx int, entry *logStr) {
		strMap[idx] = *entry
	})

	return header.CircBuf.endIndex(),
		data[mmapHeaderSize : mmapHeaderSize+header.CircBuf.Size], strMap,
		nil
}

func visitStrMap(data []byte, visit func(int, *logStr)) {
	idx := 0
	for i := 0; i+LogStrSize <= len(data); i += LogStrSize {
		mapEntry := (*logStr)(unsafe.Pointer(&data[i]))
		visit(idx, mapEntry)

		idx++
	}
}

// OutputLogs is a simple interface to, given the components of a qlog that have
// been extracted, return the logs in a slice
func OutputLogs(pastEndIdx uint64, data []byte, strMap []logStr,
	maxWorkers int) []LogOutput {

	return OutputLogsExt(pastEndIdx, data, strMap, maxWorkers, false)
}

// OutputLogsExt allows you to parse a qlog, given its components, and return a slice
// of the logs. Parallel threads and status bar output to stdout are configurable.
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

// ReadBack reads another packet log, and modifies pastIdx to point to the next
// packet's offset in the data buffer. outputType should be the same type as output,
// just a generic instance will do, and output *must* point to valid memory.
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

// FormatLogs takes logs and applies the WriteFn given to them, allowing for
// formatting, status bar to stdout, and sorting the logs by time
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

// ParseLogs reads logs from a qlog file and returns them as one large string
func ParseLogs(filepath string) string {
	rtn := ""

	err := ParseLogsExt(filepath, 0, defaultParseThreads, false,
		func(format string, args ...interface{}) (int, error) {
			rtn += fmt.Sprintf(format, args...)
			return len(format), nil
		})
	if err != nil {
		return err.Error()
	}

	return rtn
}

// ParseLogsExt reads logs from a qlog file, formats them, optionally outputs a
// statusBar to stdout, and applies a WriteFn to each
func ParseLogsExt(filepath string, tabSpaces int, maxThreads int,
	statusBar bool, fn WriteFn) error {

	pastEndIdx, dataArray, strMap, err := ExtractFields(filepath)
	if err != nil {
		return err
	}

	logs := OutputLogsExt(pastEndIdx, dataArray, strMap, maxThreads, statusBar)
	FormatLogs(logs, tabSpaces, statusBar, fn)
	return nil
}

// ParseLogsRaw is a faster parse without any flair: logs may be out of order,
// no tabbing. Returns LogOutput* which need to be Sprintf'd
func ParseLogsRaw(filepath string) ([]*LogOutput, error) {

	pastEndIdx, dataArray, strMap, err := ExtractFields(filepath)
	if err != nil {
		return nil, err
	}

	return outputLogPtrs(pastEndIdx, dataArray, strMap, defaultParseThreads,
		false), nil
}

// LogscanSkim returns true if the log file string map given contains the substring
// "ERROR"
func LogscanSkim(filepath string, exceptions map[string]struct{}) bool {
	if exceptions != nil {
		return testStrMap(filepath, exceptions)
	}

	strMapData := extractStrMapData(filepath)

	// This takes too much time, so only count one string as failing
	if bytes.Contains(strMapData, []byte("ERROR")) {
		return true
	}

	return false
}

// PrintToStdout is a premade, commonly used WriteFn for Qlog to output to stdout
func PrintToStdout(format string, args ...interface{}) error {
	format += "\n"
	_, err := fmt.Printf(format, args...)
	return err
}

// IsLogFnPair returns whether the strings are a function in/out log pair
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

// Types

// LogSubsystem is an identifier to allow logs to be categorized by code location
type LogSubsystem uint8

// String is used to convert a LogSubsystem enum into a human readable string
func (enum LogSubsystem) String() string {
	if 0 <= enum && enum <= logSubsystemMax {
		return logSubsystem[enum]
	}
	return ""
}

// NewQlog is a shortened, simplified Qlog constructor. Ramfs should be a memory
// based filesystem due to the number of writes that will be made to it.
func NewQlog(ramfsPath string) (*Qlog, error) {
	return NewQlogExt(ramfsPath, uint64(DefaultMmapSize), "noVersion",
		PrintToStdout)
}

// NewQlogExt is the full Qlog constructor. Ramfs should be a memory based fs.
// sharedMemLen must be large enough to fit MmapStrMapSize and the small qlog
// header.
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

	if ramfsPath != "" {
		// Create a file and its path to be mmap'd
		err := os.MkdirAll(ramfsPath, 0777)
		if err != nil {
			return nil, fmt.Errorf("Unable create log dir: %s",
				ramfsPath)
		}

		q.filepath = ramfsPath + string(os.PathSeparator) + defaultMmapFile

		q.logBuffer, err = newSharedMemory(q.filepath, int(sharedMemLen),
			daemonVersion, &q)

		if err != nil {
			return nil, err
		}
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

// Qlog is a shared memory, binary logging subsytem. It implements a circular buffer
// and allows concurrent read/write between processes.
type Qlog struct {
	// LogLevels stores, for each LogSubsystem, a bit for each possible log
	// level. If the number of log levels is 4, for example, then every four bits
	// represents the enabled log levels for a different LogSubsystem.
	LogLevels uint32

	// N.B. The format and args arguments are only valid until Write returns as
	// they are forced to be allocated on the stack.
	Write     func(format string, args ...interface{}) error
	logBuffer *sharedMemory

	// Maximum level to log to the qlog file
	maxLevel        uint8
	filepath        string
	ErrorExec       string
	ErrorInProgress uint32
}

// SetLogLevels stores the provided log level string in the qlog object.
// The string allows settings of two kinds, cumulative "/" and bitmask "|".
// Each token in the string is separated by a comma, and contains a subsystem
// on the left, then exactly one setting specifier "/" or "|", and then a value.
//
// The value can either be "*" for all logs to be enabled, or a number.
// If the setting specifier is cumulative, the value means enable all logs up to and
// including the level value given. If the setting specifier is bitmask, then each
// bit will correspond to a bit in the LogLevels variable at the subsystem offset.
//
// */* - Enables all logs in all subsystems
// Daemon/2 - Enables all logs at or below level 2 for a subsystem called Daemon
// LogTest|5 - Enables logs of level 0 and level 2 for a subsystem called LogTest
// Note that no matter how log levels are configured, a log level must be <= maxLevel
// in order to be used and not discarded.
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

// SetWriter sets the "stdout" writer that qlog echos logs to
func (q *Qlog) SetWriter(w func(format string, args ...interface{}) error) {
	q.Write = w
}

// SetMaxLevel sets the level at which logs start being thrown away and neither
// echoed to WriteFn nor written to the qlog file.
func (q *Qlog) SetMaxLevel(level uint8) {
	q.maxLevel = level
}

// EnterTestMode is only for tests. It causes all logs starting with prefix to be
// incompletely written
func (q *Qlog) EnterTestMode(dropPrefix string) {
	q.logBuffer.testDropStr = dropPrefix
	q.logBuffer.testMode = true
}

// Sync causes any logs currently buffered and not written to the ramfs to be flushed
func (q *Qlog) Sync() int {
	return q.logBuffer.sync()
}

// Close closes the file descriptor to the qlog file
func (q *Qlog) Close() error {
	return q.logBuffer.close()
}

// Log sends a new log to the Qlog object
// N.B. The format and args arguments to the outLog are only valid
// until outLog returns as they are forced to be allocated on the stack.
// If a client wishes to read them after outLog returns, it must make a
// copy for itself.
func (q *Qlog) Log(idx LogSubsystem, reqId uint64, level uint8, format string,
	args ...interface{}) {

	if level <= q.maxLevel {
		t := time.Now()
		q.Log_(t, idx, reqId, level, format, args...)
	}
}

// Log_ should only be used by tests which need the same functionality as Log, but
// also need to manufacture the timestamp
func (q *Qlog) Log_(t time.Time, idx LogSubsystem, reqId uint64, level uint8,
	format string, args ...interface{}) {

	// Put into the shared circular buffer, UnixNano will work until year 2262
	unixNano := t.UnixNano()
	if q.logBuffer != nil {
		q.logBuffer.logEntry(idx, reqId, level, unixNano, format, args...)

		// If this is an error log, we want to take a snapshot of the qlog
		if level == 0 && q.ErrorExec != "" {
			perform := atomic.CompareAndSwapUint32(&q.ErrorInProgress,
				0, 1)

			// We define this here to save an indent
			handleError := func() {
				defer func() {
					atomic.StoreUint32(&q.ErrorInProgress, 0)
				}()

				q.Sync()
				args := q.ErrorExec + " " +
					strconv.Itoa(os.Getpid()) + " " + q.filepath

				// run the command via sh so we don't have to parse
				// and split the args up to satisfy the Command api
				cmd := exec.Command("sh", "-c", args)
				cmd.Run()
			}

			if perform {
				go handleError()
			}
		}
	}

	if q.getLogLevel(idx, level) {
		argsCopy := make([]interface{}, 0, len(args))
		for _, arg := range args {
			argsCopy = append(argsCopy, dangerous.NoescapeInterface(arg))
		}
		q.Write(formatString(idx, reqId, t, format), argsCopy...)
	}
}

// LogProcessMode specifies how a qlog reader should read a qlog file
type LogProcessMode int

const (
	TailOnly LogProcessMode = iota
	ReadOnly
	ReadThenTail
)

// NewReader creates a new qlog reader, attached to the qlog file given
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

// DaemonVersion returns the version of qlog used to create the given qlog file
func (read *reader) DaemonVersion() string {
	return read.daemonVersion
}

// ProcessLogs reads logs according to the reader's LogProcessMode, and uses fxn
// on each log in the file. If LogProcessMode involves tail, this function will
// continue indefinitely
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

// LogOutput is a parsed log from a qlog file
type LogOutput struct {
	Subsystem LogSubsystem
	ReqId     uint64
	T         int64
	Format    string
	Args      []interface{}
}

// String turns a LogOutput into a human readable string
func (rawlog *LogOutput) String() string {
	t := time.Unix(0, rawlog.T)
	return fmt.Sprintf(formatString(rawlog.Subsystem, rawlog.ReqId, t,
		rawlog.Format), rawlog.Args...)
}

// SortByTime allows LogOutputs to be sorted by their time field
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

// SortByTimePtr allows an array of LogOutput pointers to be sorted by their time
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

// NewLogStatus creates a new progress bar
func NewLogStatus(displayWidth int) LogStatus {
	return LogStatus{
		shownHeader:  false,
		pixWidth:     displayWidth,
		lastPixShown: 0,
	}
}

// LogStatus is used for showing a progress bar during qlog operations
type LogStatus struct {
	shownHeader  bool
	pixWidth     int
	lastPixShown int
}

// Process takes a new float, indicating the overall progress, and outputs more of
// the progress bar if needed.
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
