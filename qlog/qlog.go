// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging support

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/utils/dangerous"
)

type LogSubsystem uint8

// These should be short to save space, visually different to aid in reading
const FnEnterStr = "---In "
const FnExitStr = "Out-- "

func IsFunctionIn(test string) bool {
	return strings.Index(test, FnEnterStr) == 0
}

func IsFunctionOut(test string) bool {
	return strings.Index(test, FnExitStr) == 0
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

const TimeFormat = "2006-01-02T15:04:05.000000000"

func SpecialReq(reqId uint64) string {
	if reqId > MinFixedReqId {
		switch reqId {
		default:
			return "UNKNOWN"
		case MuxReqId:
			return "[Mux]"
		case FlushReqId:
			return "[Flush]"
		case QlogReqId:
			return "[Qlog]"
		case TestReqId:
			return "[Test]"
		case RefreshReqId:
			return "[Refresh]"
		}
	}

	var format string
	var offset uint64
	switch {
	default:
		format = "%d"
		offset = 0
	case FlusherRequestIdMin <= reqId && reqId < RefreshRequestIdMin:
		format = "[Flush%d]"
		offset = FlusherRequestIdMin
	case RefreshRequestIdMin <= reqId && reqId < ForgetRequstIdMin:
		format = "[Refresh%d]"
		offset = RefreshRequestIdMin
	case ForgetRequstIdMin <= reqId && reqId < UnusedRequestIdMin:
		format = "[Forget%d]"
		offset = ForgetRequstIdMin
	}

	return fmt.Sprintf(format, reqId-offset)
}

var logSubsystem = []string{}
var logSubsystemMap = map[string]LogSubsystem{}

type logSubsystemPair struct {
	name   string
	logger LogSubsystem
}

var logSubsystemList = []logSubsystemPair{
	{"Daemon", LogDaemon},
	{"Datastore", LogDatastore},
	{"WorkspaceDb", LogWorkspaceDb},
	{"Test", LogTest},
	{"Qlog", LogQlog},
	{"Quark", LogQuark},
	{"Spin", LogSpin},
	{"Tool", LogTool},
}

func init() {
	for _, v := range logSubsystemList {
		addLogSubsystem(v.name, v.logger)
	}
}

func addLogSubsystem(sys string, l LogSubsystem) {
	logSubsystem = append(logSubsystem, sys)
	logSubsystemMap[strings.ToLower(sys)] = l
}

func (enum LogSubsystem) String() string {
	if 0 <= enum && enum <= logSubsystemMax {
		return logSubsystem[enum]
	}
	return ""
}

func getSubsystem(sys string) (LogSubsystem, error) {
	if m, ok := logSubsystemMap[strings.ToLower(sys)]; ok {
		return m, nil
	}
	return LogDaemon, errors.New("Invalid subsystem string")
}

const logEnvTag = "TRACE"
const maxLogLevels = 4
const defaultMmapFile = "qlog"
const entryCompleteBit = uint16(1 << 15)

// Get whether, given the subsystem, the given level is active for logs
func (q *Qlog) getLogLevel(idx LogSubsystem, level uint8) bool {
	var mask uint32 = (1 << uint32((uint8(idx)*maxLogLevels)+level))
	return (q.LogLevels & mask) != 0
}

func (q *Qlog) setLogLevelBitmask(sys LogSubsystem, level uint8) {
	idx := uint8(sys)
	q.LogLevels &= ^(((1 << maxLogLevels) - 1) << (idx * maxLogLevels))
	q.LogLevels |= uint32(level) << uint32(idx*maxLogLevels)
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
	logBuffer *SharedMemory
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

// Only for tests - causes all logs starting with prefix to be incompletely written
func (q *Qlog) EnterTestMode(dropPrefix string) {
	q.logBuffer.testDropStr = dropPrefix
	q.logBuffer.testMode = true
}

func formatString(idx LogSubsystem, reqId uint64, t time.Time,
	format string) string {

	var front string
	if reqId < MinSpecialReqId {
		const frontFmt = "%s | %12s %7d: "
		front = fmt.Sprintf(frontFmt, t.Format(TimeFormat),
			idx, reqId)
	} else {
		const frontFmt = "%s | %12s % 7s: "
		front = fmt.Sprintf(frontFmt, t.Format(TimeFormat),
			idx, SpecialReq(reqId))
	}

	return front + format
}

func (q *Qlog) Sync() int {
	return q.logBuffer.Sync()
}

func (q *Qlog) Close() error {
	return q.logBuffer.Close()
}

func (q *Qlog) Log(idx LogSubsystem, reqId uint64, level uint8, format string,
	args ...interface{}) {

	t := time.Now()
	q.Log_(t, idx, reqId, level, format, args...)
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

type Qlogger struct {
	RequestId uint64
	qlog      *Qlog
	subsystem LogSubsystem
}

// This is just a placeholder for now:
// TODO sanity check space for logSubsystemMax before adding it in
func newLogSubsystem(sys string) LogSubsystem {
	//logSubsystemMax++
	l := logSubsystemMax
	logSubsystemList = append(logSubsystemList, logSubsystemPair{sys, l})
	addLogSubsystem(sys, l)
	return l
}

// TODO: Add support for registering subsystems dynamically,
//       and record the subsystem in qlog file, for qparse to be able to
//       parse the logs.
func NewQlogger(subsystem string, ramfsPath string) *Qlogger {
	return NewQloggerWithSize(subsystem, ramfsPath, uint64(DefaultMmapSize))
}

func NewQloggerWithSize(subsystem string, ramfsPath string, size uint64) *Qlogger {
	sub, err := getSubsystem(subsystem)
	if err != nil {
		sub = newLogSubsystem(subsystem)
	}

	log, err := NewQlogExt(ramfsPath, size, subsystem, PrintToStdout)
	utils.AssertNoErr(err)
	qlogger := &Qlogger{
		RequestId: 0,
		qlog:      log,
		subsystem: sub,
	}
	return qlogger
}

func (q *Qlogger) SetWriter(w func(format string, args ...interface{}) error) {
	q.qlog.SetWriter(w)
}

func (q *Qlogger) wrapQlog(level uint8, format string, args ...interface{}) {
	q.qlog.Log(q.subsystem, q.RequestId, level, format, args...)
}

// Log an Error message
func (q *Qlogger) Elog(format string, args ...interface{}) {
	q.wrapQlog(0, format, args...)
}

// Log a Warning message
func (q *Qlogger) Wlog(format string, args ...interface{}) {
	q.wrapQlog(1, format, args...)
}

// Log a Debug message
func (q *Qlogger) Dlog(format string, args ...interface{}) {
	q.wrapQlog(2, format, args...)
}

// Log a Verbose tracing message
func (q *Qlogger) Vlog(format string, args ...interface{}) {
	q.wrapQlog(3, format, args...)
}

var uniqueQloggerRequestId uint64

func (q *Qlogger) NewContext() *Qlogger {
	return &Qlogger{
		RequestId: atomic.AddUint64(&uniqueQloggerRequestId, 1),
		qlog:      q.qlog,
		subsystem: q.subsystem,
	}
}
