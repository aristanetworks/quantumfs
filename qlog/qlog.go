// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging support

import "fmt"
import "errors"
import "unsafe"
import "strconv"
import "strings"
import "os"
import "time"
import "math"

type LogSubsystem uint8

func (v LogSubsystem) Primitive() interface{} {
	return uint8(v)
}

// These should be short to save space, visually different to aid in reading
const FnEnterStr = "---In "
const FnExitStr = "Out-- "

const (
	LogDaemon LogSubsystem = iota
	LogDatastore
	LogWorkspaceDb
	LogTest
	LogQlog
	logSubsystemMax = LogQlog
)

const (
	MuxReqId uint64 = math.MaxUint64 - iota
	FlushReqId
	QlogReqId
	TestReqId
	MinSpecialReqId
)

const TimeFormat = "2006-01-02T15:04:05.000000000"

func SpecialReq(reqId uint64) string {
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
	}
}

func (enum LogSubsystem) String() string {
	switch enum {
	case LogDaemon:
		return "Daemon"
	case LogDatastore:
		return "Datastore"
	case LogWorkspaceDb:
		return "WorkspaceDb"
	case LogTest:
		return "Test"
	case LogQlog:
		return "Qlog"
	}
	return ""
}

func getSubsystem(sys string) (LogSubsystem, error) {
	switch strings.ToLower(sys) {
	case "daemon":
		return LogDaemon, nil
	case "datastore":
		return LogDatastore, nil
	case "workspacedb":
		return LogWorkspaceDb, nil
	case "test":
		return LogTest, nil
	case "qlog":
		return LogQlog, nil
	}
	return LogDaemon, errors.New("Invalid subsystem string")
}

const logEnvTag = "TRACE"
const maxLogLevels = 4
const defaultMmapFile = "qlog"
const entryCompleteBit = 1 << 15

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
	for i := 0; i <= int(logSubsystemMax); i++ {
		q.setLogLevelBitmask(LogSubsystem(i), 1)
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
	Write     func(format string, args ...interface{}) error
	logBuffer *SharedMemory
}

func PrintToStdout(format string, args ...interface{}) error {
	format += "\n"
	_, err := fmt.Printf(format, args...)
	return err
}

func NewQlogTiny() *Qlog {
	return NewQlogExt("", uint32(DefaultMmapSize), PrintToStdout)
}

func NewQlog(ramfsPath string) *Qlog {
	return NewQlogExt(ramfsPath, uint32(DefaultMmapSize), PrintToStdout)
}

func NewQlogExt(ramfsPath string, sharedMemLen uint32, outLog func(format string,
	args ...interface{}) error) *Qlog {

	if sharedMemLen == 0 {
		panic(fmt.Sprintf("Invalid shared memory length provided: %d\n",
			sharedMemLen))
	}

	q := Qlog{
		LogLevels: 0,
		Write:     outLog,
	}
	q.logBuffer = newSharedMemory(ramfsPath, defaultMmapFile, int(sharedMemLen),
		&q)

	// check that our logLevel container is large enough for our subsystems
	if (uint8(logSubsystemMax) * maxLogLevels) >
		uint8(unsafe.Sizeof(q.LogLevels))*8 {

		panic("Log level structure not large enough for given subsystems")
	}

	q.SetLogLevels(os.Getenv(logEnvTag))

	return &q
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

func (q *Qlog) Log(idx LogSubsystem, reqId uint64, level uint8, format string,
	args ...interface{}) {

	t := time.Now()

	// Put into the shared circular buffer, UnixNano will work until year 2262
	unixNano := t.UnixNano()
	if q.logBuffer != nil {
		q.logBuffer.logEntry(idx, reqId, level, unixNano, format, args...)
	}

	if q.getLogLevel(idx, level) {
		q.Write(formatString(idx, reqId, t, format), args...)
	}
}
