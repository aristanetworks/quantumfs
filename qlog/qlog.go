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

const (
	LogDaemon LogSubsystem = iota
	LogDatastore
	LogWorkspaceDb
	LogTest
	logSubsystemMax = LogTest
)

const DummyReqId uint64 = math.MaxUint64

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
	}
	return LogDaemon, errors.New("Invalid subsystem string")
}

const logEnvTag = "TRACE"
const maxLogLevels = 4
const defaultMmapFile = "qlog"

// Get whether, given the subsystem, the given level is active for logs
func (q *Qlog) getLogLevel(idx LogSubsystem, level uint8) bool {
	var mask uint16 = (1 << ((uint8(idx) * maxLogLevels) + level))
	return (q.logLevels & mask) != 0
}

func (q *Qlog) setLogLevelBitmask(sys LogSubsystem, level uint8) {
	idx := uint8(sys)
	q.logLevels &= ^(((1 << maxLogLevels) - 1) << (idx * maxLogLevels))
	q.logLevels |= uint16(level) << uint16(idx*maxLogLevels)
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
	logLevels	uint16
	write		func(format string, args ...interface{}) (int, error)
	logBuffer	*SharedMemory
}

func printToStdout(format string, args ...interface{}) (int, error) {
	format += "\n"
	return fmt.Printf(format, args...)
}

func NewQlog(ramfsPath string) *Qlog {
	q := Qlog{
		logLevels: 0,
		write:     printToStdout,
	}
	q.logBuffer = newSharedMemory(ramfsPath, defaultMmapFile, &q.write)

	// check that our logLevel container is large enough for our subsystems
	if (uint8(logSubsystemMax) * maxLogLevels) >
		uint8(unsafe.Sizeof(q.logLevels))*8 {

		panic("Log level structure not large enough for given subsystems")
	}

	q.SetLogLevels(os.Getenv(logEnvTag))

	return &q
}

func (q *Qlog) SetWriter(w func(format string, args ...interface{}) (int, error)) {
	q.write = w
}

func (q *Qlog) Log(idx LogSubsystem, reqId uint64, level uint8, format string,
	args ...interface{}) {

	t := time.Now()

	// Put into the shared circular buffer, UnixNano will work until year 2262
	unixNano := t.UnixNano()
	if q.logBuffer != nil {
		q.logBuffer.logEntry(idx, reqId, level, unixNano, format, args...)
	}

	const timeFormat = "2006-01-02T15:04:05.000000000"

	if q.getLogLevel(idx, level) {
		var front string
		if reqId != DummyReqId {
			const frontFmt = "%s | %12s %5d: "
			front = fmt.Sprintf(frontFmt, t.Format(timeFormat),
				idx, reqId)
		} else {
			const frontFmt = "%s | %12s [MUX]: "
			front = fmt.Sprintf(frontFmt, t.Format(timeFormat),
				idx)
		}
		q.write(front+format, args...)
	}
}
