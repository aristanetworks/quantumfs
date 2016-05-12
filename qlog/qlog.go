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

const (
	LogDaemon LogSubsystem = iota
	LogDatastore
	LogWorkspacedb
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
	case LogWorkspacedb:
		return "WorkspaceDb"
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
		return LogWorkspacedb, nil
	}
	return LogDaemon, errors.New("Invalid subsystem string")
}

const logEnvTag = "TRACE"
const maxLogLevels = 4

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
func (q *Qlog) loadLevels(levels string) {
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
	logLevels uint16
	write     func(format string, args ...interface{}) (int, error)
}

func NewQlog() *Qlog {
	q := Qlog{
		logLevels: 0,
		write:     fmt.Printf,
	}

	// check that our logLevel container is large enough for our subsystems
	if (uint8(logSubsystemMax) * maxLogLevels) >
		uint8(unsafe.Sizeof(q.logLevels))*8 {

		panic("Log level structure not large enough for given subsystems")
	}

	q.loadLevels(os.Getenv(logEnvTag))

	return &q
}

func (q *Qlog) SetWriter(w func(format string, args ...interface{}) (int, error)) {
	q.write = w
}

func (q *Qlog) Log(idx LogSubsystem, reqId uint64, level uint8, format string,
	args ...interface{}) {

	// todo: send to shared memory
	t := time.Now()

	if q.getLogLevel(idx, level) {
		q.write(t.Format(time.StampNano)+" "+idx.String()+" "+
			strconv.FormatUint(reqId, 10)+": "+format+"\n",
			args...)
	}
}
