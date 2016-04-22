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

type LogSubsystem uint8
const (
	LogDaemon LogSubsystem = iota
	LogProcessLocal
	LogDatastore
	LogWorkspacedb
	logSubsystemmax = LogWorkspacedb
)

func (enum LogSubsystem) String() string {
	switch enum {
	case LogDaemon:
		return "Daemon"
	case LogProcessLocal:
		return "ProcessLocal"
	case LogDatastore:
		return "Datastore"
	case LogWorkspacedb:
		return "WorkspaceDb"
	}
	return ""
}

func getSubsystem(sys string) (LogSubsystem, error) {
	switch (sys) {
	case "Daemon":
		return LogDaemon, nil
	case "ProcessLocal":
		return LogProcessLocal, nil
	case "Datastore":
		return LogDatastore, nil
	case "WorkspaceDb":
		return LogWorkspacedb, nil
	}
	return LogDaemon, errors.New("Invalid subsystem string")
}

// this is the logging system level store. Increase size as the number of
// LogSubsystems increases past your capacity
var logLevels uint16
var maxLogLevels uint8
var logEnvTag = "TRACE"

func generateWriter(fileOut string) func(string, ...interface{}) (int, error) {

	fd, e := os.OpenFile(fileOut, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		fmt.Printf("Unable to open file for logging: %s, %e", fileOut, e)
		return nil
	}

	return func(format string, args ...interface{}) (int, error) {
		num, e := fd.WriteString(fmt.Sprintf(format, args...))
		return num, e
	}
}

// Get whether, given the subsystem, the given level is active for logs
func GetLogLevel(idx LogSubsystem, level uint8) bool {
	var mask uint16 = (1 << ((uint8(idx) * maxLogLevels) + level))
	return (logLevels & mask) != 0
}

func setLogLevelBitmask(sys LogSubsystem, level uint8, enable bool) {
	idx := uint8(sys)
	logLevels &= ^(((1 << maxLogLevels)-1) << (idx * maxLogLevels))
	logLevels |= uint16(level << (idx * maxLogLevels))
}

// Load desired log levels from the environment variable
func loadLevels() {
	// reset all levels
	logLevels = 0;

	levels := os.Getenv(logEnvTag)	

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
			if level > int(maxLogLevels) {
				level = int(maxLogLevels)
			}
			level = (1 << uint8(level)) - 1
		}

		var idx LogSubsystem
		idx, e = getSubsystem(tokens[0])
		if e != nil {
			continue;
		}

		setLogLevelBitmask(idx, uint8(level), true)
	}
}

func init() {
	logLevels = 0
	maxLogLevels = 4

	// check that our logLevel container is large enough for our subsystems
	if (uint8(logSubsystemmax) * maxLogLevels) >
		uint8(unsafe.Sizeof(logLevels)) * 8 {

		panic("Log level structure not large enough for given subsystems")
	}


	loadLevels()
}

