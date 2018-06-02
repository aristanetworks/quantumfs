// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// This file contains all quantumfs logging support

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const timeFormat = "2006-01-02T15:04:05.000000000"

func init() {
	for _, v := range logSubsystemList {
		addLogSubsystem(v.name, v.logger)
	}
}

var logSubsystem = []string{}
var logSubsystemMap = map[string]LogSubsystem{}

type logSubsystemPair struct {
	name   string
	logger LogSubsystem
}

func addLogSubsystem(sys string, l LogSubsystem) {
	logSubsystem = append(logSubsystem, sys)
	logSubsystemMap[strings.ToLower(sys)] = l
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

func formatString(idx LogSubsystem, reqId uint64, t time.Time,
	format string) string {

	var front string
	if reqId < MinSpecialReqId {
		const frontFmt = "%s | %12s %7d: "
		front = fmt.Sprintf(frontFmt, t.Format(timeFormat),
			idx, reqId)
	} else {
		const frontFmt = "%s | %12s % 7s: "
		front = fmt.Sprintf(frontFmt, t.Format(timeFormat),
			idx, specialReq(reqId))
	}

	return front + format
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
