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

const timeFormat = "2006-01-02T15:04:05.000000000"

func specialReq(reqId uint64) string {
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
