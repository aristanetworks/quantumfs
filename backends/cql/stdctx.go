// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package cql

import (
	"fmt"
	"log"
	"os"
	"time"
)

// StdCtx is the default ctx provided by cql package.
// Cql clients can either have custom implementation of ctx or
// use StdCtx. It logs to standard out based on the logging level.
type StdCtx struct {
	RequestID uint64
}

// DefaultCtx is a StdCtx usable by Cql clients which need one StdCtx
var DefaultCtx = &StdCtx{RequestID: 0}

// LogLevel represents a log level for use by StdCtx
type LogLevel uint8

const (
	// LogLevelError implies log all errors (default)
	LogLevelError LogLevel = iota
	// LogLevelWarn implies log all warnings
	LogLevelWarn
	// LogLevelDebug implies log all debug messages
	LogLevelDebug
	// LogLevelVerbose implies log all verbose messages
	LogLevelVerbose
)

// LogLevels are the currently enabled log levels for use by StdCtx.
var LogLevels = LogLevelError

// Logger is the default logger used, can be overridden by client
var Logger = log.New(os.Stdout, "", 0)

const timeFormat = "2006-01-02T15:04:05.000000000"

func prefix(t time.Time, requestID uint64) string {
	return fmt.Sprintf("%s | %7d: ", t.Format(timeFormat),
		requestID)
}

func logStdout(level LogLevel, requestID uint64, fmtStr string,
	args ...interface{}) {

	if LogLevels&(1<<level) != 0 {
		Logger.Printf(prefix(time.Now(), requestID)+fmtStr+"\n", args...)
	}
}

// Elog logs error messages when using StdCtx
func (s *StdCtx) Elog(fmtStr string, args ...interface{}) {
	logStdout(LogLevelError, s.RequestID, fmtStr, args...)
}

// Wlog logs warning messages when using StdCtx
func (s *StdCtx) Wlog(fmtStr string, args ...interface{}) {
	logStdout(LogLevelWarn, s.RequestID, fmtStr, args...)
}

// Dlog logs debug messages when using StdCtx
func (s *StdCtx) Dlog(fmtStr string, args ...interface{}) {
	logStdout(LogLevelDebug, s.RequestID, fmtStr, args...)
}

// Vlog logs verbose messages when using StdCtx
func (s *StdCtx) Vlog(fmtStr string, args ...interface{}) {
	logStdout(LogLevelVerbose, s.RequestID, fmtStr, args...)
}

type stdCtxExitFuncState struct {
	c        *StdCtx
	funcName string
}

// FuncIn logs the function entry when using StdCtx with args
func (s *StdCtx) FuncIn(funcName string, fmtStr string,
	args ...interface{}) FuncOut {
	entryFmt := fmt.Sprintf("In--- %s ", funcName)
	logStdout(LogLevelVerbose, s.RequestID, entryFmt+fmtStr, args...)
	return &stdCtxExitFuncState{c: s, funcName: funcName}
}

// FuncInName logs the function entry when using StdCtx
func (s *StdCtx) FuncInName(funcName string) FuncOut {
	return s.FuncIn(funcName, "")
}

// LogFuncOut logs the function exit when using StdCtx
func (es *stdCtxExitFuncState) Out() {
	exitFmt := fmt.Sprintf("--Out %s ", es.funcName)
	logStdout(LogLevelVerbose, es.c.RequestID, exitFmt)
}
