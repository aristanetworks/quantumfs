// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import (
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

// Generic request context object
type Ctx struct {
	Qlog      *qlog.Qlog
	Prefix    string
	RequestId uint64
}

// Log an Error message
func (c Ctx) Elog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	if len(c.Prefix) > 0 {
		format = c.Prefix + format
	}

	c.Qlog.Log(subsystem, c.RequestId, 0, "ERROR: "+format, args...)
}

// Log a Warning message
func (c Ctx) Wlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	if len(c.Prefix) > 0 {
		format = c.Prefix + format
	}

	c.Qlog.Log(subsystem, c.RequestId, 1, format, args...)
}

// Log a Debug message
func (c Ctx) Dlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	if len(c.Prefix) > 0 {
		format = c.Prefix + format
	}

	c.Qlog.Log(subsystem, c.RequestId, 2, format, args...)
}

// Log a Verbose tracing message
func (c Ctx) Vlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	if len(c.Prefix) > 0 {
		format = c.Prefix + format
	}

	c.Qlog.Log(subsystem, c.RequestId, 3, format, args...)
}

type ExitFuncLog struct {
	c         *Ctx
	subsystem qlog.LogSubsystem
	funcName  string
}

func (c *Ctx) FuncInName(subsystem qlog.LogSubsystem, funcName string) ExitFuncLog {
	format := qlog.FnEnterStr + funcName
	if len(c.Prefix) > 0 {
		format = c.Prefix + format
	}

	c.Qlog.Log(subsystem, c.RequestId, 3, format)
	return ExitFuncLog{
		c:         c,
		subsystem: subsystem,
		funcName:  funcName,
	}
}

func (c *Ctx) FuncIn(subsystem qlog.LogSubsystem, funcName string,
	extraFmtStr string, args ...interface{}) ExitFuncLog {

	// A format larger than LogStrSize bytes will be silently dropped
	format := [qlog.LogStrSize]byte{}
	index := 0

	if len(c.Prefix) > 0 {
		index += copy(format[index:], c.Prefix)
	}
	index += copy(format[index:], qlog.FnEnterStr)
	index += copy(format[index:], funcName)
	index += copy(format[index:], " ")
	index += copy(format[index:], extraFmtStr)
	formatStr := utils.MoveByteSliceToString(format[:index])

	c.Qlog.Log(subsystem, c.RequestId, 3, formatStr, args...)
	return ExitFuncLog{
		c:         c,
		subsystem: subsystem,
		funcName:  funcName,
	}
}

func (e ExitFuncLog) Out() {
	// A format larger than LogStrSize bytes will be silently dropped
	format := [qlog.LogStrSize]byte{}
	index := 0

	if len(e.c.Prefix) > 0 {
		index += copy(format[index:], e.c.Prefix)
	}
	index += copy(format[index:], qlog.FnExitStr)
	index += copy(format[index:], e.funcName)
	formatStr := utils.MoveByteSliceToString(format[:index])

	e.c.Qlog.Log(e.subsystem, e.c.RequestId, 3, formatStr)
}
