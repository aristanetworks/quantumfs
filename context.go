// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package quantumfs

import (
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils/dangerous"
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
	level     uint8
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
		level:     3,
	}
}

func (c *Ctx) functionEntryLog(subsystem qlog.LogSubsystem, funcName string,
	extraFmtStr string, level uint8, args ...interface{}) ExitFuncLog {

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
	formatStr := dangerous.MoveByteSliceToString(format[:index])

	c.Qlog.Log(subsystem, c.RequestId, level, formatStr, args...)
	return ExitFuncLog{
		c:         c,
		subsystem: subsystem,
		funcName:  funcName,
		level:     level,
	}
}

func (c *Ctx) FuncIn(subsystem qlog.LogSubsystem, funcName string,
	extraFmtStr string, args ...interface{}) ExitFuncLog {

	return c.functionEntryLog(subsystem, funcName, extraFmtStr, 3, args...)
}

func (c *Ctx) StatsFuncInName(subsystem qlog.LogSubsystem,
	funcName string) ExitFuncLog {

	format := qlog.FnEnterStr + funcName
	if len(c.Prefix) > 0 {
		format = c.Prefix + format
	}

	c.Qlog.Log(subsystem, c.RequestId, 2, format)
	return ExitFuncLog{
		c:         c,
		subsystem: subsystem,
		funcName:  funcName,
		level:     2,
	}
}

func (c *Ctx) StatsFuncIn(subsystem qlog.LogSubsystem, funcName string,
	extraFmtStr string, args ...interface{}) ExitFuncLog {

	return c.functionEntryLog(subsystem, funcName, extraFmtStr, 2, args...)
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
	formatStr := dangerous.MoveByteSliceToString(format[:index])

	e.c.Qlog.Log(e.subsystem, e.c.RequestId, e.level, formatStr)
}
