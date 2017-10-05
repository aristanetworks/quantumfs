// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "github.com/aristanetworks/quantumfs/qlog"

// Generic request context object
type Ctx struct {
	Qlog      *qlog.Qlog
	Prefix    string
	RequestId uint64
}

// Log an Error message
func (c Ctx) Elog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 0, c.Prefix+"ERROR: "+format, args...)
}

// Log a Warning message
func (c Ctx) Wlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 1, c.Prefix+format, args...)
}

// Log a Debug message
func (c Ctx) Dlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 2, c.Prefix+format, args...)
}

// Log a Verbose tracing message
func (c Ctx) Vlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 3, c.Prefix+format, args...)
}

type ExitFuncLog struct {
	c         *Ctx
	subsystem qlog.LogSubsystem
	funcName  string
}

func (c Ctx) FuncInName(subsystem qlog.LogSubsystem, funcName string) ExitFuncLog {
	c.Qlog.Log(subsystem, c.RequestId, 3, c.Prefix+qlog.FnEnterStr+funcName)
	return ExitFuncLog{
		c:         &c,
		subsystem: subsystem,
		funcName:  funcName,
	}
}

func (c Ctx) FuncIn(subsystem qlog.LogSubsystem, funcName string,
	extraFmtStr string, args ...interface{}) ExitFuncLog {

	c.Qlog.Log(subsystem, c.RequestId, 3, c.Prefix+qlog.FnEnterStr+
		funcName+" "+extraFmtStr, args...)
	return ExitFuncLog{
		c:         &c,
		subsystem: subsystem,
		funcName:  funcName,
	}
}

func (e ExitFuncLog) Out() {
	e.c.Qlog.Log(e.subsystem, e.c.RequestId, 3, e.c.Prefix+qlog.FnExitStr+
		e.funcName)
}
