// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "github.com/aristanetworks/quantumfs/qlog"

// Generic request context object
type Ctx struct {
	Qlog      *qlog.Qlog
	RequestId uint64
}

func (c Ctx) Elog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 0, format, args...)
}

func (c Ctx) Wlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 1, format, args...)
}

func (c Ctx) Dlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 2, format, args...)
}

func (c Ctx) Vlog(subsystem qlog.LogSubsystem, format string, args ...interface{}) {
	c.Qlog.Log(subsystem, c.RequestId, 3, format, args...)
}

type ExitFuncLog struct {
	c         *Ctx
	subsystem qlog.LogSubsystem
	funcName  string
}

func (c Ctx) FuncIn(subsystem qlog.LogSubsystem, funcName string,
	extraFmtStr string, args ...interface{}) ExitFuncLog {

	c.Qlog.Log(subsystem, c.RequestId, 3, qlog.FnEnterStr+
		funcName+" "+extraFmtStr, args...)

	return ExitFuncLog{
		c:         &c,
		subsystem: subsystem,
		funcName:  funcName,
	}
}

func (e ExitFuncLog) Out() {
	e.c.Qlog.Log(e.subsystem, e.c.RequestId, 3, qlog.FnExitStr+
		e.funcName)
}
