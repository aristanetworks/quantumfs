// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

type clientname string

type ctx struct {
	quantumfs.Ctx
	clientName clientname
}

func (c *ctx) elog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogWorkspaceDb, uint64(c.RequestId), 0, "ERROR: "+format,
		args...)
}

func (c *ctx) wlog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogWorkspaceDb, uint64(c.RequestId), 1, format, args...)
}

func (c *ctx) dlog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogWorkspaceDb, uint64(c.RequestId), 2, format, args...)
}

func (c *ctx) vlog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogWorkspaceDb, uint64(c.RequestId), 3, format, args...)
}

func (c *ctx) funcIn(funcName string) quantumfs.ExitFuncLog {
	return c.Ctx.FuncIn(qlog.LogWorkspaceDb, funcName, "")
}

func (c *ctx) FuncIn(funcName string, extraFmtStr string,
	args ...interface{}) quantumfs.ExitFuncLog {

	return c.Ctx.FuncIn(qlog.LogWorkspaceDb, funcName, extraFmtStr, args...)
}
