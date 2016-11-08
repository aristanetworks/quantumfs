// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/hanwen/go-fuse/fuse"

// The ctx type needs to be threaded through all the objects and calls of the
// system. It provides access to the configuration, the quantumfs mux and request
// specific logging information.
//
// If Go ever gets goroutine local storage it may be cleaner to move these contents
// to using that instead of threading it everywhere.
type ctx struct {
	quantumfs.Ctx
	qfs         *QuantumFs
	config      *QuantumFsConfig
	workspaceDB quantumfs.WorkspaceDB
	dataStore   *dataStore
	fuseCtx     *fuse.Context
}

func (c *ctx) reqId(reqId uint64, context *fuse.Context) *ctx {
	requestCtx := &ctx{
		Ctx: quantumfs.Ctx{
			Qlog:      c.Qlog,
			RequestId: reqId,
		},
		qfs:         c.qfs,
		config:      c.config,
		workspaceDB: c.workspaceDB,
		dataStore:   c.dataStore,
		fuseCtx:     context,
	}
	return requestCtx
}

func (c *ctx) req(header *fuse.InHeader) *ctx {
	return c.reqId(header.Unique, &header.Context)
}

// local daemon package specific log wrappers
func (c *ctx) elog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogDaemon, uint64(c.RequestId), 0, format, args...)
}

func (c *ctx) wlog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogDaemon, uint64(c.RequestId), 1, format, args...)
}

func (c *ctx) dlog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogDaemon, uint64(c.RequestId), 2, format, args...)
}

func (c *ctx) vlog(format string, args ...interface{}) {
	c.Qlog.Log(qlog.LogDaemon, uint64(c.RequestId), 3, format, args...)
}

type exitLog struct {
	c        *ctx
	funcName string
}

func (c *ctx) flog(funcName string) exitLog {

	c.Qlog.Log(qlog.LogDaemon, uint64(c.RequestId), 3, qlog.FnEnterStr+funcName)

	return exitLog{
		c:        c,
		funcName: funcName,
	}
}

func (c *ctx) Flog(funcName string, extraFmtStr string,
	args ...interface{}) exitLog {

	c.Qlog.Log(qlog.LogDaemon, uint64(c.RequestId), 3, qlog.FnEnterStr+
		funcName+extraFmtStr, args...)

	return exitLog{
		c:        c,
		funcName: funcName,
	}
}

func (e exitLog) exit() {
	e.c.Qlog.Log(qlog.LogDaemon, uint64(e.c.RequestId), 3, qlog.FnExitStr+
		e.funcName)
}
