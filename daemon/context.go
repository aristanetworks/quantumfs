// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"sync/atomic"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/hanwen/go-fuse/fuse"
)

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
			Prefix:    c.Prefix,
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

var flusherRequestIdGenerator = qlog.RefreshRequestIdMin

// Assign a unique request id to the context for a flusher goroutine
func (c *ctx) flusherCtx() *ctx {
	nc := *c
	nc.Ctx.RequestId = atomic.AddUint64(&flusherRequestIdGenerator, 1)
	return &nc
}

var forgetRequestIdGenerator = qlog.ForgetRequstIdMin

// Assign a unique request id to the context for a forget goroutine
func (c *ctx) forgetCtx() *ctx {
	nc := *c
	nc.Ctx.RequestId = atomic.AddUint64(&forgetRequestIdGenerator, 1)
	return &nc
}

// local daemon package specific log wrappers
func (c *ctx) elog(format string, args ...interface{}) {
	c.Ctx.Elog(qlog.LogDaemon, format, args...)
}

func (c *ctx) wlog(format string, args ...interface{}) {
	c.Ctx.Wlog(qlog.LogDaemon, format, args...)
}

func (c *ctx) dlog(format string, args ...interface{}) {
	c.Ctx.Dlog(qlog.LogDaemon, format, args...)
}

func (c *ctx) vlog(format string, args ...interface{}) {
	c.Ctx.Vlog(qlog.LogDaemon, format, args...)
}

func (c *ctx) funcIn(funcName string) quantumfs.ExitFuncLog {
	return c.Ctx.FuncIn(qlog.LogDaemon, funcName, "")
}

func (c *ctx) FuncIn(funcName string, extraFmtStr string,
	args ...interface{}) quantumfs.ExitFuncLog {

	return c.Ctx.FuncIn(qlog.LogDaemon, funcName, extraFmtStr, args...)
}
