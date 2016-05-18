// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "arista.com/quantumfs"
import "arista.com/quantumfs/qlog"
import "github.com/hanwen/go-fuse/fuse"

// The ctx type needs to be threaded through all the objects and calls of the
// system. It provides access to the configuration, the quantumfs mux and request
// specific logging information.
//
// If Go ever gets goroutine local storage it may be cleaner to move these contents
// to using that instead of threading it everywhere.
type ctx struct {
	qfs          *QuantumFs
	config       *QuantumFsConfig
	workspaceDB  quantumfs.WorkspaceDB
	durableStore quantumfs.DataStore
	requestId    uint64
	fuseCtx      fuse.Context
}

func (c *ctx) req(header *fuse.InHeader) *ctx {
	requestCtx := &ctx{
		qfs:          c.qfs,
		config:       c.config,
		workspaceDB:  c.workspaceDB,
		durableStore: c.durableStore,
		requestId:    header.Unique,
                fuseCtx:      header.Context,
	}
	return requestCtx
}

//only to be used for some testing - not all functions will work with this
func (c *ctx) dummyReq(request uint64) *ctx {
	requestCtx := &ctx{
		qfs:          c.qfs,
		config:       c.config,
		workspaceDB:  c.workspaceDB,
		durableStore: c.durableStore,
		requestId:    request,
	}
	return requestCtx
}

// local daemon package specific log wrappers
func (c ctx) elog(format string, args ...interface{}) {
	qlog.Log(qlog.LogDaemon, c.requestId, 0, format, args...)
}

func (c ctx) wlog(format string, args ...interface{}) {
	qlog.Log(qlog.LogDaemon, c.requestId, 1, format, args...)
}

func (c ctx) dlog(format string, args ...interface{}) {
	qlog.Log(qlog.LogDaemon, c.requestId, 2, format, args...)
}

func (c ctx) vlog(format string, args ...interface{}) {
	qlog.Log(qlog.LogDaemon, c.requestId, 3, format, args...)
}
