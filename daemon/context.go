// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "arista.com/quantumfs"

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
}

func (c *ctx) req(request uint64) *ctx {
	requestCtx := &ctx{
		qfs:          c.qfs,
		config:       c.config,
		workspaceDB:  c.workspaceDB,
		durableStore: c.durableStore,
		requestId:    request,
	}
	return requestCtx
}
