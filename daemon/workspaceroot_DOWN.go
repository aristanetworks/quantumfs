// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to workspaceroot.go

import "arista.com/quantumfs"

func (wsr *WorkspaceRoot) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("WorkspaceRoot::sync Enter")
	defer c.vlog("WorkspaceRoot::sync Exit")

	wsr.advanceRootId(c)
	return wsr.rootId
}
