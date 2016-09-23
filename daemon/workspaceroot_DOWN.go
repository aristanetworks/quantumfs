// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to workspaceroot.go

import "github.com/aristanetworks/quantumfs"

func (wsr *WorkspaceRoot) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("WorkspaceRoot::sync Enter")
	defer c.vlog("WorkspaceRoot::sync Exit")

	wsr.Directory.flush_DOWN(c)
	wsr.publish(c)
	return wsr.rootId
}
