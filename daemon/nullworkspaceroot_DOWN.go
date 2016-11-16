// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// handler of creating a hardlink in nullworkspace
func (nwsr *NullWorkspaceRoot) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullWorkspaceRoot) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("NullWorkspaceRoot::sync Enter")
	defer c.vlog("NullWorkspaceRoot::sync Exit")

	nwsr.Directory.flush_DOWN(c)
	return nwsr.rootId
}
