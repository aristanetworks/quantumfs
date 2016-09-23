// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to workspacelisting.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (wsl *WorkspaceList) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on WorkspaceList")
	return fuse.ENOTDIR
}

func (wsl *WorkspaceList) forget_DOWN(c *ctx) {
	c.elog("Invalid forget_DOWN on WorkspaceList")
}

func (wsl *WorkspaceList) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}

func (wsl *WorkspaceList) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (nsl *NamespaceList) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on NamespaceList")
	return fuse.ENOTDIR
}

func (nsl *NamespaceList) forget_DOWN(c *ctx) {
	c.elog("Invalid forget_DOWN on NamespaceList")
}

func (nsl *NamespaceList) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}

func (nsl *NamespaceList) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}
