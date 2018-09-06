// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to workspacelisting.go

import "github.com/hanwen/go-fuse/fuse"

func (wsl *WorkspaceList) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on WorkspaceList")
	return fuse.ENOTDIR
}

func (wsl *WorkspaceList) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (wsl *WorkspaceList) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild_DOWN on WorkspaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on NamespaceList")
	return fuse.ENOTDIR
}

func (nsl *NamespaceList) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (nsl *NamespaceList) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild_DOWN on NamespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on TypespaceList")
	return fuse.ENOTDIR
}

func (tsl *TypespaceList) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (tsl *TypespaceList) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild_DOWN on TypespaceList")
	return fuse.ENOSYS
}
