// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// _DOWN counterpart to lowmemfile.go

import "github.com/hanwen/go-fuse/fuse"

func (lm *LowMemFile) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on LowMemFile")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) Sync_DOWN(c *ctx) fuse.Status {
	c.vlog("LowMemFile::Sync_DOWN doing nothing")
	return fuse.OK
}

func (lm *LowMemFileHandle) Sync_DOWN(c *ctx) fuse.Status {
	c.vlog("ApiHandle::Sync_DOWN doing nothing")
	return fuse.OK
}