// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to symlink.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (link *Symlink) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	link.parent(c).syncChild(c, link.inodeNum(), link.key)
	return link.key
}

func (link *Symlink) Sync_DOWN(c *ctx) fuse.Status {
	link.flush_DOWN(c)

	return fuse.OK
}
