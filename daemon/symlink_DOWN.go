// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// This is the _DOWN counterpart to symlink.go

import "github.com/hanwen/go-fuse/fuse"

func (link *Symlink) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Sync_DOWN(c *ctx) fuse.Status {
	defer c.funcIn("Symlink::Sync_DOWN").Out()
	link.flush(c)

	return fuse.OK
}

func (link *Symlink) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.wlog("Invalid MvChild_DOWN on Symlink")
	return fuse.ENOSYS
}
