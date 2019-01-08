// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// This is the _DOWN counterpart to special.go

import "github.com/hanwen/go-fuse/fuse"

func (special *Special) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on Special")
	return fuse.ENOTDIR
}

func (special *Special) Sync_DOWN(c *ctx) fuse.Status {
	defer c.funcIn("Special::Sync_DOWN").Out()
	special.flush(c)

	return fuse.OK
}

func (special *Special) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.wlog("Invalid MvChild_DOWN on Special")
	return fuse.ENOSYS
}
