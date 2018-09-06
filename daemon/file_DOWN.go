// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to file.go

import "github.com/hanwen/go-fuse/fuse"

func (fi *File) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on File")
	return fuse.ENOTDIR
}

func (fi *File) Sync_DOWN(c *ctx) fuse.Status {
	defer c.funcIn("File::Sync_DOWN").Out()
	fi.flush(c)
	return fuse.OK
}

func (fi *File) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild_DOWN on File")
	return fuse.ENOSYS
}
