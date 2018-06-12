// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// _DOWN counterpart to api.go

import "github.com/hanwen/go-fuse/fuse"

func (api *ApiInode) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Link on ApiInode")
	return fuse.ENOTDIR
}

func (api *ApiInode) Sync_DOWN(c *ctx) fuse.Status {
	c.vlog("ApiInode::Sync_DOWN doing nothing")
	return fuse.OK
}
