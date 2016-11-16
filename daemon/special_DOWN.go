// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to special.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (special *Special) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on Special")
	return fuse.ENOTDIR
}

func (special *Special) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	special.setDirty(false)

	return special.embedDataIntoKey_(c)
}

func (special *Special) Sync_DOWN(c *ctx) fuse.Status {
	key := special.flush_DOWN(c)
	special.parent().syncChild(c, special.InodeCommon.id, key)

	return fuse.OK
}
