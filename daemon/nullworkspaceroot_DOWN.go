// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/hanwen/go-fuse/fuse"

// handler of creating a hardlink in nullworkspace
func (nwsr *NullSpaceNameRoot) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}
