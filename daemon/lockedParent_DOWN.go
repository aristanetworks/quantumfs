// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to lockedParent.go
// We must be careful not to call anything that calls Sync_DOWN on a child,
// since that will require coming here and doubly locking the parent

import "github.com/hanwen/go-fuse/fuse"

func (lp *lockedParent) makeHardlink_DOWN(c *ctx, srcInode Inode,
	destWsr *WorkspaceRoot) (DirectoryRecordIf, fuse.Status) {

	defer lp.lock.Lock().Unlock()

	// Grab the source parent as a Directory
	var srcParent *Directory
	switch v := lp.parent_(c).(type) {
	case *Directory:
		srcParent = v
	case *WorkspaceRoot:
		srcParent = &(v.Directory)
	default:
		c.elog("Source directory is not a directory: %d",
			srcInode.inodeNum())
		return nil, fuse.EINVAL
	}

	// Ensure the source and dest are in the same workspace
	if srcParent.wsr != destWsr {
		c.dlog("Source and dest are different workspaces.")
		return nil, fuse.EPERM
	}

	newRecord, err := srcParent.makeHardlink_DOWN(c, srcInode)
	if err != fuse.OK {
		c.elog("Link Failed with srcInode record")
		return nil, err
	}

	return newRecord, fuse.OK
}

func (lp *lockedParent) generateChildTypeKey_DOWN(c *ctx, inodeNum InodeId) ([]byte,
	fuse.Status) {

	defer lp.lock.Lock().Unlock()

	var dir *Directory
	parent := lp.parent_(c)
	if parent.isWorkspaceRoot() {
		dir = &parent.(*WorkspaceRoot).Directory
	} else {
		dir = parent.(*Directory)
	}

	return dir.generateChildTypeKey_DOWN(c, inodeNum)
}
