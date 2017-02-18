// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to workspaceroot.go

import "github.com/hanwen/go-fuse/fuse"

// Requires tree lock
func (wsr *WorkspaceRoot) generateHardlinkTypeKey_DOWN_(c *ctx,
	inodeNum InodeId) ([]byte, fuse.Status) {

	// Update the Hash value before generating the key
	if childInode := c.qfs.inodeNoInstantiate(c, inodeNum); childInode != nil {
		// Since the child is instantiated it may be modified, flush it
		// synchronously and recusively to be sure.
		childInode.Sync_DOWN(c)
	}

	defer wsr.RLock().RUnlock()

	isHardlink, linkId := wsr.checkHardlink(inodeNum)
	if !isHardlink {
		return nil, fuse.OK
	}

	valid, record := wsr.getHardlink(linkId)
	if !valid {
		c.elog("LockTree not protecting hardlink during keygen")
		return nil, fuse.OK
	}

	typeKey := encodeExtendedKey(record.ID(), record.Type(), record.Size())

	return typeKey, fuse.OK
}

