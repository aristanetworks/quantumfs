// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to workspaceroot.go

import "github.com/aristanetworks/quantumfs"

func (wsr *WorkspaceRoot) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("WorkspaceRoot::flush_DOWN Enter")
	defer c.vlog("WorkspaceRoot::flush_DOWN Exit")

	wsr.Directory.flush_DOWN(c)
	wsr.flushHardlinks_DOWN(c)
	wsr.publish(c)
	return wsr.rootId
}

func (wsr *WorkspaceRoot) flushHardlinks_DOWN(c *ctx) {
	defer wsr.linkLock.RLock().RUnlock()

	for inodeId, hardlinkId := range wsr.dirtyLinks {
		child := c.qfs.inodeNoInstantiate(c, inodeId)
		if child == nil {
			c.elog("Uninstantiated dirty hardlink? %d %d", inodeId,
				hardlinkId)
			continue
		}

		newKey := child.flush_DOWN(c)
		wsr.hardlinks[hardlinkId].record.SetID(newKey)
	}

	wsr.dirtyLinks = make(map[InodeId]uint64, 0)
}
