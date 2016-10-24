// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (dir *Directory) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Directory::Link Enter")
	defer c.vlog("Directory::Link Exit")

	origRecord, err := srcInode.parent().getChildRecord(c, srcInode.inodeNum())
	if err != nil {
		c.elog("QuantumFs::Link Failed to get srcInode record %v:", err)
		return fuse.EIO
	}
	srcInode.markSelfAccessed(c, false)

	newRecord := cloneDirectoryRecord(&origRecord)
	newRecord.SetFilename(newName)
	newRecord.SetID(srcInode.flush_DOWN(c))

	// We cannot lock earlier because the parent of srcInode may be us
	defer dir.Lock().Unlock()

	inodeNum := dir.loadChild_(c, *newRecord)
	dir.self.markAccessed(c, newName, true)

	c.dlog("CoW linked %d to %s as inode %d", srcInode.inodeNum(), newName,
		inodeNum)

	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		newRecord)

	dir.self.dirty(c)
	c.qfs.addUninstantiated(c, []InodeId{inodeNum}, dir)

	return fuse.OK
}

func (dir *Directory) forget_DOWN(c *ctx) {
	c.vlog("Directory::forget_DOWN not yet supported")
}

func (dir *Directory) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("Directory::sync Enter")
	defer c.vlog("Directory::sync Exit")
	if !dir.isDirty() {
		c.vlog("directory not dirty")
		return dir.baseLayerId
	}

	defer dir.Lock().Unlock()

	dir.updateRecords_DOWN_(c)
	return dir.publish(c)
}

// Walk the list of children which are dirty and have them recompute their new key
// wsr can update its new key.
func (dir *Directory) updateRecords_DOWN_(c *ctx) {
	for _, child := range dir.dirtyChildren_ {
		newKey := child.flush_DOWN(c)
		dir.childrenRecords[child.inodeNum()].SetID(newKey)
	}
	dir.dirtyChildren_ = make(map[InodeId]Inode, 0)
}

func (dir *Directory) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (dir *directorySnapshot) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}
