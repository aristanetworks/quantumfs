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

	dir.dirChildren.setRecord(c, newRecord)
	inodeNum, exists := dir.dirChildren.getInode(c, newRecord.Filename())
	if !exists {
		panic("Failure to set record in children")
	}
	dir.self.markAccessed(c, newName, true)

	c.dlog("CoW linked %d to %s as inode %d", srcInode.inodeNum(), newName,
		inodeNum)

	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		newRecord)

	dir.self.dirty(c)

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
	dirtyChildren := dir.dirChildren.popDirtyInodes()
	if dirtyChildren == nil {
		return
	}

	for _, childId := range dirtyChildren {
		child := c.qfs.inode(c, childId)

		newKey := child.flush_DOWN(c)
		record, exists := dir.dirChildren.getRecord(c, childId)
		if !exists {
			panic("Unexpected missing child during update")
		}
		record.SetID(newKey)
	}
}

func (dir *Directory) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (dir *directorySnapshot) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

// Return extended key by combining ObjectKey, inode type, and inode size
func (dir *Directory) generateChildTypeKey_DOWN(c *ctx, inodeNum InodeId) ([]byte,
	fuse.Status) {

	// Update the Hash value before generating the key
	dir.flush_DOWN(c)

	// flush_DOWN already acquired an Inode lock exclusively. In case of the
        // dead lock, the Inode lock for reading should be required after releasing
        // its exclusive lock. The gap between two locks, other threads cannot come
        // in because the function holds the exclusive tree lock, so it is the only
        // thread accessing this Inode. Also, recursive lock requiring won't occur.
	defer dir.RLock().RUnlock()
	record, err := dir.getChildRecord(c, inodeNum)
	if err != nil {
		c.elog("Unable to get record from parent for inode %s", inodeNum)
		return nil, fuse.EIO
	}
	typeKey := encodeExtendedKey(record.ID(), record.Type(), record.Size())

	return typeKey, fuse.OK
}

// go along the given path to the destination
// The path is stored in a string slice, each cell index contains an inode
func (dir *Directory) followPath_DOWN(c *ctx, path []string) (Inode, error) {

	// traverse through the workspace, reach the target inode
	length := len(path) - 1 // leave the target node at the end
	currDir := dir
	// skip the first two Inodes: namespace / workspace
	for num := 2; num < length; num++ {
		// all preceding nodes have to be directories
		child, err := currDir.lookupInternal(c, path[num],
			quantumfs.ObjectTypeDirectoryEntry)
		if err != nil {
			return child, err
		}
		currDir = child.(*Directory)
	}

	return currDir, nil
}
