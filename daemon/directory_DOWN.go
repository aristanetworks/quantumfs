// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (dir *Directory) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::link_DOWN").out()

	// Grab the source parent as a Directory
	var srcParent *Directory
	switch v := srcInode.parent(c).(type) {
	case *Directory:
		srcParent = v
	case *WorkspaceRoot:
		srcParent = &(v.Directory)
	default:
		c.elog("Source directory is not a directory: %d",
			srcInode.inodeNum())
		return fuse.EINVAL
	}

	// Ensure the source and dest are in the same workspace
	if srcParent.wsr != dir.wsr {
		c.elog("Source and dest are not different workspaces.")
		return fuse.EPERM
	}

	newRecord, err := srcParent.makeHardlink_DOWN(c, srcInode)
	if err != fuse.OK {
		c.elog("QuantumFs::Link Failed with srcInode record")
		return err
	}
	srcInode.markSelfAccessed(c, false)

	newRecord.SetFilename(newName)

	// We cannot lock earlier because the parent of srcInode may be us
	defer dir.Lock().Unlock()

	inodeNum := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.loadChild(c, newRecord)
	}()

	dir.self.markAccessed(c, newName, true)

	c.dlog("Hardlinked %d to %s", srcInode.inodeNum(), newName)

	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		newRecord)

	dir.self.dirty(c)
	// Hardlinks aren't tracked by the uninstantiated list, they need a more
	// complicated reference counting system handled by workspaceroot

	return fuse.OK
}

func (dir *Directory) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("Directory::flush_DOWN", "%d %s", dir.inodeNum(),
		dir.name_).out()

	if !dir.isDirty() {
		c.vlog("directory %s not dirty", dir.name_)
		return dir.baseLayerId
	}

	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()

	dir.updateRecords_DOWN_(c)
	return dir.publish_(c)
}

// Walk the list of children which are dirty and have them recompute their new key
// wsr can update its new key.
//
// Requires the Inode lock and childRecordLock
func (dir *Directory) updateRecords_DOWN_(c *ctx) {
	defer c.funcIn("Directory::updateRecords_DOWN_").out()

	dirtyIds := dir.children.popDirty()
	for _, childId := range dirtyIds {
		child := c.qfs.inodeNoInstantiate(c, childId)
		newKey := child.flush_DOWN(c)
		dir.children.record(childId).SetID(newKey)
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
	// skip the first three Inodes: typespace / namespace / workspace
	for num := 3; num < length; num++ {
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

func (dir *Directory) makeHardlink_DOWN(c *ctx,
	toLink Inode) (copy DirectoryRecordIf, err fuse.Status) {

	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.children.record(toLink.inodeNum())
	if record == nil {
		return nil, fuse.ENOENT
	}

	// if the file isn't a hardlink, then we must flush it first
	if record.Type() != quantumfs.ObjectTypeHardlink {
		newKey := toLink.flush_DOWN(c)
		dir.children.recordByName(c, record.Filename()).SetID(newKey)
		dir.children.clearDirty(toLink.inodeNum())
	}

	dir.self.dirty(c)
	return dir.children.makeHardlink(c, record.Filename())
}
