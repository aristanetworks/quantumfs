// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (dir *Directory) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::link_DOWN").out()

	// Make sure the file's flushed before we try to hardlink it. We can't do
	// this with the inode parentLock locked since Sync locks the parent as well.
	srcInode.Sync_DOWN(c)

	newRecord, err := func() (quantumfs.DirectoryRecord, fuse.Status) {
		defer srcInode.getParentLock().Lock().Unlock()

		// ensure we're not orphaned
		if srcInode.isOrphaned_() {
			c.wlog("Can't hardlink an orphaned file")
			return nil, fuse.EPERM
		}

		// Grab the source parent as a Directory
		var srcParent *Directory
		switch v := srcInode.parent_(c).(type) {
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
		if srcParent.wsr != dir.wsr {
			c.dlog("Source and dest are different workspaces.")
			return nil, fuse.EPERM
		}

		newRecord, err := srcParent.makeHardlink_DOWN_(c, srcInode)
		if err != fuse.OK {
			c.elog("Link Failed with srcInode record")
			return nil, err
		}

		// We need to reparent under the srcInode lock
		srcInode.setParent_(dir.wsr.inodeNum())

		return newRecord, fuse.OK
	}()
	if err != fuse.OK {
		return err
	}

	srcInode.markSelfAccessed(c, false)

	newRecord.SetFilename(newName)

	// We cannot lock earlier because the parent of srcInode may be us
	defer dir.Lock().Unlock()

	inodeNum := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.loadChild(c, newRecord, quantumfs.InodeIdInvalid)
	}()

	dir.self.markAccessed(c, newName, true)

	c.dlog("Hardlinked %d to %s", srcInode.inodeNum(), newName)

	out.NodeId = uint64(inodeNum)
	c.qfs.increaseLookupCount(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		newRecord)

	dir.self.dirty(c)
	// Hardlinks aren't tracked by the uninstantiated list, they need a more
	// complicated reference counting system handled by workspaceroot

	return fuse.OK
}

func (dir *Directory) Sync_DOWN(c *ctx) fuse.Status {
	defer c.FuncIn("Directory::Sync_DOWN", "dir %d", dir.inodeNum()).out()

	children := dir.directChildInodes()
	for _, child := range children {
		if inode := c.qfs.inodeNoInstantiate(c, child); inode != nil {
			inode.Sync_DOWN(c)
		}
	}

	dir.flush(c)

	return fuse.OK
}

func (dir *directorySnapshot) Sync_DOWN(c *ctx) fuse.Status {
	c.vlog("directorySnapshot::Sync_DOWN doing nothing")
	return fuse.OK
}

// Return extended key by combining ObjectKey, inode type, and inode size
func (dir *Directory) generateChildTypeKey_DOWN(c *ctx, inodeNum InodeId) ([]byte,
	fuse.Status) {

	defer c.FuncIn("Directory::generateChildTypeKey_DOWN", "inode %d",
		inodeNum).out()

	// flush already acquired an Inode lock exclusively. In case of the dead
	// lock, the Inode lock for reading should be required after releasing its
	// exclusive lock. The gap between two locks, other threads cannot come in
	// because the function holds the exclusive tree lock, so it is the only
	// thread accessing this Inode. Also, recursive lock requiring won't occur.
	defer dir.RLock().RUnlock()
	record, err := dir.getChildRecordCopy(c, inodeNum)
	if err != nil {
		c.elog("Unable to get record from parent for inode %s", inodeNum)
		return nil, fuse.EIO
	}
	typeKey := record.EncodeExtendedKey()

	return typeKey, fuse.OK
}

// The returned Forget function of terminal directory should be triggered in the end
// of the caller
func (dir *Directory) followPath_DOWN(c *ctx, path []string) (terminalDir Inode,
	cleanup func(), err error) {

	defer c.funcIn("Directory::followPath_DOWN").out()

	// Traverse through the workspace, reach the target inode
	length := len(path) - 1 // leave the target node at the end
	currDir := dir
	// Indicate we've started instantiating inodes and therefore need to start
	// Forgetting them
	startForgotten := false
	// Go along the given path to the destination. The path is stored in a string
	// slice, each cell index contains an inode.
	// Skip the first three Inodes: typespace / namespace / workspace
	for num := 3; num < length; num++ {
		if startForgotten {
			// The lookupInternal() doesn't increase the lookupCount of
			// the current directory, so it should be forgotten with 0
			defer c.qfs.Forget(uint64(currDir.inodeNum()), 0)
		}
		// all preceding nodes have to be directories
		child, instantiated, err := currDir.lookupInternal(c, path[num],
			quantumfs.ObjectTypeDirectoryEntry)
		startForgotten = !instantiated
		if err != nil {
			return child, nil, err
		}

		currDir = child.(*Directory)
	}

	cleanup = func() {
		c.qfs.Forget(uint64(currDir.inodeNum()), 0)
	}
	return currDir, cleanup, nil
}

// the toLink parentLock must be locked
func (dir *Directory) makeHardlink_DOWN_(c *ctx,
	toLink Inode) (copy quantumfs.DirectoryRecord, err fuse.Status) {

	defer c.funcIn("Directory::makeHardlink_DOWN").out()

	// If someone is trying to link a hardlink, we just need to return a copy
	if isHardlink, id := dir.wsr.checkHardlink(toLink.inodeNum()); isHardlink {
		// Update the reference count
		dir.wsr.hardlinkInc(id)

		linkCopy := newHardlink(toLink.name(), id, dir.wsr)
		return linkCopy, fuse.OK
	}

	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()

	return dir.children.makeHardlink(c, toLink.inodeNum())
}
