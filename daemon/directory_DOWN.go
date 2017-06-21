// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func (dir *Directory) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::link_DOWN").Out()

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

	newRecord.SetFilename(newName)

	// We cannot lock earlier because the parent of srcInode may be us
	defer dir.Lock().Unlock()

	inodeNum := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.loadChild(c, newRecord, quantumfs.InodeIdInvalid)
	}()

	dir.self.markAccessed(c, newName,
		markType(newRecord.Type(), quantumfs.PathCreated))

	c.dlog("Hardlinked %d to %s", srcInode.inodeNum(), newName)

	out.NodeId = uint64(inodeNum)
	c.qfs.increaseLookupCount(c, inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		newRecord)

	dir.self.dirty(c)
	// Hardlinks aren't tracked by the uninstantiated list, they need a more
	// complicated reference counting system handled by workspaceroot

	return fuse.OK
}

func (dir *Directory) Sync_DOWN(c *ctx) fuse.Status {
	defer c.FuncIn("Directory::Sync_DOWN", "dir %d", dir.inodeNum()).Out()

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
		inodeNum).Out()

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

// The returned cleanup function of terminal directory should be called at the end of
// the caller
func (dir *Directory) followPath_DOWN(c *ctx, path []string) (terminalDir Inode,
	cleanup func(), err error) {

	defer c.funcIn("Directory::followPath_DOWN").Out()
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
			quantumfs.ObjectTypeDirectory)
		startForgotten = !instantiated
		if err != nil {
			return child, func() {}, err
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

	defer c.funcIn("Directory::makeHardlink_DOWN").Out()

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

func (dir *Directory) normalizeHardlinks_DOWN(c *ctx,
	hrc *HardlinkRefreshCtx, localRecord quantumfs.DirectoryRecord,
	remoteRecord *quantumfs.DirectRecord) *quantumfs.DirectRecord {

	defer c.funcIn("Directory::normalizeHardlinks_DOWN").Out()
	inodeId := dir.children.inodeNum(remoteRecord.Filename())
	inode := c.qfs.inodeNoInstantiate(c, inodeId)

	if localRecord.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(localRecord.ID())
		hrc.claimedLinks[linkId] = inodeId
		inode.setParent(dir.inodeNum())
		return remoteRecord
	}
	utils.Assert(remoteRecord.Type() == quantumfs.ObjectTypeHardlink,
		"either local or remote should be hardlinks to be normalized")

	linkId := decodeHardlinkKey(remoteRecord.ID())
	valid, hardlinkRecord := dir.wsr.getHardlink(linkId)
	utils.Assert(valid, "hardlink %d not found", linkId)
	dir.wsr.updateHardlinkInodeId(c, linkId, inodeId)
	inode.setParent(dir.wsr.inodeNum())
	return &hardlinkRecord
}

func (dir *Directory) unlinkChild_DOWN(c *ctx, childname string, childId InodeId) {
	defer c.FuncIn("Directory::unlinkChild_DOWN", "%s", childname).Out()

	inode := c.qfs.inodeNoInstantiate(c, childId)
	if inode == nil {
		dir.children.deleteChild(c, childname, false)
	} else {
		localRecord := dir.children.recordByName(c, childname)
		if localRecord.Type() == quantumfs.ObjectTypeDirectory {
			subdir := inode.(*Directory)
			subdir.children.foreachChild(c, func(childname string,
				childId InodeId) {

				subdir.unlinkChild_DOWN(c, childname, childId)
			})
		}
		result := inode.deleteSelf(c,
			func() (quantumfs.DirectoryRecord, fuse.Status) {
				delRecord := dir.children.deleteChild(c, childname,
					false)
				return delRecord, fuse.OK
			})
		if result != fuse.OK {
			panic("XXX handle deletion failure")
		}
	}
	c.qfs.noteDeletedInode(dir.id, childId, childname)
}

func (dir *Directory) handleChild_DOWN(c *ctx, hrc *HardlinkRefreshCtx,
	remoteRecord *quantumfs.DirectRecord,
	childname string, childId InodeId) (newInodeId *InodeId,
	removedInodeId *InodeId) {

	defer c.FuncIn("Directory::handleChild_DOWN", "%s", childname).Out()
	existsOnRemote := remoteRecord != nil
	localRecord := dir.children.recordByName(c, childname)
	canReuseInode := func() bool {
		if !existsOnRemote {
			return false
		}
		if localRecord == nil {
			c.vlog("%s does not exist locally.", childname)
			return false
		}
		if !underlyingTypesMatch(dir.wsr, localRecord, remoteRecord) {
			c.vlog("%s had a major type change %d -> %d",
				childname, underlyingTypeOf(dir.wsr, localRecord),
				underlyingTypeOf(dir.wsr, remoteRecord))
			return false
		}
		if localRecord.Type().IsImmutable() {
			// If this is an immutable type, then dentry must have been
			// unlinked and recreated
			return false
		}
		if localRecord.Type() == quantumfs.ObjectTypeHardlink &&
			remoteRecord.Type() != quantumfs.ObjectTypeHardlink {

			linkId := decodeHardlinkKey(localRecord.ID())
			if _, exists := hrc.claimedLinks[linkId]; exists {
				// There are other dentries that might have claimed
				// this inode, or it has not been stale at all.
				c.vlog("%s cannot inherit the hardlink inode",
					childname)
				return false
			}
		}
		if remoteRecord.Type() == quantumfs.ObjectTypeHardlink &&
			localRecord.Type() != quantumfs.ObjectTypeHardlink {

			linkId := decodeHardlinkKey(remoteRecord.ID())
			inodeId := dir.wsr.getInodeIdNoInstantiate(c, linkId)
			if inodeId != quantumfs.InodeIdInvalid {
				c.vlog("link %d already has inode %d associated",
					linkId, inodeId)
				// someone (the other leg of the hardlink) has given
				// this hardlink an inode id already
				return false
			}
		}
		return true
	}()
	if !canReuseInode && localRecord != nil {
		dir.unlinkChild_DOWN(c, childname, childId)
		removedInodeId = &childId
	}
	if !existsOnRemote {
		return
	}
	if !canReuseInode {
		// We have to allocate a new inode for this record
		inodeId := dir.children.loadChild(c, remoteRecord,
			quantumfs.InodeIdInvalid)
		status := c.qfs.noteChildCreated(dir.id, remoteRecord.Filename())
		utils.Assert(status == fuse.OK,
			"marking %s created failed with %d", remoteRecord.Filename(),
			status)
		newInodeId = &inodeId
		return
	}
	if remoteRecord.ID().IsEqualTo(localRecord.ID()) {
		return
	}

	c.wlog("entry %s goes %d:%s -> %d:%s", remoteRecord.Filename(),
		localRecord.Type(), localRecord.ID().Text(),
		remoteRecord.Type(), remoteRecord.ID().Text())

	utils.Assert(underlyingTypesMatch(dir.wsr, localRecord, remoteRecord),
		"type mismatch %d vs. %d", underlyingTypeOf(dir.wsr, localRecord),
		underlyingTypeOf(dir.wsr, remoteRecord))

	if !localRecord.Type().Matches(remoteRecord.Type()) {
		remoteRecord = dir.normalizeHardlinks_DOWN(c, hrc, localRecord,
			remoteRecord)
		// XXX In the future, a nil remoteRecord from normalization function
		// should signal lack of enough information to process the record.
		// In such an event, keep the pair of (localRecord, remoteRecord)
		// for post-processing and return without reloading the inode.
	}
	if inode := c.qfs.inodeNoInstantiate(c, childId); inode != nil {
		reload(c, hrc, inode, *remoteRecord)
	}
	status := c.qfs.invalidateInode(childId)
	utils.Assert(status == fuse.OK,
		"invalidating %d failed with %d", childId, status)

	return
}

// Returns the list of new uninstantiated inodes ids and the list of
// inode ids that should be removed
func (dir *Directory) refresh_DOWN(c *ctx,
	hrc *HardlinkRefreshCtx,
	baseLayerId quantumfs.ObjectKey) ([]InodeId, []InodeId) {

	defer c.funcIn("Directory::refresh_DOWN").Out()
	uninstantiated := make([]InodeId, 0)
	deletedInodeIds := make([]InodeId, 0)

	remoteEntries := make(map[string]*quantumfs.DirectRecord, 0)

	foreachDentry(c, baseLayerId, func(record *quantumfs.DirectRecord) {
		remoteEntries[record.Filename()] = record
	})

	defer dir.childRecordLock.Lock().Unlock()

	// go through all local entries and refresh them to correctly
	// represent remote
	dir.children.foreachChild(c, func(childname string, childId InodeId) {
		remoteRecord := remoteEntries[childname]
		newInodeId, removedInodeId := dir.handleChild_DOWN(c, hrc,
			remoteRecord, childname, childId)
		if newInodeId != nil {
			uninstantiated = append(uninstantiated, *newInodeId)
		}
		if removedInodeId != nil {
			deletedInodeIds = append(deletedInodeIds, *removedInodeId)
		}
		delete(remoteEntries, childname)
	})

	// now go through all remote entries that did not exist in the
	// local workspace
	for childname, record := range remoteEntries {
		newInodeId, removedInodeId := dir.handleChild_DOWN(c, hrc, record,
			childname, quantumfs.InodeIdInvalid)
		if newInodeId != nil {
			uninstantiated = append(uninstantiated, *newInodeId)
		}
		utils.Assert(removedInodeId == nil, "inode deletion not expected")
	}

	// reload all children, whether instantiated or not,
	// based on the new base layer id
	dir.children.baseLayerIs(c, baseLayerId)
	return uninstantiated, deletedInodeIds
}
