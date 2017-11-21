// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import (
	"fmt"
	"time"

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

		srcParent := asDirectory(srcInode.parent_(c))

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
		return dir.children.loadPublishableChild(c, newRecord,
			quantumfs.InodeIdInvalid)
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

		linkCopy := newHardlink(toLink.name(), id,
			quantumfs.NewTime(time.Now()), dir.wsr)
		return linkCopy, fuse.OK
	}

	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()

	return dir.children.makeHardlink(c, toLink.inodeNum())
}

// The caller must hold the childRecordLock
func (dir *Directory) normalizeHardlinks_DOWN_(c *ctx,
	rc *RefreshContext, localRecord quantumfs.DirectoryRecord,
	remoteRecord *quantumfs.DirectRecord) quantumfs.DirectoryRecord {

	defer c.funcIn("Directory::normalizeHardlinks_DOWN_").Out()
	inodeId := dir.children.inodeNum(remoteRecord.Filename())
	inode := c.qfs.inodeNoInstantiate(c, inodeId)

	if localRecord.Type() == quantumfs.ObjectTypeHardlink {
		if inode != nil {
			inode.setParent(dir.inodeNum())
		}
		return remoteRecord
	}
	utils.Assert(remoteRecord.Type() == quantumfs.ObjectTypeHardlink,
		"either local or remote should be hardlinks to be normalized")

	fileId := remoteRecord.FileId()
	dir.wsr.updateHardlinkInodeId(c, fileId, inodeId)
	if inode != nil {
		inode.setParent(dir.wsr.inodeNum())
	}
	return newHardlink(localRecord.Filename(), fileId,
		remoteRecord.ContentTime(), dir.wsr)
}

// The caller must hold the childRecordLock
func (dir *Directory) loadNewChild_DOWN_(c *ctx,
	remoteRecord *quantumfs.DirectRecord, inodeId InodeId) InodeId {

	defer c.FuncIn("Directory::loadNewChild_DOWN_", "%d : %s : %d",
		dir.inodeNum(), remoteRecord.Filename(), inodeId).Out()

	// Allocate a new inode for regular files or return an already
	// existing inode for hardlinks to existing inodes
	inodeId = dir.children.loadPublishableChild(c, remoteRecord, inodeId)
	status := c.qfs.noteChildCreated(dir.id, remoteRecord.Filename())
	utils.Assert(status == fuse.OK,
		"marking %s created failed with %d", remoteRecord.Filename(),
		status)
	return inodeId
}

// The caller must hold the childRecordLock
func (dir *Directory) refreshChild_DOWN_(c *ctx, rc *RefreshContext,
	localRecord quantumfs.DirectoryRecord, childId InodeId,
	remoteRecord *quantumfs.DirectRecord) {

	childname := remoteRecord.Filename()
	defer c.FuncIn("Directory::refreshChild_DOWN_", "%s", childname).Out()

	if remoteRecord.ID().IsEqualTo(localRecord.ID()) {
		c.wlog("No changes to record %s", remoteRecord.Filename())
		if localRecord.Type() != quantumfs.ObjectTypeHardlink {
			dir.children.setRecord(c, childId, remoteRecord)
		}
		return
	}

	c.wlog("entry %s goes %d:%s -> %d:%s", remoteRecord.Filename(),
		localRecord.Type(), localRecord.ID().String(),
		remoteRecord.Type(), remoteRecord.ID().String())

	utils.Assert(underlyingTypesMatch(dir.wsr, localRecord, remoteRecord),
		"type mismatch %d vs. %d", underlyingTypeOf(dir.wsr, localRecord),
		underlyingTypeOf(dir.wsr, remoteRecord))

	record := quantumfs.DirectoryRecord(remoteRecord)
	if !localRecord.Type().Matches(remoteRecord.Type()) {
		record = dir.normalizeHardlinks_DOWN_(c, rc, localRecord,
			remoteRecord)
	}
	dir.children.setRecord(c, childId, record)
	if inode := c.qfs.inodeNoInstantiate(c, childId); inode != nil {
		reload(c, dir.wsr, rc, inode, record)
	}
	status := c.qfs.invalidateInode(childId)
	utils.Assert(status == fuse.OK,
		"invalidating %d failed with %d", childId, status)
}

func updateMapDescend_DOWN(c *ctx, rc *RefreshContext,
	inodeId InodeId, remoteRecord quantumfs.DirectoryRecord) {

	defer c.funcIn("updateMapDescend_DOWN").Out()
	if inode := c.qfs.inodeNoInstantiate(c, inodeId); inode != nil {
		subdir := inode.(*Directory)
		var id *quantumfs.ObjectKey
		if remoteRecord != nil && remoteRecord.Type() ==
			quantumfs.ObjectTypeDirectory {

			id_ := remoteRecord.ID()
			id = &id_
		} else {
			id = nil
		}
		subdir.updateRefreshMap_DOWN(c, rc, id)
	}
}

// The caller must hold the childRecordLock
func (dir *Directory) hideEntry_DOWN_(c *ctx, childId InodeId,
	localRecord quantumfs.DirectoryRecord) {

	oldName := localRecord.Filename()
	hiddenName := fmt.Sprintf(".hidden.%d", localRecord.FileId())
	for {
		if dir.children.recordByName(c, hiddenName) == nil {
			break
		}
		c.wlog("Child %s already exists", hiddenName)
		hiddenName = fmt.Sprintf(".hidden.%d",
			utils.RandomNumberGenerator.Uint64())
	}
	dir.children.renameChild(c, oldName, hiddenName)
	c.qfs.noteDeletedInode(dir.inodeNum(), childId, oldName)
}

func (dir *Directory) updateRefreshMap_DOWN(c *ctx, rc *RefreshContext,
	baseLayerId *quantumfs.ObjectKey) {

	defer c.funcIn("Directory::updateRefreshMap_DOWN").Out()

	defer dir.childRecordLock.Lock().Unlock()

	remoteEntries := make(map[string]quantumfs.DirectoryRecord, 0)
	if baseLayerId != nil {
		foreachDentry(c, *baseLayerId, func(record *quantumfs.DirectRecord) {
			remoteEntries[record.Filename()] = record
		})
	}

	dir.children.foreachChild(c, func(childname string, childId InodeId) {
		localRecord := dir.children.recordByName(c, childname)
		remoteRecord := remoteEntries[childname]

		if rc.isInodeUsedAfterRefresh(c, localRecord, remoteRecord) {
			if shouldHideLocalRecord(localRecord, remoteRecord) {
				dir.hideEntry_DOWN_(c, childId, localRecord)
			}
			moved := remoteRecord == nil ||
				remoteRecord.FileId() != localRecord.FileId()
			rc.attachLocalRecord(c, dir.inodeNum(), childId, moved,
				localRecord, remoteRecord)
		} else {
			rc.addStaleEntry(c, dir.inodeNum(), childId, localRecord)
		}
		if localRecord.Type() == quantumfs.ObjectTypeDirectory {
			updateMapDescend_DOWN(c, rc, childId, remoteRecord)
		}
	})
}

// The caller must hold the childRecordLock
func (dir *Directory) findLocalMatch_DOWN_(c *ctx, rc *RefreshContext,
	record *quantumfs.DirectRecord, localEntries map[string]InodeId) (
	localRecord quantumfs.DirectoryRecord, inodeId InodeId,
	missingDentry bool) {

	localRecord = dir.children.recordByName(c, record.Filename())
	if localRecord != nil && localRecord.FileId() == record.FileId() {
		// perfect match, the file has not moved
		return localRecord, localEntries[record.Filename()], false
	}
	matchingLoadRecord := rc.fileMap[record.FileId()]
	return matchingLoadRecord.localRecord, matchingLoadRecord.inodeId, true
}

func (dir *Directory) refresh_DOWN(c *ctx, rc *RefreshContext,
	baseLayerId quantumfs.ObjectKey) {

	defer c.funcIn("Directory::refresh_DOWN").Out()
	uninstantiated := make([]InodeId, 0)

	localEntries := make(map[string]InodeId, 0)
	defer dir.childRecordLock.Lock().Unlock()
	dir.children.foreachChild(c, func(childname string, childId InodeId) {
		localEntries[childname] = childId
	})
	foreachDentry(c, baseLayerId, func(record *quantumfs.DirectRecord) {
		localRecord, inodeId, missingDentry :=
			dir.findLocalMatch_DOWN_(c, rc, record, localEntries)
		if localRecord == nil {
			uninstantiated = append(uninstantiated,
				dir.loadNewChild_DOWN_(c, record, inodeId))
			return
		}
		if missingDentry {
			if record.Type() == quantumfs.ObjectTypeHardlink {
				dir.loadNewChild_DOWN_(c, record, inodeId)
			}
			return
		}
		dir.refreshChild_DOWN_(c, rc, localRecord, inodeId, record)
	})
	c.qfs.addUninstantiated(c, uninstantiated, dir.inodeNum())
}
