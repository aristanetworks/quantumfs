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

	var srcParent *Directory
	newRecord, needsSync, err := func() (quantumfs.DirectoryRecord,
		bool, fuse.Status) {

		defer srcInode.getParentLock().Lock().Unlock()

		// ensure we're not orphaned
		if srcInode.isOrphaned_() {
			c.wlog("Can't hardlink an orphaned file")
			return nil, false, fuse.EPERM
		}

		srcParent = asDirectory(srcInode.parent_(c))

		// Ensure the source and dest are in the same workspace
		if srcParent.hardlinkTable != dir.hardlinkTable {
			c.dlog("Source and dest are different workspaces.")
			return nil, false, fuse.EPERM
		}

		newRecord, needsSync, err :=
			srcParent.makeHardlink_DOWN_(c, srcInode)
		if err != fuse.OK {
			c.elog("Link Failed with srcInode record")
			return nil, false, err
		}

		// We need to reparent under the srcInode lock
		dir.hardlinkTable.claimAsChild_(srcInode)

		return newRecord, needsSync, fuse.OK
	}()
	if err != fuse.OK {
		return err
	}

	newRecord.SetFilename(newName)
	// Update the reference count
	dir.hardlinkInc(newRecord.FileId())

	if needsSync {
		// In order to avoid the scenarios where the second leg's delta
		// bubbles up to the workspaceroot before the first leg's type
		// change we have to sync the first leg's former parent.
		// However, Syncing must happen after hardlinkInc to avoid
		// normalization.
		srcParent.Sync_DOWN(c)
	}

	// We cannot lock earlier because the parent of srcInode may be us
	defer dir.Lock().Unlock()

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.setRecord(c, srcInode.inodeNum(), newRecord)
	}()

	dir.self.markAccessed(c, newName,
		markType(newRecord.Type(), quantumfs.PathCreated))

	c.dlog("Hardlinked %d to %s", srcInode.inodeNum(), newName)

	inodeNum := srcInode.inodeNum()
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

	// We hold the exclusive tree lock and so cannot be racing against another
	// access. Thus it is safe to assume nothing has changed between the flush in
	// our caller and the lock grab in getRecordChildCall_() below.
	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	if record == nil {
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

func (dir *Directory) convertToHardlinkLeg_DOWN(c *ctx, childname string,
	childId InodeId) (copy quantumfs.DirectoryRecord,
	needsSync bool, err fuse.Status) {

	defer c.FuncIn("Directory::convertToHardlinkLeg_DOWN",
		"inode %d name %s", childId, childname).Out()

	child := dir.children.recordByName(c, childname)
	if child == nil {
		c.elog("No child record for name %s", childname)
		return nil, false, fuse.ENOENT
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*HardlinkLeg); isLink {
		c.vlog("Already a hardlink")
		recordCopy := *link
		return &recordCopy, false, fuse.OK
	}

	// record must be a file type to be hardlinked
	if !child.Type().IsRegularFile() &&
		child.Type() != quantumfs.ObjectTypeSymlink &&
		child.Type() != quantumfs.ObjectTypeSpecial {

		c.dlog("Cannot hardlink %s - not a file", child.Filename())
		return nil, false, fuse.EINVAL
	}

	// remove the record from the childmap before donating it to be a hardlink
	donateChild := dir.children.deleteChild(c, childname)

	c.vlog("Converting %s into a hardlink", childname)
	newLink := dir.hardlinkTable.newHardlink(c, childId, donateChild)

	linkSrcCopy := newLink.Clone()
	linkSrcCopy.SetFilename(childname)
	dir.children.setRecord(c, childId, linkSrcCopy)

	newLink.setCreationTime(quantumfs.NewTime(time.Now()))
	newLink.SetContentTime(newLink.creationTime())
	return newLink, true, fuse.OK
}

// the toLink parentLock must be locked
func (dir *Directory) makeHardlink_DOWN_(c *ctx,
	toLink Inode) (copy quantumfs.DirectoryRecord, needsSync bool,
	err fuse.Status) {

	defer c.funcIn("Directory::makeHardlink_DOWN_").Out()

	// If someone is trying to link a hardlink, we just need to return a copy
	isHardlink, id := dir.hardlinkTable.checkHardlink(toLink.inodeNum())
	if isHardlink {
		linkCopy := newHardlinkLeg(toLink.name(), id,
			quantumfs.NewTime(time.Now()), dir.hardlinkTable)
		return linkCopy, false, fuse.OK
	}

	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()

	return dir.convertToHardlinkLeg_DOWN(c, toLink.name(), toLink.inodeNum())
}

// The caller must hold the childRecordLock
func (dir *Directory) normalizeHardlinks_DOWN_(c *ctx,
	rc *RefreshContext, localRecord quantumfs.ImmutableDirectoryRecord,
	remoteRecord quantumfs.DirectoryRecord) quantumfs.DirectoryRecord {

	defer c.funcIn("Directory::normalizeHardlinks_DOWN_").Out()
	inodeId := dir.children.inodeNum(localRecord.Filename())
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
	dir.hardlinkTable.updateHardlinkInodeId(c, fileId, inodeId)
	if inode != nil {
		func() {
			defer inode.getParentLock().Lock().Unlock()
			dir.hardlinkTable.claimAsChild_(inode)
		}()
	}
	return newHardlinkLeg(localRecord.Filename(), fileId,
		remoteRecord.ContentTime(), dir.hardlinkTable)
}

// The caller must hold the childRecordLock
func (dir *Directory) loadNewChild_DOWN_(c *ctx,
	remoteRecord quantumfs.DirectoryRecord, inodeId InodeId) InodeId {

	defer c.FuncIn("Directory::loadNewChild_DOWN_", "%d : %s : %d",
		dir.inodeNum(), remoteRecord.Filename(), inodeId).Out()

	if inodeId == quantumfs.InodeIdInvalid {
		// Allocate a new inode for regular files
		inodeId = dir.children.loadChild(c, remoteRecord)
	} else {
		// An already existing inode for hardlinks to existing inodes
		utils.Assert(remoteRecord.Type() == quantumfs.ObjectTypeHardlink,
			"Child is of type %d not hardlink", remoteRecord.Type())
		hll := newHardlinkLegFromRecord(remoteRecord, dir.hardlinkTable)
		dir.children.setRecord(c, inodeId, hll)
	}
	c.qfs.noteChildCreated(c, dir.id, remoteRecord.Filename())
	return inodeId
}

// This function is a simplified alternative to Rename/Move which is called
// while refreshing. This function is required as the regular Rename/Move
// function have to update the hardlink table and normalize the source and
// destination of the operation
func (dir *Directory) moveHardlinkLeg_DOWN(c *ctx, newParent Inode, oldName string,
	remoteRecord quantumfs.DirectoryRecord, inodeId InodeId) {

	defer c.FuncIn("Directory::moveHardlinkLeg_DOWN", "%d : %s : %d",
		dir.inodeNum(), remoteRecord.Filename(), inodeId).Out()

	// Unlike regular rename, we throw away the result of deleteChild and
	// just use the new remote record for creating the move destination
	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.deleteChild(c, oldName)
	}()

	dst := asDirectory(newParent)
	defer dst.childRecordLock.Lock().Unlock()
	dst.children.setRecord(c, inodeId, remoteRecord)
}

// The caller must hold the childRecordLock
func (dir *Directory) refreshChild_DOWN_(c *ctx, rc *RefreshContext,
	localRecord quantumfs.ImmutableDirectoryRecord, childId InodeId,
	remoteRecord quantumfs.DirectoryRecord) {

	childname := remoteRecord.Filename()
	defer c.FuncIn("Directory::refreshChild_DOWN_", "%s", childname).Out()

	if remoteRecord.ID().IsEqualTo(localRecord.ID()) {
		c.wlog("No changes to record %s", remoteRecord.Filename())
		if localRecord.Type() != quantumfs.ObjectTypeHardlink {
			dir.children.setRecord(c, childId, remoteRecord)
			dir.children.makePublishable(c, remoteRecord.Filename())
		}
		return
	}

	c.wlog("entry %s goes %d:%s -> %d:%s", remoteRecord.Filename(),
		localRecord.Type(), localRecord.ID().String(),
		remoteRecord.Type(), remoteRecord.ID().String())

	utils.Assert(underlyingTypesMatch(dir.hardlinkTable, localRecord,
		remoteRecord), "type mismatch %d vs. %d",
		underlyingTypeOf(dir.hardlinkTable, localRecord),
		underlyingTypeOf(dir.hardlinkTable, remoteRecord))

	if !localRecord.Type().Matches(remoteRecord.Type()) {
		remoteRecord = dir.normalizeHardlinks_DOWN_(c, rc, localRecord,
			remoteRecord)
	}
	dir.children.setRecord(c, childId, remoteRecord)
	dir.children.makePublishable(c, remoteRecord.Filename())
	if inode := c.qfs.inodeNoInstantiate(c, childId); inode != nil {
		reload(c, dir.hardlinkTable, rc, inode, remoteRecord)
	}
	c.qfs.invalidateInode(c, childId)
}

func updateMapDescend_DOWN(c *ctx, rc *RefreshContext,
	inodeId InodeId, remoteRecord quantumfs.ImmutableDirectoryRecord) {

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
func (dir *Directory) hideEntry_DOWN_(c *ctx,
	localRecord quantumfs.ImmutableDirectoryRecord,
	childId InodeId) (newLocal quantumfs.ImmutableDirectoryRecord) {

	defer c.funcIn("Directory::hideEntry_DOWN_").Out()

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
	c.qfs.noteDeletedInode(c, dir.inodeNum(), childId, oldName)
	rtn := dir.children.recordByName(c, hiddenName)
	utils.Assert(rtn != nil, "Rename failed during hideEntry")
	return rtn
}

func (dir *Directory) updateRefreshMap_DOWN(c *ctx, rc *RefreshContext,
	baseLayerId *quantumfs.ObjectKey) {

	defer c.funcIn("Directory::updateRefreshMap_DOWN").Out()

	defer dir.childRecordLock.Lock().Unlock()

	remoteEntries := make(map[string]quantumfs.DirectoryRecord, 0)
	if baseLayerId != nil {
		foreachImmutableDentry(c, *baseLayerId,
			func(record quantumfs.ImmutableDirectoryRecord) {

				remoteEntries[record.Filename()] = record.Clone()
			})
	}

	dir.children.foreachChild(c, func(childname string, childId InodeId) {
		localRecord := dir.children.recordByName(c, childname)
		remoteRecord := remoteEntries[childname]

		c.vlog("Processing %s local %t remote %t", childname,
			localRecord != nil, remoteRecord != nil)

		if rc.isLocalRecordUsable(c, localRecord, remoteRecord) {
			if shouldHideLocalRecord(localRecord, remoteRecord) {
				localRecord = dir.hideEntry_DOWN_(c, localRecord,
					childId)
			}
			moved := remoteRecord == nil ||
				remoteRecord.FileId() != localRecord.FileId()
			fileId := rc.attachLocalRecord(c, dir.inodeNum(), childId,
				moved, localRecord, remoteRecord)
			if fileId != localRecord.FileId() {
				// Don't be wasteful, only modify if a change
				// occurred
				dir.children.modifyChildWithFunc(c, childId,
					func(record quantumfs.DirectoryRecord) {

						record.SetFileId(fileId)
					})
				dir.children.makePublishable(c,
					localRecord.Filename())
			}
		} else {
			rc.addStaleEntry(c, dir.inodeNum(), childId, localRecord)
		}

		// Ensure we ignore any subdirectories that haven't changed
		if localRecord.Type() == quantumfs.ObjectTypeDirectory &&
			!skipDir(localRecord, remoteRecord) {

			updateMapDescend_DOWN(c, rc, childId, remoteRecord)
		}
	})
}

// The caller must hold the childRecordLock
func (dir *Directory) findLocalMatch_DOWN_(c *ctx, rc *RefreshContext,
	record quantumfs.DirectoryRecord, localEntries map[string]InodeId) (
	localRecord quantumfs.ImmutableDirectoryRecord, inodeId InodeId,
	missingDentry bool) {

	localRecord = dir.children.recordByName(c, record.Filename())
	if localRecord != nil && localRecord.FileId() == record.FileId() {
		// perfect match, the file has not moved
		return localRecord, localEntries[record.Filename()], false
	}
	matchingLoadRecord, exists := rc.fileMap[record.FileId()]
	utils.Assert(exists, "Missing filemap record for %s", record.Filename())

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
	foreachImmutableDentry(c, baseLayerId,
		func(immrecord quantumfs.ImmutableDirectoryRecord) {

			record := immrecord.Clone()
			localRecord, inodeId, missingDentry :=
				dir.findLocalMatch_DOWN_(c, rc, record, localEntries)
			if localRecord == nil {
				uninstantiated = append(uninstantiated,
					dir.loadNewChild_DOWN_(c, record, inodeId))
				return
			}
			if missingDentry {
				if record.Type() != quantumfs.ObjectTypeHardlink {
					// Will be handled in the moveDentries stage
					return
				}
				if !rc.setHardlinkAsMoveDst(c, localRecord, record) {
					dir.loadNewChild_DOWN_(c, record, inodeId)
				}
				return
			}
			dir.refreshChild_DOWN_(c, rc, localRecord, inodeId, record)
		})
	c.qfs.addUninstantiated(c, uninstantiated, dir.inodeNum())
}
