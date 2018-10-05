// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import (
	"fmt"
	"syscall"
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
	newRecord, inodeInfo, needsSync, err := func() (quantumfs.DirectoryRecord,
		InodeIdInfo, bool, fuse.Status) {

		defer srcInode.getParentLock().Lock().Unlock()

		// ensure we're not orphaned
		if srcInode.isOrphaned_() {
			c.wlog("Can't hardlink an orphaned file")
			return nil, invalidIdInfo(), false, fuse.EPERM
		}

		srcParent_, release := srcInode.parent_(c)
		defer release()
		srcParent = asDirectory(srcParent_)

		// Ensure the source and dest are in the same workspace
		if srcParent.hardlinkTable != dir.hardlinkTable {
			c.dlog("Source and dest are different workspaces.")
			return nil, invalidIdInfo(), false, fuse.EPERM
		}

		newRecord, needsSync, inodeInfo, err :=
			srcParent.makeHardlink_DOWN_(c, srcInode)
		if err != fuse.OK {
			c.elog("Link Failed with srcInode record")
			return nil, invalidIdInfo(), false, err
		}

		// We need to reparent under the srcInode lock
		dir.hardlinkTable.claimAsChild_(c, srcInode)

		return newRecord, inodeInfo, needsSync, fuse.OK
	}()
	if err != fuse.OK {
		return err
	}

	newRecord.SetFilename(newName)
	// Update the reference count
	func() {
		defer dir.Lock().Unlock()
		dir.hardlinkInc_(newRecord.FileId())
	}()

	if needsSync {
		// In order to avoid the scenarios where the second leg's delta
		// bubbles up to the workspaceroot before the first leg's type
		// change we have to sync the first leg's former parent.
		// However, Syncing must happen after hardlinkInc to avoid
		// normalization.
		srcParent.Sync_DOWN(c)
	}

	doUnlocked := func() {}
	func() {
		// We cannot lock earlier because the parent of srcInode may be us
		defer dir.Lock().Unlock()

		func() {
			defer dir.childRecordLock.Lock().Unlock()
			doUnlocked = dir.children.setRecord(c, inodeInfo, newRecord)
		}()

		c.vlog("Hardlinked %d to %s", srcInode.inodeNum(), newName)

		out.NodeId = uint64(inodeInfo.id)
		out.Generation = inodeInfo.generation
		c.qfs.incrementLookupCount(c, inodeInfo.id)
		fillEntryOutCacheData(c, out)
		fillAttrWithDirectoryRecord(c, &out.Attr, inodeInfo.id,
			c.fuseCtx.Owner, newRecord)

		dir.self.dirty(c)
		// Hardlinks aren't tracked by the uninstantiated list, they need a
		// more complicated ref counting system handled by workspaceroot
	}()
	doUnlocked()

	dir.self.markAccessed(c, newName,
		markType(newRecord.Type(), quantumfs.PathCreated))

	return fuse.OK
}

func (dir *Directory) Sync_DOWN(c *ctx) fuse.Status {
	defer c.FuncIn("Directory::Sync_DOWN", "dir %d", dir.inodeNum()).Out()

	children := make([]InodeId, 0)
	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.foreachDirectInode(c, func(child InodeId) bool {
			children = append(children, child)

			return true
		})
	}()

	for _, child := range children {
		func() {
			inode, release := c.qfs.inodeNoInstantiate(c, child)
			defer release()
			if inode != nil {
				inode.Sync_DOWN(c)
			}
		}()
	}

	dir.flush(c)

	return fuse.OK
}

// The returned cleanup function of terminal directory should be called at the end of
// the caller
func (dir *Directory) followPath_DOWN(c *ctx, path []string) (terminalDir Inode,
	cleanup func(), err error) {

	defer c.funcIn("Directory::followPath_DOWN").Out()
	// Traverse through the workspace, reach the target inode
	length := len(path) - 1 // leave the target node at the end
	currDir := dir
	// Go along the given path to the destination. The path is stored in a string
	// slice, each cell index contains an inode.
	// Skip the first three Inodes: typespace / namespace / workspace
	for num := 3; num < length; num++ {
		// all preceding nodes have to be directories
		child, err := currDir.lookupInternal(c, path[num],
			quantumfs.ObjectTypeDirectory)
		if err != nil {
			return child, func() {}, err
		}
		if num < length-1 {
			defer c.qfs.Forget(uint64(child.inodeNum()), 1)
		}

		currDir = child.(*Directory)
	}

	cleanup = func() {
		if length > 3 {
			c.qfs.Forget(uint64(currDir.inodeNum()), 1)
		}
	}
	return currDir, cleanup, nil
}

func (dir *Directory) convertToHardlinkLeg_DOWN(c *ctx,
	childname string) (copy quantumfs.DirectoryRecord, needsSync bool,
	inodeIdInfo InodeIdInfo, err fuse.Status, doUnlocked func()) {

	defer c.FuncIn("Directory::convertToHardlinkLeg_DOWN",
		"name %s", childname).Out()

	doUnlocked = func() {}
	childId := dir.children.inodeNum(childname)

	c.vlog("Converting inode %d to hardlink", childId.id)

	child := dir.children.recordByName(c, childname)
	if child == nil {
		c.elog("No child record for name %s", childname)
		return nil, false, invalidIdInfo(), fuse.ENOENT, doUnlocked
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*HardlinkLeg); isLink {
		c.vlog("Already a hardlink")
		recordCopy := *link
		return &recordCopy, false, invalidIdInfo(), fuse.OK, doUnlocked
	}

	// record must be a file type to be hardlinked
	if !child.Type().IsRegularFile() &&
		child.Type() != quantumfs.ObjectTypeSymlink &&
		child.Type() != quantumfs.ObjectTypeSpecial {

		c.vlog("Cannot hardlink %s - not a file", child.Filename())
		return nil, false, invalidIdInfo(), fuse.EINVAL, doUnlocked
	}

	// remove the record from the childmap before donating it to be a hardlink
	donateChild := dir.children.deleteChild(c, childname)

	c.vlog("Converting %s into a hardlink", childname)
	newLink := dir.hardlinkTable.newHardlink(c, childId, donateChild)

	linkSrcCopy := newLink.Clone()
	linkSrcCopy.SetFilename(childname)
	doUnlocked = dir.children.setRecord(c, childId, linkSrcCopy)

	newLink.setCreationTime(quantumfs.NewTime(time.Now()))
	return newLink, true, childId, fuse.OK, doUnlocked
}

// the toLink parentLock must be locked
func (dir *Directory) makeHardlink_DOWN_(c *ctx,
	toLink Inode) (copy quantumfs.DirectoryRecord, needsSync bool,
	inodeIdInfo InodeIdInfo, err fuse.Status) {

	defer c.funcIn("Directory::makeHardlink_DOWN_").Out()

	// If someone is trying to link a hardlink, we just need to return a copy
	isHardlink, id := dir.hardlinkTable.checkHardlink(toLink.inodeNum())
	if isHardlink {
		linkCopy := newHardlinkLeg(toLink.name(), id,
			quantumfs.NewTime(time.Now()), dir.hardlinkTable)
		return linkCopy, false, dir.hardlinkTable.findHardlinkInodeId(c, id),
			fuse.OK
	}

	doUnlocked := func() {}
	func() {
		defer dir.Lock().Unlock()
		defer dir.childRecordLock.Lock().Unlock()

		copy, needsSync, inodeIdInfo, err,
			doUnlocked = dir.convertToHardlinkLeg_DOWN(c, toLink.name())
	}()
	doUnlocked()

	return copy, needsSync, inodeIdInfo, err
}

// The caller must hold the childRecordLock
// Normalize an inode that's a local hardlink, but remote regular file.
// Link-ify an inode that's a local regular file, but remote hardlink.
func (dir *Directory) convertHardlinks_DOWN_(c *ctx,
	rc *RefreshContext, localRecord quantumfs.ImmutableDirectoryRecord,
	remoteRecord quantumfs.DirectoryRecord) quantumfs.DirectoryRecord {

	defer c.funcIn("Directory::convertHardlinks_DOWN_").Out()
	inodeId := dir.children.inodeNum(localRecord.Filename())

	if localRecord.Type() == quantumfs.ObjectTypeHardlink {
		func() {
			inode, release := c.qfs.inodeNoInstantiate(c, inodeId.id)
			defer release()

			if inode != nil {
				inode.setParent(c, dir)
			}
		}()
		return remoteRecord
	}

	// Convert a regular file into a hardlink
	utils.Assert(remoteRecord.Type() == quantumfs.ObjectTypeHardlink,
		"either local or remote should be hardlinks to be converted")

	fileId := remoteRecord.FileId()
	dir.hardlinkTable.updateHardlinkInodeId(c, fileId, inodeId)

	inode, release := c.qfs.inodeNoInstantiate(c, inodeId.id)
	defer release()
	if inode != nil {
		func() {
			defer inode.getParentLock().Lock().Unlock()
			dir.hardlinkTable.claimAsChild_(c, inode)
		}()
	}
	return newHardlinkLeg(localRecord.Filename(), fileId,
		remoteRecord.ContentTime(), dir.hardlinkTable)
}

// The caller must hold the childRecordLock
func (dir *Directory) loadNewChild_DOWN_(c *ctx,
	remoteRecord quantumfs.DirectoryRecord, inodeId InodeIdInfo) InodeId {

	defer c.FuncIn("Directory::loadNewChild_DOWN_", "%d : %s : %d",
		dir.inodeNum(), remoteRecord.Filename(), inodeId.id).Out()

	rtn := inodeId.id
	if inodeId.id == quantumfs.InodeIdInvalid {
		// Allocate a new inode for regular files
		rtn = dir.children.loadChild(c, remoteRecord)
	} else {
		// An already existing inode for hardlinks to existing inodes
		utils.Assert(remoteRecord.Type() == quantumfs.ObjectTypeHardlink,
			"Child is of type %d not hardlink", remoteRecord.Type())
		hll := newHardlinkLegFromRecord(remoteRecord, dir.hardlinkTable)
		dir.children.setRecord(c, inodeId, hll)
	}
	c.qfs.noteChildCreated(c, dir.id, remoteRecord.Filename())
	return rtn
}

// This function is a simplified alternative to Rename/Move which is called
// while refreshing. This function is required as the regular Rename/Move
// function have to update the hardlink table and normalize the source and
// destination of the operation
func (dir *Directory) moveHardlinkLeg_DOWN(c *ctx, newParent Inode, oldName string,
	remoteRecord quantumfs.DirectoryRecord, inodeId InodeIdInfo) {

	defer c.FuncIn("Directory::moveHardlinkLeg_DOWN", "%d : %s : %d",
		dir.inodeNum(), remoteRecord.Filename(), inodeId.id).Out()

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
	localRecord quantumfs.ImmutableDirectoryRecord, childId InodeIdInfo,
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
		remoteRecord = dir.convertHardlinks_DOWN_(c, rc, localRecord,
			remoteRecord)
	}
	dir.children.setRecord(c, childId, remoteRecord)
	dir.children.makePublishable(c, remoteRecord.Filename())

	func() {
		inode, release := c.qfs.inodeNoInstantiate(c, childId.id)
		defer release()
		if inode != nil {
			reload(c, dir.hardlinkTable, rc, inode, remoteRecord)
		}
	}()

	c.qfs.invalidateInode(c, childId.id)
}

func updateMapDescend_DOWN(c *ctx, rc *RefreshContext,
	inodeId InodeId, remoteRecord quantumfs.ImmutableDirectoryRecord) {

	defer c.funcIn("updateMapDescend_DOWN").Out()
	inode, release := c.qfs.inodeNoInstantiate(c, inodeId)
	defer release()

	if inode != nil {
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
	dir.children.makePublishable(c, hiddenName)
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
		foreachDentry(c, *baseLayerId,
			func(record quantumfs.ImmutableDirectoryRecord) {

				remoteEntries[record.Filename()] = record.Clone()
			})
	}

	dir.children.foreachChild(c, func(childname string, childId InodeIdInfo) {
		localRecord := dir.children.recordByName(c, childname)
		remoteRecord := remoteEntries[childname]

		c.vlog("Processing %s local %t remote %t", childname,
			localRecord != nil, remoteRecord != nil)

		if rc.isLocalRecordUsable(c, localRecord, remoteRecord) {
			if shouldHideLocalRecord(localRecord, remoteRecord) {
				localRecord = dir.hideEntry_DOWN_(c, localRecord,
					childId.id)
			}
			moved := remoteRecord == nil ||
				remoteRecord.FileId() != localRecord.FileId()
			fileId := rc.attachLocalRecord(c, dir.inodeNum(), childId,
				moved, localRecord, remoteRecord)
			if fileId != localRecord.FileId() {
				// Don't be wasteful, only modify if a change
				// occurred
				dir.children.modifyChildWithFunc(c, childId.id,
					func(record quantumfs.DirectoryRecord) {

						record.SetFileId(fileId)
					})
				dir.children.makePublishable(c,
					localRecord.Filename())
			}
		} else {
			rc.addStaleEntry(c, dir.inodeNum(), childId.id, localRecord)
		}

		// Ensure we ignore any subdirectories that haven't changed
		if localRecord.Type() == quantumfs.ObjectTypeDirectory &&
			!skipDir(localRecord, remoteRecord) {

			updateMapDescend_DOWN(c, rc, childId.id, remoteRecord)
		}
	})
}

// The caller must hold the childRecordLock
func (dir *Directory) findLocalMatch_DOWN_(c *ctx, rc *RefreshContext,
	record quantumfs.DirectoryRecord, localEntries map[string]InodeIdInfo) (
	localRecord quantumfs.ImmutableDirectoryRecord, inodeId InodeIdInfo,
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
	uninstantiated := make([]inodePair, 0)

	localEntries := make(map[string]InodeIdInfo, 0)
	defer dir.childRecordLock.Lock().Unlock()
	dir.children.foreachChild(c, func(childname string, childId InodeIdInfo) {
		localEntries[childname] = childId
	})
	foreachDentry(c, baseLayerId, func(
		immrecord quantumfs.ImmutableDirectoryRecord) {

		record := immrecord.Clone()
		localRecord, inodeId, missingDentry :=
			dir.findLocalMatch_DOWN_(c, rc, record, localEntries)
		if localRecord == nil {
			parent := dir.inodeNum()
			if record.Type() == quantumfs.ObjectTypeHardlink {
				parent = dir.hardlinkTable.getWorkspaceRoot().id
			}

			childId := dir.loadNewChild_DOWN_(c, record, inodeId)

			uninstantiated = append(uninstantiated,
				newInodePair(childId, parent))
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
	c.qfs.addUninstantiated(c, uninstantiated)
}

func (dir *Directory) MvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) (result fuse.Status) {

	defer c.FuncIn("Directory::MvChild", "%s -> %s", oldName, newName).Out()

	fileType, overwritten, result := dir.mvChild_DOWN(c, dstInode, oldName,
		newName)
	if result == fuse.OK {
		dst := asDirectory(dstInode)
		if overwritten != nil {
			dst.self.markAccessed(c, overwritten.Filename(),
				markType(overwritten.Type(),
					quantumfs.PathDeleted))
		}

		// This is the same entry just moved, so we can use the same
		// record for both the old and new paths.
		dir.self.markAccessed(c, oldName,
			markType(fileType, quantumfs.PathDeleted))
		dst.self.markAccessed(c, newName,
			markType(fileType, quantumfs.PathCreated))
	}

	return result
}

func (dir *Directory) mvChild_DOWN(c *ctx, dstInode Inode, oldName string,
	newName string) (fileType quantumfs.ObjectType,
	overwritten quantumfs.ImmutableDirectoryRecord, result fuse.Status) {

	// check write permission for both directories

	result = func() fuse.Status {
		defer dstInode.getParentLock().RLock().RUnlock()
		return hasDirectoryWritePerm_(c, dstInode)
	}()
	if result != fuse.OK {
		return
	}

	result = func() fuse.Status {
		defer dir.parentLock.RLock().RUnlock()
		defer dir.childRecordLock.Lock().Unlock()

		record := dir.children.recordByName(c, oldName)
		return hasDirectoryWritePermSticky_(c, dir, record.Owner())
	}()
	if result != fuse.OK {
		return
	}

	dst := asDirectory(dstInode)

	defer func() {
		dir.updateSize(c, result)
		dst.updateSize(c, result)
	}()
	childInodeId := dir.childInodeNum(oldName)
	childInode, release := c.qfs.inode(c, childInodeId.id)
	defer release()

	overwrittenInodeId := dst.childInodeNum(newName).id
	overwrittenInode, release := c.qfs.inode(c, overwrittenInodeId)
	defer release()

	c.vlog("Aquiring locks")
	if childInode != nil && overwrittenInode != nil {
		firstChild, lastChild := getLockOrder(childInode, overwrittenInode)
		defer firstChild.getParentLock().Lock().Unlock()
		defer lastChild.getParentLock().Lock().Unlock()
	} else if childInode != nil {
		defer childInode.getParentLock().Lock().Unlock()
	} else if overwrittenInode != nil {
		defer overwrittenInode.getParentLock().Lock().Unlock()
	}

	// The locking here is subtle.
	//
	// Firstly we must protect against the case where a concurrent rename
	// in the opposite direction (from dst into dir) is occurring as we
	// are renaming a file from dir into dst. If we lock naively we'll
	// end up with a lock ordering inversion and deadlock in this case.
	//
	// We prevent this by locking dir and dst in a consistent ordering
	// based upon their inode number. All multi-inode locking must call
	// getLockOrder() to facilitate this.
	firstLock, lastLock := getLockOrder(dst, dir)
	defer firstLock.Lock().Unlock()
	defer lastLock.Lock().Unlock()

	result = func() fuse.Status {
		c.vlog("Checking if destination is an empty directory")
		defer dst.childRecordLock.Lock().Unlock()

		dstRecord := dst.children.recordByName(c, newName)
		if dstRecord != nil &&
			dstRecord.Type() == quantumfs.ObjectTypeDirectory &&
			dstRecord.Size() != 0 {

			// We can not overwrite a non-empty directory
			return fuse.Status(syscall.ENOTEMPTY)
		}
		return fuse.OK
	}()
	if result != fuse.OK {
		return
	}

	// Remove from source. This is atomic because both the source and destination
	// directories are locked.
	c.vlog("Removing source")
	newEntry := func() quantumfs.DirectoryRecord {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.deleteChild(c, oldName)
	}()
	if newEntry == nil {
		c.vlog("No source!")
		return
	}

	// fix the name on the copy
	newEntry.SetFilename(newName)

	hardlink, isHardlink := newEntry.(*HardlinkLeg)
	if !isHardlink {
		newEntry.SetContentTime(quantumfs.NewTime(time.Now()))
		// Update the inode to point to the new name and
		// mark as accessed in both parents.
		if childInode != nil {
			c.vlog("Updating name and parent")
			utils.Assert(dst.inodeNum() != childInode.inodeNum(),
				"Cannot orphan child by renaming %s %d",
				newName, dst.inodeNum())
			childInode.setParent_(c, dst)
			childInode.setName(newName)
			childInode.clearAccessedCache()
		}
	} else {
		c.vlog("Updating hardlink creation time")
		hardlink.setCreationTime(quantumfs.NewTime(time.Now()))
		dst.hardlinkTable.modifyChildWithFunc(c, childInodeId.id,
			func(record quantumfs.DirectoryRecord) {

				record.SetContentTime(hardlink.creationTime())
			})
	}

	// Add to destination, possibly removing the overwritten inode
	c.vlog("Adding to destination directory")
	func() {
		defer dst.childRecordLock.Lock().Unlock()
		overwritten = dst.orphanChild_(c, newName, overwrittenInode)
		dst.children.setRecord(c, childInodeId, newEntry)

		// Being inserted means you need to be synced to be publishable. If
		// the inode is instantiated mark it dirty, otherwise mark it
		// publishable immediately.
		if childInode != nil {
			childInode.dirty(c)
		} else {
			if isHardlink {
				dst.hardlinkTable.makePublishable(c,
					newEntry.FileId())
			} else {
				dst.children.makePublishable(c, newName)
			}
		}
		dst.self.dirty(c)
	}()

	fileType = newEntry.Type()

	// Set entry in new directory. If the renamed inode is
	// uninstantiated, we swizzle the parent here. If it's a hardlink, it's
	// already matched to the workspaceroot so don't corrupt that
	if childInode == nil && !isHardlink {
		c.qfs.addUninstantiated(c, []inodePair{
			newInodePair(childInodeId.id, dir.inodeNum())})
	}

	result = fuse.OK
	return
}
