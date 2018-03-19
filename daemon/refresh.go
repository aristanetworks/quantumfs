// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"strings"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

type FileLoadRecord struct {
	remoteRecord  quantumfs.DirectoryRecord
	localRecord   quantumfs.ImmutableDirectoryRecord
	inodeId       InodeId
	parentId      InodeId
	newParentPath string
	moved         bool
	moveHasDst    bool
}

type FileRemoveRecord struct {
	parentId InodeId
	inodeId  InodeId
	name     string
	toOrphan quantumfs.DirectoryRecord
	type_    quantumfs.ObjectType
}

type RefreshContext struct {
	// fileMap is the authoritative source of truth for refreshing
	// all file types except directories and hardlinks. There is an
	// entry in the fileMap for each record in the remote workspace
	// and the information about the corresponding local entry
	// (if found) is also added to the same entry
	// The fileId of directories is ignored as they cannot be
	// maintained when two directories get merged.
	// The hardlinks are processed using the hardlink table as well
	// as this map.
	fileMap map[quantumfs.FileId]*FileLoadRecord

	// StaleRecords are records that existed in the local workspace
	// but are not present in the remote workspace.
	// This list will be populated when building the fileMap
	staleRecords []FileRemoveRecord
	rootId       quantumfs.ObjectKey
}

// should be called under the treelock
func newRefreshContext_(c *ctx, localRootId quantumfs.ObjectKey,
	remoteRootId quantumfs.ObjectKey) *RefreshContext {

	rc := RefreshContext{
		fileMap:      make(map[quantumfs.FileId]*FileLoadRecord, 0),
		staleRecords: make([]FileRemoveRecord, 0),
		rootId:       remoteRootId,
	}

	rc.buildRefreshMapWsr(c, localRootId, remoteRootId)
	return &rc
}

func (rc *RefreshContext) addStaleEntry(c *ctx, parentId InodeId, inodeId InodeId,
	localRecord quantumfs.ImmutableDirectoryRecord) {

	defer c.FuncIn("RefreshContext::addStaleEntry", "name %s inode %d",
		localRecord.Filename(), inodeId).Out()
	staleRecord := FileRemoveRecord{
		parentId: parentId,
		inodeId:  inodeId,
		name:     localRecord.Filename(),
		type_:    localRecord.Type(),
	}
	rc.staleRecords = append(rc.staleRecords, staleRecord)
}

func (rc *RefreshContext) attachLocalRecord(c *ctx, parentId InodeId,
	inodeId InodeId, moved bool, localRecord quantumfs.ImmutableDirectoryRecord,
	remoteRecord quantumfs.DirectoryRecord) quantumfs.FileId {

	defer c.FuncIn("RefreshContext::attachLocalRecord", "name %s inode %d",
		localRecord.Filename(), inodeId).Out()
	fileId := localRecord.FileId()
	loadRecord, found := rc.fileMap[fileId]
	if !found {
		utils.Assert(localRecord.Type() == quantumfs.ObjectTypeDirectory,
			"Did not find loadRecord for %d", localRecord.FileId())
		// The dentry has been re-created, update its fileId in our caller to
		// match the new incarnation
		fileId = remoteRecord.FileId()
		loadRecord, found = rc.fileMap[remoteRecord.FileId()]
		utils.Assert(found, "Dir moved, but not found in buildmap %d",
			localRecord.Filename())

		moved = false
	}
	if loadRecord.localRecord != nil {
		c.vlog("Overwriting loadRecord")
		// Previously a localRecord has been attached to this load record.
		// This can only happen when local is a hardlinkleg and another
		// leg has been attached. Remove that leg now as we have found an
		// exact match.
		utils.Assert(localRecord.Type() == quantumfs.ObjectTypeHardlink,
			"Tried to overwrite object of type %d", localRecord.Type())
		utils.Assert(localRecord.Filename() == remoteRecord.Filename(),
			"Overriding needs an exact match, got %s vs %s",
			localRecord.Filename(), remoteRecord.Filename())
		if remoteRecord.Type() != quantumfs.ObjectTypeHardlink {
			// remote record is not a hardlink, so it can only have
			// one match
			rc.addStaleEntry(c, loadRecord.parentId, loadRecord.inodeId,
				loadRecord.localRecord)
		}

	}
	loadRecord.localRecord = localRecord
	loadRecord.inodeId = inodeId
	loadRecord.parentId = parentId
	loadRecord.moved = moved
	rc.fileMap[fileId] = loadRecord
	return fileId
}

// See if the remoteRecord can be used as a destination of a move from localRecord.
// Only one leg can be the destination of the move and it will be handled as part of
// moveDentry()
func (rc *RefreshContext) setHardlinkAsMoveDst(c *ctx,
	localRecord quantumfs.ImmutableDirectoryRecord,
	remoteRecord quantumfs.DirectoryRecord) bool {

	if localRecord == nil {
		c.vlog("nil localRecord")
		return false
	}
	if localRecord.Type() == quantumfs.ObjectTypeHardlink {
		c.vlog("localRecord is a hardlink as well")
		return false
	}
	loadRecord := rc.fileMap[localRecord.FileId()]
	if loadRecord.moved && !loadRecord.moveHasDst {
		c.vlog("Setting %s as the move destination", remoteRecord.Filename())
		loadRecord.moveHasDst = true
		loadRecord.remoteRecord = remoteRecord
		rc.fileMap[localRecord.FileId()] = loadRecord
		return true
	}
	return false
}

func (rc *RefreshContext) isLocalRecordUsable(c *ctx,
	localRecord quantumfs.ImmutableDirectoryRecord,
	remoteRecord quantumfs.DirectoryRecord) bool {

	loadRecord := rc.fileMap[localRecord.FileId()]

	if loadRecord == nil {
		return remoteRecord != nil &&
			localRecord.Type() == quantumfs.ObjectTypeDirectory &&
			remoteRecord.Type() == quantumfs.ObjectTypeDirectory
	}
	if localRecord.Type() == quantumfs.ObjectTypeHardlink {
		// the fact that it exists does not tell us anything
		// for hardlinks, detach it
		if remoteRecord != nil &&
			localRecord.FileId() == remoteRecord.FileId() {
			return true
		}
		if loadRecord.remoteRecord != nil &&
			loadRecord.localRecord == nil {
			// assume a move if no local record is attached yet
			return loadRecord.remoteRecord.Type() !=
				quantumfs.ObjectTypeHardlink
		}
		return false
	}

	if loadRecord.localRecord == nil {
		return true
	}

	// localRecord is populated the first time we encounter it. Except for
	// directories, if we hit it twice then we have a problem and should assert
	utils.Assert(localRecord.Type() == quantumfs.ObjectTypeDirectory,
		"Object of type %d already has a match.", localRecord.Type())
	return false
}

func (rc *RefreshContext) buildRefreshMapWsr(c *ctx, localRootId quantumfs.ObjectKey,
	remoteRootId quantumfs.ObjectKey) {

	defer c.funcIn("RefreshContext::buildRefreshMapWsr").Out()

	localWsr := MutableCopy(c, c.dataStore.Get(&c.Ctx,
		localRootId)).AsWorkspaceRoot()
	remoteWsr := MutableCopy(c, c.dataStore.Get(&c.Ctx,
		remoteRootId)).AsWorkspaceRoot()

	rc.buildRefreshMap(c, localWsr.BaseLayer(), remoteWsr.BaseLayer(), "")

	// we need to include all hardlink legs, since hardlink legs may have been
	// skipped in buildRefreshMap

	hardlinks := loadHardlinks(c, remoteWsr.HardlinkEntry())
	for fileId, linkEntry := range hardlinks {
		if _, exists := rc.fileMap[fileId]; exists {
			continue
		}

		record := newHardlinkLegFromRecord(linkEntry.record, nil)

		rc.fileMap[fileId] = &FileLoadRecord{
			remoteRecord:  record,
			inodeId:       quantumfs.InodeIdInvalid,
			parentId:      quantumfs.InodeIdInvalid,
			newParentPath: "",
			moved:         false,
		}
	}
}

func (rc *RefreshContext) buildRefreshMap(c *ctx, localDir quantumfs.ObjectKey,
	remoteDir quantumfs.ObjectKey, path string) {

	defer c.FuncIn("RefreshContext::buildRefreshMap", "%s", path).Out()

	c.vlog("Loading local records")
	localRecords := make(map[quantumfs.FileId]quantumfs.DirectoryRecord)
	foreachDentry(c, localDir,
		func(record quantumfs.ImmutableDirectoryRecord) {

			localRecords[record.FileId()] = record.Clone()
		})

	c.vlog("Loading remote records")
	foreachDentry(c, remoteDir, func(
		record quantumfs.ImmutableDirectoryRecord) {

		c.vlog("Added filemap entry for %s: %x", record.Filename(),
			record.FileId())

		rc.fileMap[record.FileId()] = &FileLoadRecord{
			remoteRecord:  record.Clone(),
			inodeId:       quantumfs.InodeIdInvalid,
			parentId:      quantumfs.InodeIdInvalid,
			newParentPath: path,
			moved:         false,
		}

		if record.Type() == quantumfs.ObjectTypeDirectory {
			localKey := quantumfs.EmptyDirKey

			// don't recurse into any directories that
			// haven't changed
			localRecord, exists := localRecords[record.FileId()]
			if exists {
				if skipDir(localRecord, record) {
					c.vlog("Skipping %s since no change",
						localRecord.Filename())
					return
				}

				localKey = localRecord.ID()
			}

			rc.buildRefreshMap(c, localKey, record.ID(),
				path+"/"+record.Filename())
		}
	})
}

func skipDir(local quantumfs.ImmutableDirectoryRecord,
	remote quantumfs.ImmutableDirectoryRecord) bool {

	if local == nil || remote == nil {
		return false
	}

	return local.ID().IsEqualTo(remote.ID()) &&
		local.FileId() == remote.FileId() &&
		local.Filename() == remote.Filename()
}

func shouldHideLocalRecord(localRecord quantumfs.ImmutableDirectoryRecord,
	remoteRecord quantumfs.ImmutableDirectoryRecord) bool {
	if remoteRecord == nil {
		return false
	}
	if remoteRecord.FileId() == localRecord.FileId() {
		return false
	}
	if localRecord.Type() == quantumfs.ObjectTypeDirectory &&
		remoteRecord.Type() == quantumfs.ObjectTypeDirectory {
		return false
	}
	// The localRecord has morphed into another entity in remote,
	return true
}

func detachInode(c *ctx, inode Inode, staleRecord *FileRemoveRecord) {
	defer c.FuncIn("detachInode", "name %s inode %d", staleRecord.name,
		staleRecord.inodeId).Out()
	dir := asDirectory(inode)
	defer dir.childRecordLock.Lock().Unlock()
	staleRecord.toOrphan = dir.children.deleteChild(c, staleRecord.name)
	c.qfs.noteDeletedInode(c, dir.id, staleRecord.inodeId, staleRecord.name)
}

// The reason we cannot unlink the stale dentries at this point is that
// if they are directories, then they might contain non-stale entries.
func detachStaleDentries(c *ctx, rc *RefreshContext) {
	defer c.funcIn("detachStaleDentries").Out()
	for i, staleRecord := range rc.staleRecords {
		inode := c.qfs.inodeNoInstantiate(c, staleRecord.parentId)
		if inode != nil {
			detachInode(c, inode, &staleRecord)
			rc.staleRecords[i] = staleRecord
		}
	}
}

// The caller must hold the linkLock
func (wsr *WorkspaceRoot) refreshRemoteHardlink_(c *ctx,
	rc *RefreshContext, hardlink *quantumfs.HardlinkRecord) {

	defer c.funcIn("WorkspaceRoot::refreshRemoteHardlink_").Out()
	id := quantumfs.FileId(hardlink.FileId())
	if entry, exists := wsr.hardlinkTable.hardlinks[id]; !exists {
		c.vlog("Adding new hardlink entry with id %d", id)
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = hardlink.Nlinks()
		wsr.hardlinkTable.hardlinks[id] = newLink
	} else {
		c.vlog("found mapping %d -> %s (nlink %d vs. %d)", id,
			entry.record.Filename(), hardlink.Nlinks(), entry.nlink)
		oldRecord := entry.record

		entry.nlink = hardlink.Nlinks()
		entry.record = hardlink.Record()
		wsr.hardlinkTable.hardlinks[id] = entry

		if !oldRecord.ID().IsEqualTo(hardlink.Record().ID()) {
			if inode := c.qfs.inodeNoInstantiate(c,
				entry.inodeId); inode != nil {

				c.vlog("Reloading inode %d: %s -> %s", entry.inodeId,
					oldRecord.ID().String(),
					hardlink.Record().ID().String())
				utils.Assert(!hardlink.Record().Type().IsImmutable(),
					"An immutable type cannot be reloaded.")
				reload(c, wsr.hardlinkTable, rc, inode,
					hardlink.Record())
			}
		}

		c.qfs.invalidateInode(c, entry.inodeId)
	}
}

func (wsr *WorkspaceRoot) refreshHardlinks(c *ctx,
	rc *RefreshContext, entry quantumfs.HardlinkEntry) {

	defer c.funcIn("WorkspaceRoot::refreshHardlinks").Out()
	defer wsr.hardlinkTable.linkLock.Lock().Unlock()

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		wsr.refreshRemoteHardlink_(c, rc, hardlink)
	})
}

// The caller must hold the tree lock
func (wsr *WorkspaceRoot) moveDentry_(c *ctx, oldName string,
	oldType quantumfs.ObjectType, remoteRecord quantumfs.DirectoryRecord,
	inodeId InodeId, parentId InodeId, path string) {

	defer c.FuncIn("WorkspaceRoot::moveDentry_", "%s %d %d",
		oldName, parentId, inodeId).Out()

	pathElements := strings.Split(path, "/")
	newParent, cleanup, err := wsr.followPath_DOWN(c, pathElements)
	defer cleanup()
	utils.Assert(err == nil, "could not handle error %v", err)

	srcInode := c.qfs.inode(c, parentId)

	inode := c.qfs.inodeNoInstantiate(c, inodeId)

	switch remoteRecord.Type() {
	case quantumfs.ObjectTypeDirectory:
		utils.Assert(false, "directories cannot be moved when refreshing.")
	case quantumfs.ObjectTypeHardlink:
		remoteRecord = newHardlinkLegFromRecord(remoteRecord,
			wsr.hardlinkTable)
		asDirectory(srcInode).moveHardlinkLeg_DOWN(c, newParent, oldName,
			remoteRecord, inodeId)

		if inode != nil {
			wsr.hardlinkTable.claimAsChild(inode)
		}
		wsr.hardlinkTable.updateHardlinkInodeId(c, remoteRecord.FileId(),
			inodeId)
	default:
		if oldType == quantumfs.ObjectTypeHardlink {
			asDirectory(srcInode).moveHardlinkLeg_DOWN(c, newParent,
				oldName, remoteRecord, inodeId)
			if inode != nil {
				inode.setParent(newParent.inodeNum())
			}
		} else if parentId == newParent.inodeNum() {
			srcInode.RenameChild(c, oldName, remoteRecord.Filename())
		} else {
			srcInode.MvChild(c, newParent, oldName,
				remoteRecord.Filename())
		}
	}

	c.qfs.noteDeletedInode(c, srcInode.inodeNum(), inodeId, oldName)
	c.qfs.noteChildCreated(c, newParent.inodeNum(), remoteRecord.Filename())
}

// The caller must hold the tree lock
func (wsr *WorkspaceRoot) moveDentries_(c *ctx, rc *RefreshContext) {
	defer c.funcIn("WorkspaceRoot::moveDentries_").Out()
	wsName := wsr.typespace + "/" + wsr.namespace + "/" + wsr.workspace
	for _, loadRecord := range rc.fileMap {
		if loadRecord.moved {
			oldName := loadRecord.localRecord.Filename()
			path := wsName + loadRecord.newParentPath + "/" + oldName
			wsr.moveDentry_(c, oldName, loadRecord.localRecord.Type(),
				loadRecord.remoteRecord,
				loadRecord.inodeId, loadRecord.parentId, path)
			inode := c.qfs.inodeNoInstantiate(c, loadRecord.inodeId)
			if inode != nil {
				reload(c, wsr.hardlinkTable, rc, inode,
					loadRecord.remoteRecord)
				c.qfs.invalidateInode(c, loadRecord.inodeId)
			}
		}
	}
}

func unlinkStaleDentries(c *ctx, rc *RefreshContext) {
	defer c.funcIn("unlinkStaleDentries").Out()
	for _, staleRecord := range rc.staleRecords {
		if staleRecord.type_ == quantumfs.ObjectTypeHardlink {
			continue
		}
		c.vlog("Unlinking entry %s type %d inodeId %d",
			staleRecord.name, staleRecord.type_, staleRecord.inodeId)
		if inode := c.qfs.inodeNoInstantiate(c,
			staleRecord.inodeId); inode != nil {

			result := inode.deleteSelf(c,
				func() (quantumfs.DirectoryRecord, fuse.Status) {
					return staleRecord.toOrphan, fuse.OK
				})
			if result != fuse.OK {
				panic("XXX handle deletion failure")
			}
		}
		c.qfs.removeUninstantiated(c, []InodeId{staleRecord.inodeId})
	}
}

func (wsr *WorkspaceRoot) unlinkStaleHardlinks(c *ctx,
	rc *RefreshContext, entry quantumfs.HardlinkEntry) {

	defer c.funcIn("WorkspaceRoot::unlinkStaleHardlinks").Out()
	defer wsr.hardlinkTable.linkLock.Lock().Unlock()

	for fileId, entry := range wsr.hardlinkTable.hardlinks {
		loadRecord, exists := rc.fileMap[fileId]
		if !exists {
			c.vlog("Removing hardlink id %d, inode %d, nlink %d",
				fileId, entry.inodeId, entry.nlink)
			if inode := c.qfs.inodeNoInstantiate(c,
				entry.inodeId); inode != nil {

				inode.orphan(c, entry.record)
			}
			wsr.hardlinkTable.removeHardlink_(fileId, entry.inodeId)
		} else if loadRecord.remoteRecord.Type() !=
			quantumfs.ObjectTypeHardlink {

			wsr.hardlinkTable.removeHardlink_(fileId, entry.inodeId)
		}
	}
}

// The caller must hold the tree lock
func (wsr *WorkspaceRoot) refreshTo_(c *ctx, rc *RefreshContext) {

	defer c.funcIn("WorkspaceRoot::refreshTo_").Out()

	buffer := c.dataStore.Get(&c.Ctx, rc.rootId)
	workspaceRoot := MutableCopy(c, buffer).AsWorkspaceRoot()
	baseLayerId := workspaceRoot.BaseLayer()
	hardlinkEntry := workspaceRoot.HardlinkEntry()

	wsr.updateRefreshMap_DOWN(c, rc, &baseLayerId)
	detachStaleDentries(c, rc)
	wsr.refreshHardlinks(c, rc, hardlinkEntry)
	wsr.refresh_DOWN(c, rc, baseLayerId)
	wsr.moveDentries_(c, rc)
	unlinkStaleDentries(c, rc)
	wsr.unlinkStaleHardlinks(c, rc, hardlinkEntry)
}
