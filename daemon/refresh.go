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
	localRecord   quantumfs.DirectoryRecord
	inodeId       InodeId
	parentId      InodeId
	newParentPath string
	moved         bool
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
}

func newRefreshContext() RefreshContext {
	return RefreshContext{
		fileMap:      make(map[quantumfs.FileId]*FileLoadRecord, 0),
		staleRecords: make([]FileRemoveRecord, 0),
	}
}

func (rc *RefreshContext) addStaleEntry(c *ctx, parentId InodeId, inodeId InodeId,
	localRecord quantumfs.DirectoryRecord) {

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
	inodeId InodeId, moved bool, localRecord quantumfs.DirectoryRecord) {

	defer c.FuncIn("RefreshContext::attachLocalRecord", "name %s inode %d",
		localRecord.Filename(), inodeId).Out()
	loadRecord := rc.fileMap[localRecord.FileId()]
	loadRecord.localRecord = localRecord
	loadRecord.inodeId = inodeId
	loadRecord.parentId = parentId
	loadRecord.moved = moved
	rc.fileMap[localRecord.FileId()] = loadRecord
}

func (rc *RefreshContext) isInodeUsedAfterRefresh(c *ctx,
	localRecord quantumfs.DirectoryRecord,
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
		return remoteRecord != nil &&
			localRecord.FileId() == remoteRecord.FileId()
	}
	if loadRecord.localRecord == nil {
		return true
	}
	utils.Assert(localRecord.Type() == quantumfs.ObjectTypeDirectory,
		"Object of type %d already has a match.", localRecord.Type())
	return false
}

func (rc *RefreshContext) buildRefreshMap(c *ctx, baseLayerId quantumfs.ObjectKey,
	path string) {

	defer c.FuncIn("RefreshContext::buildRefreshMap", "%s", path).Out()
	foreachDentry(c, baseLayerId, func(record *quantumfs.DirectRecord) {
		rc.fileMap[record.FileId()] = &FileLoadRecord{
			remoteRecord:  record,
			inodeId:       quantumfs.InodeIdInvalid,
			parentId:      quantumfs.InodeIdInvalid,
			newParentPath: path,
			moved:         false,
		}
		if record.Type() == quantumfs.ObjectTypeDirectory {
			rc.buildRefreshMap(c, record.ID(),
				path+"/"+record.Filename())
		}
	})
}

func shouldHideLocalRecord(localRecord quantumfs.DirectoryRecord,
	remoteRecord quantumfs.DirectoryRecord) bool {
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
	staleRecord.toOrphan = dir.children.deleteChild(c, staleRecord.name, false)
	c.qfs.noteDeletedInode(dir.id, staleRecord.inodeId, staleRecord.name)
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
	if entry, exists := wsr.hardlinks[id]; !exists {
		c.vlog("Adding new hardlink entry with id %d", id)
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = hardlink.Nlinks()
		wsr.hardlinks[id] = newLink
	} else {
		c.vlog("found mapping %d -> %s (nlink %d vs. %d)", id,
			entry.record.Filename(), hardlink.Nlinks(), entry.nlink)
		oldRecord := entry.record

		entry.nlink = hardlink.Nlinks()
		entry.record = hardlink.Record()
		wsr.hardlinks[id] = entry

		if !oldRecord.ID().IsEqualTo(hardlink.Record().ID()) {
			if inode := c.qfs.inodeNoInstantiate(c,
				entry.inodeId); inode != nil {

				c.vlog("Reloading inode %d: %s -> %s", entry.inodeId,
					oldRecord.ID().String(),
					hardlink.Record().ID().String())
				utils.Assert(!hardlink.Record().Type().IsImmutable(),
					"An immutable type cannot be reloaded.")
				reload(c, wsr, rc, inode, hardlink.Record())
			}
		}

		status := c.qfs.invalidateInode(entry.inodeId)
		utils.Assert(status == fuse.OK,
			"invalidating %d failed with %d", entry.inodeId, status)
	}
}

func (wsr *WorkspaceRoot) refreshHardlinks(c *ctx,
	rc *RefreshContext, entry quantumfs.HardlinkEntry) {

	defer c.funcIn("WorkspaceRoot::refreshHardlinks").Out()
	defer wsr.linkLock.Lock().Unlock()

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		wsr.refreshRemoteHardlink_(c, rc, hardlink)
	})
}

// The caller must hold the tree lock
func (wsr *WorkspaceRoot) moveDentry_(c *ctx, oldName string,
	remoteRecord quantumfs.DirectoryRecord,
	inodeId InodeId, parentId InodeId, path string) {

	defer c.FuncIn("WorkspaceRoot::moveDentry_", "%s %d %d",
		oldName, parentId, inodeId).Out()

	utils.Assert(remoteRecord.Type() != quantumfs.ObjectTypeDirectory,
		"directories cannot be moved when refreshing.")
	pathElements := strings.Split(path, "/")
	newParent, cleanup, err := wsr.followPath_DOWN(c, pathElements)
	defer cleanup()
	utils.Assert(err == nil, "could not handle error %v", err)

	srcInode := c.qfs.inode(c, parentId)
	if parentId == newParent.inodeNum() {
		srcInode.RenameChild(c, oldName, remoteRecord.Filename())
	} else {
		srcInode.MvChild(c, newParent, oldName, remoteRecord.Filename())
	}
	c.qfs.noteDeletedInode(srcInode.inodeNum(), inodeId, oldName)
	c.qfs.noteChildCreated(newParent.inodeNum(), remoteRecord.Filename())
}

// The caller must hold the tree lock
func (wsr *WorkspaceRoot) moveDentries_(c *ctx, rc *RefreshContext) {
	wsName := wsr.typespace + "/" + wsr.namespace + "/" + wsr.workspace
	for _, loadRecord := range rc.fileMap {
		if loadRecord.moved {
			oldName := loadRecord.localRecord.Filename()
			path := wsName + loadRecord.newParentPath + "/" + oldName
			wsr.moveDentry_(c, oldName, loadRecord.remoteRecord,
				loadRecord.inodeId, loadRecord.parentId, path)
			inode := c.qfs.inodeNoInstantiate(c, loadRecord.inodeId)
			if inode != nil {
				reload(c, wsr, rc, inode, loadRecord.remoteRecord)
				status := c.qfs.invalidateInode(loadRecord.inodeId)
				utils.Assert(status == fuse.OK,
					"invalidating %d failed with %d",
					loadRecord.inodeId, status)
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
	defer wsr.linkLock.Lock().Unlock()

	for fileId, entry := range wsr.hardlinks {
		loadRecord, exists := rc.fileMap[fileId]
		if !exists {
			c.vlog("Removing hardlink id %d, inode %d, nlink %d",
				fileId, entry.inodeId, entry.nlink)
			if inode := c.qfs.inodeNoInstantiate(c,
				entry.inodeId); inode != nil {

				inode.orphan(c, entry.record)
			}
			wsr.removeHardlink_(fileId, entry.inodeId)
		} else if loadRecord.remoteRecord.Type() !=
			quantumfs.ObjectTypeHardlink {

			wsr.removeHardlink_(fileId, entry.inodeId)
		}
	}
}

// The caller must hold the tree lock
func (wsr *WorkspaceRoot) refreshTo_(c *ctx, rootId quantumfs.ObjectKey) {
	defer c.funcIn("WorkspaceRoot::refreshTo_").Out()

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()
	baseLayerId := workspaceRoot.BaseLayer()
	hardlinkEntry := workspaceRoot.HardlinkEntry()
	rc := newRefreshContext()

	rc.buildRefreshMap(c, baseLayerId, "")
	wsr.updateRefreshMap_DOWN(c, &rc, &baseLayerId)
	detachStaleDentries(c, &rc)
	wsr.refreshHardlinks(c, &rc, hardlinkEntry)
	wsr.refresh_DOWN(c, &rc, baseLayerId)
	wsr.moveDentries_(c, &rc)
	unlinkStaleDentries(c, &rc)
	wsr.unlinkStaleHardlinks(c, &rc, hardlinkEntry)
}
