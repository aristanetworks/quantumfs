// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type HardlinkTableEntry struct {
	record  quantumfs.DirectoryRecord
	nlink   uint32
	inodeId InodeId
	paths   []string

	// delta shows the changes made to the nlink in the tree that
	// have not been published yet.
	// The invariant for each entry is that nlink+delta is always
	// coherent a user perspective and nlink is always consistent with
	// the workspaceroot that is getting published
	delta int
}

type HardlinkTable interface {
	getHardlinkByInode(inodeId InodeId) (bool, quantumfs.DirectoryRecord)
	checkHardlink(inodeId InodeId) (bool, quantumfs.FileId)
	instantiateHardlink(c *ctx, inodeNum InodeId) Inode
	markHardlinkPath(c *ctx, path string, fileId quantumfs.FileId)
	findHardlinkInodeId(c *ctx, fileId quantumfs.FileId, inodeId InodeId) InodeId
	hardlinkDec(fileId quantumfs.FileId) quantumfs.DirectoryRecord
	hardlinkInc(fileId quantumfs.FileId)
	newHardlink(c *ctx, inodeId InodeId,
		record quantumfs.DirectoryRecord) *HardlinkLeg
	getHardlink(fileId quantumfs.FileId) (valid bool,
		record quantumfs.ImmutableDirectoryRecord)
	updateHardlinkInodeId(c *ctx, fileId quantumfs.FileId, inodeId InodeId)
	setHardlink(fileId quantumfs.FileId,
		fnSetter func(dir quantumfs.DirectoryRecord))
	nlinks(fileId quantumfs.FileId) uint32
	claimAsChild_(inode Inode)
	claimAsChild(inode Inode)
	getWorkspaceRoot() *WorkspaceRoot
	apply(c *ctx, hardlinkDelta *HardlinkDelta)
	getNormalized(fileId quantumfs.FileId) quantumfs.DirectoryRecord
	invalidateNormalizedRecordLock(fileId quantumfs.FileId) utils.NeedWriteUnlock
}

type HardlinkTableImpl struct {
	wsr *WorkspaceRoot

	linkLock    utils.DeferableRwMutex
	hardlinks   map[quantumfs.FileId]HardlinkTableEntry
	inodeToLink map[InodeId]quantumfs.FileId
}

func newLinkEntry(record_ quantumfs.DirectoryRecord) HardlinkTableEntry {
	return HardlinkTableEntry{
		record:  record_,
		nlink:   1,
		inodeId: quantumfs.InodeIdInvalid,
		delta:   0,
	}
}

func (ht *HardlinkTableImpl) checkHardlink(inodeId InodeId) (bool,
	quantumfs.FileId) {

	defer ht.linkLock.RLock().RUnlock()
	fileId, exists := ht.inodeToLink[inodeId]
	if !exists {
		return false, quantumfs.InvalidFileId
	}

	return true, fileId
}

func newHardlinkTable(c *ctx, wsr *WorkspaceRoot,
	hardlinkEntry quantumfs.HardlinkEntry) *HardlinkTableImpl {

	table := HardlinkTableImpl{
		inodeToLink: make(map[InodeId]quantumfs.FileId),
		wsr:         wsr,
	}
	table.hardlinks = loadHardlinks(c, hardlinkEntry)
	return &table
}

// Must be called with inode's parentLock locked for writing
func (ht *HardlinkTableImpl) claimAsChild_(inode Inode) {
	inode.setParent_(ht.getWorkspaceRoot().inodeNum())
}

func (ht *HardlinkTableImpl) claimAsChild(inode Inode) {
	defer inode.getParentLock().Lock().Unlock()
	inode.setParent_(ht.getWorkspaceRoot().inodeNum())
}

func (ht *HardlinkTableImpl) getWorkspaceRoot() *WorkspaceRoot {
	return ht.wsr
}

func (ht *HardlinkTableImpl) nlinks(fileId quantumfs.FileId) uint32 {
	defer ht.linkLock.RLock().RUnlock()

	entry, exists := ht.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Invalid fileId in system %d", fileId))
	}

	return entry.effectiveNlink()
}

func (ht *HardlinkTableImpl) hardlinkInc(fileId quantumfs.FileId) {
	defer ht.linkLock.Lock().Unlock()

	entry, exists := ht.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))
	}

	// Linking updates ctime
	entry.record.SetContentTime(quantumfs.NewTime(time.Now()))

	entry.delta++
	ht.hardlinks[fileId] = entry
}

func (ht *HardlinkTableImpl) hardlinkDec(
	fileId quantumfs.FileId) quantumfs.DirectoryRecord {

	defer ht.linkLock.Lock().Unlock()

	entry, exists := ht.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))
	}

	if entry.effectiveNlink() > 0 {
		entry.delta--
	} else {
		panic("over decrement in hardlink ref count")
	}

	// Unlinking updates ctime
	entry.record.SetContentTime(quantumfs.NewTime(time.Now()))

	if entry.effectiveNlink() > 0 {
		ht.hardlinks[fileId] = entry
		return nil
	}

	// all references to this hardlink are gone and we must remove it
	return entry.record
}

// Must hold the linkLock for writing
func (ht *HardlinkTableImpl) removeHardlink_(fileId quantumfs.FileId,
	inodeId InodeId) {

	delete(ht.hardlinks, fileId)

	if inodeId != quantumfs.InodeIdInvalid {
		delete(ht.inodeToLink, inodeId)
	}
}

func (ht *HardlinkTableImpl) newHardlink(c *ctx, inodeId InodeId,
	record quantumfs.DirectoryRecord) *HardlinkLeg {

	defer c.FuncIn("HardlinkTableImpl::newHardlink", "inode %d", inodeId).Out()

	if _, isLink := record.(*HardlinkLeg); isLink {
		panic("newHardlink called on existing hardlink")
	}

	defer ht.linkLock.Lock().Unlock()

	newEntry := newLinkEntry(record)
	newEntry.inodeId = inodeId
	// Linking updates ctime
	newEntry.record.SetContentTime(quantumfs.NewTime(time.Now()))
	newEntry.record.SetFilename("")

	fileId := record.FileId()
	utils.Assert(fileId != quantumfs.InvalidFileId, "invalid fileId")
	ht.hardlinks[fileId] = newEntry
	ht.inodeToLink[inodeId] = fileId

	// Don't reparent the inode, the caller must do so while holding the inode's
	// parent lock
	ht.getWorkspaceRoot().dirty(c)

	return newHardlinkLeg(record.Filename(), fileId,
		quantumfs.NewTime(time.Now()), ht)
}

func (ht *HardlinkTableImpl) instantiateHardlink(c *ctx, inodeId InodeId) Inode {
	defer c.FuncIn("HardlinkTableImpl::instantiateHardlink",
		"inode %d", inodeId).Out()

	hardlinkRecord := func() quantumfs.DirectoryRecord {
		defer ht.linkLock.RLock().RUnlock()

		id, exists := ht.inodeToLink[inodeId]
		if !exists {
			return nil
		}

		c.dlog("Instantiating hardlink %d", id)
		return ht.hardlinks[id].record
	}()
	if hardlinkRecord == nil {
		return nil
	}
	if inode := c.qfs.inodeNoInstantiate(c, inodeId); inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeId)
		return inode
	}
	inode, _ := ht.getWorkspaceRoot().Directory.recordToChild(c,
		inodeId, hardlinkRecord)
	return inode
}

func (ht *HardlinkTableImpl) findHardlinkInodeId(c *ctx,
	fileId quantumfs.FileId, inodeId InodeId) InodeId {

	defer c.FuncIn("HardlinkTableImpl::findHardlinkInodeId", "%d inode %d",
		fileId, inodeId).Out()
	defer ht.linkLock.Lock().Unlock()

	hardlink, exists := ht.hardlinks[fileId]
	if !exists {
		c.vlog("fileId isn't a hardlink")
		return inodeId
	}
	if hardlink.inodeId != quantumfs.InodeIdInvalid {
		if inodeId != quantumfs.InodeIdInvalid {
			utils.Assert(inodeId == hardlink.inodeId,
				"requested hardlink inode %d exists as %d",
				inodeId, hardlink.inodeId)
		}
		c.vlog("filedId a hardlink with inode %d", hardlink.inodeId)
		return hardlink.inodeId
	}

	if inodeId != quantumfs.InodeIdInvalid {
		return inodeId
	}

	inodeId = c.qfs.newInodeId()
	hardlink.inodeId = inodeId
	ht.hardlinks[fileId] = hardlink
	ht.inodeToLink[inodeId] = fileId

	c.vlog("Allocated new inode %d for hardlink", inodeId)
	return inodeId
}

// Ensure we don't return the vanilla record, enclose it in a hardlink wrapper so
// that the wrapper can correctly pick and choose attributes like nlink
func (ht *HardlinkTableImpl) getHardlinkByInode(inodeId InodeId) (valid bool,
	record quantumfs.DirectoryRecord) {

	defer ht.linkLock.RLock().RUnlock()

	fileId, exists := ht.inodeToLink[inodeId]
	if !exists {
		return false, nil
	}

	link, exists := ht.hardlinks[fileId]
	if !exists {
		return false, nil
	}

	return true, newHardlinkLeg(link.record.Filename(), fileId,
		quantumfs.Time(0), ht)
}

func (ht *HardlinkTableImpl) getHardlink(fileId quantumfs.FileId) (valid bool,
	record quantumfs.ImmutableDirectoryRecord) {

	defer ht.linkLock.RLock().RUnlock()

	link, exists := ht.hardlinks[fileId]
	if exists {
		return true, link.record
	}

	return false, nil
}

func (ht *HardlinkTableImpl) updateHardlinkInodeId(c *ctx, fileId quantumfs.FileId,
	inodeId InodeId) {

	defer c.FuncIn("HardlinkTableImpl::updateHardlinkInodeId", "%d: %d",
		fileId, inodeId).Out()
	defer ht.linkLock.Lock().Unlock()

	hardlink, exists := ht.hardlinks[fileId]
	utils.Assert(exists, "Hardlink id %d does not exist.", fileId)

	if hardlink.inodeId == inodeId {
		return
	}
	utils.Assert(hardlink.inodeId == quantumfs.InodeIdInvalid,
		"Hardlink id %d already has associated inodeid %d",
		fileId, hardlink.inodeId)
	hardlink.inodeId = inodeId
	ht.hardlinks[fileId] = hardlink
	ht.inodeToLink[inodeId] = fileId
}

// We need the hardlink table linklock to cover setting safely
func (ht *HardlinkTableImpl) setHardlink(fileId quantumfs.FileId,
	fnSetter func(dir quantumfs.DirectoryRecord)) {

	defer ht.linkLock.Lock().Unlock()

	link, exists := ht.hardlinks[fileId]
	utils.Assert(exists, fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))

	// It's critical that our lock covers both the fetch and this change
	fnSetter(link.record)
}

func (hte *HardlinkTableEntry) effectiveNlink() uint32 {
	return uint32(int(hte.nlink) + hte.delta)
}

func loadHardlinks(c *ctx,
	entry quantumfs.HardlinkEntry) map[quantumfs.FileId]HardlinkTableEntry {

	defer c.funcIn("loadHardlinks").Out()

	hardlinks := make(map[quantumfs.FileId]HardlinkTableEntry)

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = hardlink.Nlinks()
		id := quantumfs.FileId(hardlink.FileId())
		hardlinks[id] = newLink
	})

	return hardlinks
}

func (ht *HardlinkTableImpl) markHardlinkPath(c *ctx, path string,
	fileId quantumfs.FileId) {

	defer c.FuncIn("HardlinkTableImpl::markHardlinkPath", "%s %d", path,
		fileId).Out()
	defer ht.linkLock.Lock().Unlock()

	list := make([]string, 0)
	link, exists := ht.hardlinks[fileId]
	if exists {
		list = link.paths
	}

	// ensure there are no duplicates (like from renames, etc)
	for _, curPath := range list {
		if curPath == path {
			// done early
			return
		}
	}

	link.paths = append(link.paths, path)
	ht.hardlinks[fileId] = link
}

func (ht *HardlinkTableImpl) apply(c *ctx, hardlinkDelta *HardlinkDelta) {
	defer c.funcIn("HardlinkTableImpl::apply").Out()

	defer ht.linkLock.Lock().Unlock()
	hardlinkDelta.foreach(func(fileId quantumfs.FileId, delta int) {
		entry, exists := ht.hardlinks[fileId]
		if !exists {
			c.vlog("Did not find %d. Dropping delta %d",
				fileId, delta)
			return
		}
		c.vlog("Updating nlink of %d: %d+%d (delta %d)", fileId,
			entry.nlink, delta, entry.delta)

		entry.nlink = uint32(int(entry.nlink) + delta)
		entry.delta -= delta
		if entry.nlink == 0 {
			ht.removeHardlink_(fileId, entry.inodeId)
		} else {
			ht.hardlinks[fileId] = entry
		}
	})
	hardlinkDelta.reset()
}

func (ht *HardlinkTableImpl) getNormalized(
	fileId quantumfs.FileId) quantumfs.DirectoryRecord {

	defer ht.linkLock.RLock().RUnlock()

	link, exists := ht.hardlinks[fileId]
	if !exists {
		return nil
	}
	if link.effectiveNlink() != 1 {
		return nil
	}
	return link.record
}

// Invalidate the inode of the normalizing child in the hardlink table
// and allow the caller to update the parent of the inode
func (ht *HardlinkTableImpl) invalidateNormalizedRecordLock(
	fileId quantumfs.FileId) (unlock utils.NeedWriteUnlock) {

	unlock = ht.linkLock.Lock()

	link, exists := ht.hardlinks[fileId]
	utils.Assert(exists, "did not find %d in the hardlink table", fileId)

	delete(ht.inodeToLink, link.inodeId)
	link.inodeId = quantumfs.InodeIdInvalid
	ht.hardlinks[fileId] = link
	return
}
