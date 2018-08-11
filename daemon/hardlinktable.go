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
	publishableRecord quantumfs.DirectoryRecord
	effectiveRecord   quantumfs.DirectoryRecord
	nlink             int64
	inodeId           InodeId
	paths             []string

	// delta shows the changes made to the nlink in the tree that
	// have not been published yet.
	//
	// The invariant for each entry is that nlink+delta is always
	// coherent a user perspective and nlink is always consistent with
	// the workspaceroot that is getting published
	delta int

	// numDeltas is the total number of deltas still bubbling up to the root. If
	// this is zero then the effective and publishable views are identical,
	// otherwise the two views are different, even if delta is zero. This is to
	// handle the case where a hardlink is created and deleted in several
	// directories in such a way that the deltas happen to cancel out to zero.
	numDeltas int
}

func (hte *HardlinkTableEntry) record() quantumfs.DirectoryRecord {
	if hte.effectiveRecord != nil {
		return hte.effectiveRecord
	}
	return hte.publishableRecord
}

func (hte *HardlinkTableEntry) setID(c *ctx, key quantumfs.ObjectKey) {
	defer c.FuncIn("HardlinkTableEntry::setID", "inode %d newKey %s",
		hte.inodeId, key.String())

	record := hte.record()
	record.SetID(key)

	if hte.effectiveRecord != nil {
		hte.publishableRecord = hte.effectiveRecord
		hte.effectiveRecord = nil
	}
}

type HardlinkTable interface {
	recordByInodeId(c *ctx, inodeId InodeId) quantumfs.ImmutableDirectoryRecord
	recordByFileId(fileId quantumfs.FileId) (
		record quantumfs.ImmutableDirectoryRecord)
	modifyChildWithFunc(c *ctx, inodeId InodeId,
		modify func(record quantumfs.DirectoryRecord))
	makePublishable(c *ctx, fileId quantumfs.FileId)
	setID(c *ctx, fileId quantumfs.FileId, key quantumfs.ObjectKey)
	checkHardlink(inodeId InodeId) (bool, quantumfs.FileId)
	instantiateHardlink(c *ctx, inodeNum InodeId) Inode
	markHardlinkPath(c *ctx, path string, fileId quantumfs.FileId)
	findHardlinkInodeId(c *ctx, fileId quantumfs.FileId, inodeId InodeId) InodeId
	hardlinkDec(fileId quantumfs.FileId) (effective quantumfs.DirectoryRecord)
	hardlinkInc(fileId quantumfs.FileId)
	newHardlink(c *ctx, inodeId InodeId,
		record quantumfs.DirectoryRecord) *HardlinkLeg
	updateHardlinkInodeId(c *ctx, fileId quantumfs.FileId, inodeId InodeId)
	nlinks(fileId quantumfs.FileId) uint32
	claimAsChild_(c *ctx, inode Inode)
	claimAsChild(c *ctx, inode Inode)
	getWorkspaceRoot() *WorkspaceRoot
	apply(c *ctx, hardlinkDelta *HardlinkDelta)
	getNormalized(fileId quantumfs.FileId) (
		publishable quantumfs.DirectoryRecord,
		effective quantumfs.DirectoryRecord)
	invalidateNormalizedRecordLock(fileId quantumfs.FileId) utils.NeedWriteUnlock
}

type HardlinkTableImpl struct {
	wsr *WorkspaceRoot

	linkLock    utils.DeferableRwMutex
	hardlinks   map[quantumfs.FileId]*HardlinkTableEntry
	inodeToLink map[InodeId]quantumfs.FileId
}

func newLinkEntry(record_ quantumfs.DirectoryRecord) *HardlinkTableEntry {
	return &HardlinkTableEntry{
		publishableRecord: record_,
		nlink:             1,
		inodeId:           quantumfs.InodeIdInvalid,
		delta:             0,
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
func (ht *HardlinkTableImpl) claimAsChild_(c *ctx, inode Inode) {
	inode.setParent_(c, ht.getWorkspaceRoot())
}

func (ht *HardlinkTableImpl) claimAsChild(c *ctx, inode Inode) {
	inode.setParent(c, ht.getWorkspaceRoot())
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
	entry.record().SetContentTime(quantumfs.NewTime(time.Now()))

	entry.delta++
	entry.numDeltas++
}

func (ht *HardlinkTableImpl) hardlinkDec(
	fileId quantumfs.FileId) (effective quantumfs.DirectoryRecord) {

	defer ht.linkLock.Lock().Unlock()

	entry, exists := ht.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))
	}

	entry.numDeltas++
	if entry.effectiveNlink() > 0 {
		entry.delta--
	} else {
		panic("over decrement in hardlink ref count")
	}

	// Unlinking updates ctime
	entry.record().SetContentTime(quantumfs.NewTime(time.Now()))

	if entry.effectiveNlink() > 0 {
		ht.hardlinks[fileId] = entry
		return nil
	}

	// all references to this hardlink are gone and we must remove it
	if entry.effectiveRecord != nil {
		return entry.effectiveRecord
	} else {
		return entry.publishableRecord
	}
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
	newEntry.record().SetContentTime(quantumfs.NewTime(time.Now()))
	newEntry.record().SetFilename("")

	fileId := record.FileId()
	utils.Assert(fileId != quantumfs.InvalidFileId, "invalid fileId")
	_, exists := ht.hardlinks[fileId]
	utils.Assert(!exists, "newHardlink on pre-existing FileId %d", fileId)

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

		c.vlog("Instantiating hardlink %d", id)
		return ht.hardlinks[id].record()
	}()
	if hardlinkRecord == nil {
		return nil
	}
	inode, release := c.qfs.inodeNoInstantiate(c, inodeId)
	defer release()
	if inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeId)
		return inode
	}
	inode := ht.getWorkspaceRoot().Directory.recordToChild(c,
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
	ht.inodeToLink[inodeId] = fileId

	c.vlog("Allocated new inode %d for hardlink", inodeId)
	return inodeId
}

// Ensure we don't return the vanilla record, enclose it in a hardlink wrapper so
// that the wrapper can correctly pick and choose attributes like nlink
func (ht *HardlinkTableImpl) recordByInodeId(c *ctx, inodeId InodeId) (
	record quantumfs.ImmutableDirectoryRecord) {

	defer ht.linkLock.RLock().RUnlock()

	fileId, exists := ht.inodeToLink[inodeId]
	if !exists {
		return nil
	}

	link, exists := ht.hardlinks[fileId]
	if !exists {
		return nil
	}

	return newHardlinkLeg(link.record().Filename(), fileId,
		quantumfs.Time(0), ht)
}

func (ht *HardlinkTableImpl) recordByFileId(fileId quantumfs.FileId) (
	record quantumfs.ImmutableDirectoryRecord) {

	defer ht.linkLock.RLock().RUnlock()

	link, exists := ht.hardlinks[fileId]
	if exists {
		return link.record()
	}

	return nil
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
	ht.inodeToLink[inodeId] = fileId
}

func (ht *HardlinkTableImpl) modifyChildWithFunc(c *ctx, inodeId InodeId,
	modify func(record quantumfs.DirectoryRecord)) {

	defer c.FuncIn("HardlinkTableImpl::modifyChildWithFunc", "inode %d",
		inodeId).Out()

	defer ht.linkLock.Lock().Unlock()

	fileId, exists := ht.inodeToLink[inodeId]
	if !exists {
		return
	}

	link, exists := ht.hardlinks[fileId]
	utils.Assert(exists, "Hardlink is in inodeToLink but not hardlinks %d %d",
		inodeId, fileId)

	if link.effectiveRecord == nil {
		link.effectiveRecord = link.publishableRecord.Clone()
	}
	modify(link.effectiveRecord)
}

func (ht *HardlinkTableImpl) makePublishable(c *ctx, fileId quantumfs.FileId) {
	defer c.FuncIn("HardlinkTableImpl::makePublishable", "fileId %d", fileId)

	defer ht.linkLock.Lock().Unlock()

	link, exists := ht.hardlinks[fileId]
	utils.Assert(exists, "Hardlink %d does not exist", fileId)

	if link.effectiveRecord != nil {
		link.publishableRecord = link.effectiveRecord
		link.effectiveRecord = nil
	}
}

func (ht *HardlinkTableImpl) setID(c *ctx, fileId quantumfs.FileId,
	key quantumfs.ObjectKey) {

	defer c.FuncIn("HardlinkTableImpl::setID", "fileId %d key %s", fileId,
		key.String()).Out()

	defer ht.linkLock.Lock().Unlock()
	entry := ht.hardlinks[fileId]
	entry.setID(c, key)
}

func (hte *HardlinkTableEntry) effectiveNlink() uint32 {
	return uint32(int(hte.nlink) + hte.delta)
}

func loadHardlinks(c *ctx,
	entry quantumfs.HardlinkEntry) map[quantumfs.FileId]*HardlinkTableEntry {

	defer c.funcIn("loadHardlinks").Out()

	hardlinks := make(map[quantumfs.FileId]*HardlinkTableEntry)

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = int64(hardlink.Nlinks())
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
}

func (ht *HardlinkTableImpl) apply(c *ctx, hardlinkDelta *HardlinkDelta) {
	defer c.funcIn("HardlinkTableImpl::apply").Out()

	defer ht.linkLock.Lock().Unlock()
	hardlinkDelta.foreach(func(fileId quantumfs.FileId, delta deltaTuple) {
		entry, exists := ht.hardlinks[fileId]
		if !exists {
			c.elog("Did not find %d. Dropping delta +%d -%d",
				fileId, delta.additions, delta.deletions)
			return
		}
		c.vlog("Updating nlink of %d: %d + d (delta +%d -%d)", fileId,
			entry.nlink, entry.numDeltas, delta.additions,
			delta.deletions)

		entry.nlink = int64(int(entry.nlink) + delta.additions -
			delta.deletions)
		entry.delta = entry.delta - delta.additions + delta.deletions
		entry.numDeltas -= delta.additions + delta.deletions
		if entry.nlink == 0 && entry.numDeltas == 0 {
			ht.removeHardlink_(fileId, entry.inodeId)
		}
	})
	hardlinkDelta.reset()
}

func (ht *HardlinkTableImpl) getNormalized(
	fileId quantumfs.FileId) (publishable quantumfs.DirectoryRecord,
	effective quantumfs.DirectoryRecord) {

	defer ht.linkLock.RLock().RUnlock()

	link, exists := ht.hardlinks[fileId]
	if !exists {
		return nil, nil
	}

	// Only normalize if there are no more legs or deltas inbound
	if link.nlink == 1 && link.numDeltas == 0 {
		return link.publishableRecord, link.effectiveRecord
	}

	return nil, nil
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
	return
}
