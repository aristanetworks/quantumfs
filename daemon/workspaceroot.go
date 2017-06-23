// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func init(){
	rand.Seed(time.Now().UnixNano())
}

// WorkspaceRoot acts similarly to a directory except only a single object ID is used
// instead of one for each layer and that ID is directly requested from the
// WorkspaceDB instead of passed in from the parent.
type WorkspaceRoot struct {
	Directory
	typespace       string
	namespace       string
	workspace       string
	publishedRootId quantumfs.ObjectKey

	listLock   sync.Mutex
	accessList quantumfs.PathsAccessed

	// The RWMutex which backs the treeLock for all the inodes in this workspace
	// tree.
	realTreeLock sync.RWMutex

	// Hardlink support structures
	linkLock       utils.DeferableRwMutex
	hardlinks      map[HardlinkId]linkEntry
	inodeToLink    map[InodeId]HardlinkId
}

type linkEntry struct {
	record  *quantumfs.DirectRecord
	nlink   uint32
	inodeId InodeId
}

func newLinkEntry(record_ *quantumfs.DirectRecord) linkEntry {
	return linkEntry{
		record:  record_,
		nlink:   2,
		inodeId: quantumfs.InodeIdInvalid,
	}
}

// Fetching the number of child directories for all the workspaces within a namespace
// is relatively expensive and not terribly useful. Instead fake it and assume a
// normal number here.
func fillWorkspaceAttrFake(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	typespace string, namespace string) {

	defer c.FuncIn("fillWorkspaceAttrFake", "inode %d typespace %s namespace %s",
		inodeNum, typespace, namespace).Out()

	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(c *ctx, typespace string, namespace string, workspace string,
	parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("WorkspaceRoot::newWorkspaceRoot", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	var wsr WorkspaceRoot

	rootId, err := c.workspaceDB.Workspace(&c.Ctx,
		typespace, namespace, workspace)
	utils.Assert(err == nil, "BUG: 175630 - handle workspace API errors")
	c.vlog("Workspace Loading %s/%s/%s %s",
		typespace, namespace, workspace, rootId.Text())

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	defer wsr.Lock().Unlock()

	wsr.self = &wsr
	wsr.typespace = typespace
	wsr.namespace = namespace
	wsr.workspace = workspace
	wsr.publishedRootId = rootId
	wsr.accessList = quantumfs.NewPathsAccessed()

	wsr.treeLock_ = &wsr.realTreeLock
	utils.Assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	func() {
		defer wsr.linkLock.Lock().Unlock()
		wsr.hardlinks = loadHardlinks(c,
			workspaceRoot.HardlinkEntry())
		wsr.inodeToLink = make(map[InodeId]HardlinkId)
	}()
	uninstantiated := initDirectory(c, workspace, &wsr.Directory, &wsr,
		workspaceRoot.BaseLayer(), inodeNum, parent.inodeNum(),
		&wsr.realTreeLock)

	return &wsr, uninstantiated
}

func (wsr *WorkspaceRoot) checkHardlink(inodeId InodeId) (isHardlink bool,
	id HardlinkId) {

	defer wsr.linkLock.RLock().RUnlock()
	linkId, exists := wsr.inodeToLink[inodeId]
	if !exists {
		return false, 0
	}

	return true, linkId
}

func (wsr *WorkspaceRoot) dirtyChild(c *ctx, childId InodeId) {
	defer c.funcIn("WorkspaceRoot::dirtyChild").Out()

	isHardlink, _ := wsr.checkHardlink(childId)

	if isHardlink {
		wsr.self.dirty(c)
	} else {
		wsr.Directory.dirtyChild(c, childId)
	}
}

func (wsr *WorkspaceRoot) nlinks(hardlinkId HardlinkId) uint32 {
	defer wsr.linkLock.RLock().RUnlock()

	entry, exists := wsr.hardlinks[hardlinkId]
	if !exists {
		panic(fmt.Sprintf("Invalid hardlinkId in system %d", hardlinkId))
	}

	return entry.nlink
}

func (wsr *WorkspaceRoot) hardlinkInc(linkId HardlinkId) {
	defer wsr.linkLock.Lock().Unlock()

	entry, exists := wsr.hardlinks[linkId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
	}

	// Linking updates ctime
	entry.record.SetContentTime(quantumfs.NewTime(time.Now()))

	entry.nlink++
	wsr.hardlinks[linkId] = entry
}

func (wsr *WorkspaceRoot) hardlinkDec(linkId HardlinkId) bool {
	defer wsr.linkLock.Lock().Unlock()

	entry, exists := wsr.hardlinks[linkId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
	}

	if entry.nlink > 0 {
		entry.nlink--
	} else {
		panic("over decrement in hardlink ref count")
	}

	// Unlinking updates ctime
	entry.record.SetContentTime(quantumfs.NewTime(time.Now()))

	// Normally, nlink should still be at least 1
	if entry.nlink > 0 {
		wsr.hardlinks[linkId] = entry
		return true
	}

	// But via races, it's possible nlink could be zero here, at which point
	// all references to this hardlink are gone and we must remove it
	wsr.removeHardlink_(linkId, entry.inodeId)
	return false
}

// Must hold the linkLock for writing
func (wsr *WorkspaceRoot) removeHardlink_(linkId HardlinkId, inodeId InodeId) {
	delete(wsr.hardlinks, linkId)

	if inodeId != quantumfs.InodeIdInvalid {
		delete(wsr.inodeToLink, inodeId)
	}
}

func (wsr *WorkspaceRoot) newHardlink(c *ctx, fingerprint string, inodeId InodeId,
	record quantumfs.DirectoryRecord) *Hardlink {

	defer c.FuncIn("WorkspaceRoot::newHardlink", "inode %d", inodeId).Out()

	if _, isLink := record.(*Hardlink); isLink {
		panic("newHardlink called on existing hardlink")
	}

	dirRecord, isRecord := record.(*quantumfs.DirectRecord)
	if !isRecord {
		panic("newHardlink called on non-DirectRecord")
	}

	defer wsr.linkLock.Lock().Unlock()

	// Use a random uuid
	newId := HardlinkId(rand.Uint64())

	c.dlog("New Hardlink %d created with inodeId %d", newId, inodeId)
	newEntry := newLinkEntry(dirRecord)
	newEntry.inodeId = inodeId
	// Linking updates ctime
	newEntry.record.SetContentTime(quantumfs.NewTime(time.Now()))
	// The hardlink filename will be the fingerprint of the filename
	// which was the source of the first link operation that created
	// the hardlink. This field is used to support seamless refreshing
	// of a set of files into a set of hardlinks.
	newEntry.record.SetFilename(fingerprint)

	wsr.hardlinks[newId] = newEntry
	wsr.inodeToLink[inodeId] = newId

	// Don't reparent the inode, the caller must do so while holding the inode's
	// parent lock
	wsr.dirty(c)

	return newHardlink(record.Filename(), newId, wsr)
}

func (wsr *WorkspaceRoot) instantiateChild(c *ctx, inodeNum InodeId) (Inode,
	[]InodeId) {

	defer c.FuncIn("WorkspaceRoot::instantiateChild", "inode %d", inodeNum).Out()

	hardlinkRecord := func() *quantumfs.DirectRecord {
		defer wsr.linkLock.RLock().RUnlock()

		id, exists := wsr.inodeToLink[inodeNum]
		if !exists {
			return nil
		}

		c.dlog("Instantiating hardlink %d", id)
		return wsr.hardlinks[id].record
	}()
	if hardlinkRecord != nil {
		return wsr.Directory.recordToChild(c, inodeNum, hardlinkRecord)
	}

	// This isn't a hardlink, so proceed as normal
	return wsr.Directory.instantiateChild(c, inodeNum)
}

func (wsr *WorkspaceRoot) getHardlinkInodeId(c *ctx, linkId HardlinkId) InodeId {
	defer c.FuncIn("WorkspaceRoot::getHardlinkInodeId", "linkId %d",
		linkId).Out()
	defer wsr.linkLock.Lock().Unlock()

	// Ensure the linkId is valid
	hardlink, exists := wsr.hardlinks[linkId]
	if !exists {
		// It should be possible, via races, that someone could check
		// on a link which has *just* been deleted
		return quantumfs.InodeIdInvalid
	}

	if hardlink.inodeId != quantumfs.InodeIdInvalid {
		return hardlink.inodeId
	}

	// we need to load this Hardlink partially - giving it an inode number
	inodeId := c.qfs.newInodeId()
	hardlink.inodeId = inodeId
	wsr.hardlinks[linkId] = hardlink
	wsr.inodeToLink[inodeId] = linkId

	return inodeId
}

// Ensure we don't return the vanilla record, enclose it in a hardlink wrapper so
// that the wrapper can correctly pick and choose attributes like nlink
func (wsr *WorkspaceRoot) getHardlinkByInode(inodeId InodeId) (valid bool,
	record quantumfs.DirectoryRecord) {

	defer wsr.linkLock.RLock().RUnlock()

	linkId, exists := wsr.inodeToLink[inodeId]
	if !exists {
		return false, nil
	}

	link, exists := wsr.hardlinks[linkId]
	if !exists {
		return false, nil
	}

	return true, newHardlink(link.record.Filename(), linkId, wsr)
}

// Return a snapshot / instance so that it's concurrency safe
func (wsr *WorkspaceRoot) getHardlink(linkId HardlinkId) (valid bool,
	record quantumfs.DirectRecord) {

	defer wsr.linkLock.RLock().RUnlock()

	link, exists := wsr.hardlinks[linkId]
	if exists {
		return true, *(link.record)
	}

	return false, quantumfs.DirectRecord{}
}

func (wsr *WorkspaceRoot) getInodeIdNoInstantiate(c *ctx,
	linkId HardlinkId) InodeId {

	defer c.FuncIn("WorkspaceRoot::getInodeIdNoInstantiate", "linkId %d",
		linkId).Out()
	defer wsr.linkLock.Lock().Unlock()

	hardlink, exists := wsr.hardlinks[linkId]
	if !exists {
		return quantumfs.InodeIdInvalid
	}
	return hardlink.inodeId
}

func (wsr *WorkspaceRoot) updateHardlinkInodeId(c *ctx, linkId HardlinkId,
	inodeId InodeId) {

	defer c.FuncIn("WorkspaceRoot::updateHardlinkInodeId", "%d: %d",
		linkId, inodeId).Out()
	defer wsr.linkLock.Lock().Unlock()

	hardlink, exists := wsr.hardlinks[linkId]
	utils.Assert(exists, "Hardlink id %d does not exist.", linkId)

	utils.Assert(hardlink.inodeId == quantumfs.InodeIdInvalid,
		"Hardlink id %d already has associated inodeid %d",
		linkId, hardlink.inodeId)
	hardlink.inodeId = inodeId
	wsr.hardlinks[linkId] = hardlink
	wsr.inodeToLink[inodeId] = linkId
}

func (wsr *WorkspaceRoot) hardlinkExists(c *ctx, linkId HardlinkId) bool {
	defer wsr.linkLock.Lock().Unlock()

	_, exists := wsr.hardlinks[linkId]
	return exists
}

func (wsr *WorkspaceRoot) removeHardlink(c *ctx,
	linkId HardlinkId) (record quantumfs.DirectoryRecord, inodeId InodeId) {

	defer c.FuncIn("WorkspaceRoot::removeHardlink", "link %d", linkId).Out()

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[linkId]
	if !exists {
		c.vlog("Hardlink id %d does not exist.", link.nlink)
		return nil, quantumfs.InodeIdInvalid
	}

	if link.nlink > 1 {
		// Not ready to remove hardlink yet
		c.vlog("Hardlink count %d, not ready to remove", link.nlink)
		return nil, quantumfs.InodeIdInvalid
	}

	// Unlinking updates ctime
	link.record.SetContentTime(quantumfs.NewTime(time.Now()))

	// our return variables
	inodeId = link.inodeId

	// ensure we have a valid inodeId to return
	if inodeId == quantumfs.InodeIdInvalid {
		// hardlink was never given an inodeId
		inodeId = c.qfs.newInodeId()
	}

	wsr.removeHardlink_(linkId, link.inodeId)
	// we're throwing link away, but be safe and clear its inodeId
	link.inodeId = quantumfs.InodeIdInvalid

	// Do not reparent here. It must be done safety with either the treeLock or
	// the child, parent, and lockedParent locks locked in an UP order

	wsr.dirty(c)

	return link.record, inodeId
}

// We need the wsr lock to cover setting safely
func (wsr *WorkspaceRoot) setHardlink(linkId HardlinkId,
	fnSetter func(dir *quantumfs.DirectRecord)) {

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[linkId]
	utils.Assert(exists, fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))

	// It's critical that our lock covers both the fetch and this change
	fnSetter(link.record)
}

func loadHardlinks(c *ctx,
	entry quantumfs.HardlinkEntry) (map[HardlinkId]linkEntry) {

	defer c.funcIn("loadHardlinks").Out()

	hardlinks := make(map[HardlinkId]linkEntry)

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = hardlink.Nlinks()
		id := HardlinkId(hardlink.HardlinkID())
		hardlinks[id] = newLink
	})

	return hardlinks
}

func publishHardlinkMap(c *ctx,
	records map[HardlinkId]linkEntry) *quantumfs.HardlinkEntry {

	defer c.funcIn("publishHardlinkMap").Out()

	entryNum := len(records)
	// entryIdx indexes into the metadata block
	entryNum, baseLayer := quantumfs.NewHardlinkEntry(entryNum)
	nextBaseLayerId := quantumfs.EmptyDirKey
	var err error
	entryIdx := 0
	for hardlinkID, entry := range records {
		record := entry.record
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			baseLayer.SetNumEntries(entryIdx)
			baseLayer.SetNext(nextBaseLayerId)

			buf := newBuffer(c, baseLayer.Bytes(),
				quantumfs.KeyTypeMetadata)

			nextBaseLayerId, err = buf.Key(&c.Ctx)
			if err != nil {
				panic("Failed to upload new baseLayer object")
			}

			entryNum, baseLayer = quantumfs.NewHardlinkEntry(entryNum)
			entryIdx = 0
		}

		newRecord := quantumfs.NewHardlinkRecord()
		newRecord.SetHardlinkID(uint64(hardlinkID))
		newRecord.SetRecord(record)
		newRecord.SetNlinks(entry.nlink)
		baseLayer.SetEntry(entryIdx, newRecord)

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	baseLayer.SetNext(nextBaseLayerId)
	return baseLayer
}

func foreachHardlink(c *ctx, entry quantumfs.HardlinkEntry,
	visitor func(*quantumfs.HardlinkRecord)) {

	for {
		for i := 0; i < entry.NumEntries(); i++ {
			visitor(entry.Entry(i))
		}
		if entry.HasNext() {
			buffer := c.dataStore.Get(&c.Ctx, entry.Next())
			if buffer == nil {
				panic("Missing next HardlinkEntry object")
			}
			entry = buffer.AsHardlinkEntry()
		} else {
			break
		}
	}
}

func (wsr *WorkspaceRoot) handleRemoteHardlink(c *ctx,
	hrc *HardlinkRefreshCtx, hardlink *quantumfs.HardlinkRecord) {

	defer c.funcIn("WorkspaceRoot::handleRemoteHardlink").Out()
	id := HardlinkId(hardlink.HardlinkID())
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
					oldRecord.ID().Text(),
					hardlink.Record().ID().Text())
				utils.Assert(!hardlink.Record().Type().IsImmutable(),
					"An immutable type cannot be reloaded.")
				reload(c, hrc, inode, *hardlink.Record())
			}
		}

		status := c.qfs.invalidateInode(entry.inodeId)
		utils.Assert(status == fuse.OK,
			"invalidating %d failed with %d", entry.inodeId, status)
	}
}

type HardlinkLoadRecord struct {
	parent       *Directory
	remoteRecord *quantumfs.DirectRecord
}

type HardlinkRefreshCtx struct {
	claimedLinks map[HardlinkId]InodeId
	loadRecords  []HardlinkLoadRecord
}

func (wsr *WorkspaceRoot) refreshHardlinks(c *ctx,
	hrc *HardlinkRefreshCtx, entry quantumfs.HardlinkEntry) {

	defer c.funcIn("WorkspaceRoot::refreshHardlinks").Out()
	defer wsr.linkLock.Lock().Unlock()

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		hrc.claimedLinks[HardlinkId(hardlink.HardlinkID())] =
			quantumfs.InodeIdInvalid
		wsr.handleRemoteHardlink(c, hrc, hardlink)
	})
}

func (wsr *WorkspaceRoot) removeStaleHardlinks(c *ctx,
	hrc *HardlinkRefreshCtx, entry quantumfs.HardlinkEntry) {

	defer c.funcIn("WorkspaceRoot::removeStaleHardlinks").Out()
	defer wsr.linkLock.Lock().Unlock()

	for hardlinkId, entry := range wsr.hardlinks {
		if inodeId, exists := hrc.claimedLinks[hardlinkId]; !exists {
			c.vlog("Removing hardlink id %d, inode %d, nlink %d",
				hardlinkId, entry.inodeId, entry.nlink)
			if inode := c.qfs.inodeNoInstantiate(c,
				entry.inodeId); inode != nil {

				inode.orphan(c, entry.record)
			}
			wsr.removeHardlink_(hardlinkId, entry.inodeId)
		} else if inodeId != quantumfs.InodeIdInvalid {
			wsr.removeHardlink_(hardlinkId, entry.inodeId)
		}
	}
}

func (wsr *WorkspaceRoot) refreshTo(c *ctx, rootId quantumfs.ObjectKey) {
	defer c.funcIn("WorkspaceRoot::refreshTo").Out()

	c.vlog("Workspace Refreshing %s/%s/%s rootid: %s -> %s",
		wsr.typespace, wsr.namespace, wsr.workspace,
		wsr.publishedRootId.String(), rootId.String())

	func() {
		defer c.qfs.mutabilityLock.Lock().Unlock()
		utils.Assert(c.qfs.workspaceMutability[wsr.workspace] !=
			workspaceMutable,
			"Cannot handle mutable workspaces yet")
	}()

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	var hrc HardlinkRefreshCtx
	hrc.claimedLinks = make(map[HardlinkId]InodeId, 0)
	hrc.loadRecords = make([]HardlinkLoadRecord, 0)

	wsr.refreshHardlinks(c, &hrc, workspaceRoot.HardlinkEntry())

	var addedUninstantiated, removedUninstantiated []InodeId
	func() {
		defer wsr.LockTree().Unlock()
		addedUninstantiated, removedUninstantiated =
			wsr.refresh_DOWN(c, &hrc, workspaceRoot.BaseLayer())
	}()

	for _, loadRecord := range hrc.loadRecords {
		func() {
			defer wsr.LockTree().Unlock()
			defer loadRecord.parent.childRecordLock.Lock().Unlock()
			loadRecord.parent.loadNewChild_DOWN(c,
				loadRecord.remoteRecord)
		}()
	}

	c.qfs.addUninstantiated(c, addedUninstantiated, wsr.inodeNum())
	c.qfs.removeUninstantiated(c, removedUninstantiated)

	wsr.removeStaleHardlinks(c, &hrc, workspaceRoot.HardlinkEntry())
}

func (wsr *WorkspaceRoot) refresh(c *ctx) {
	defer c.funcIn("WorkspaceRoot::refresh").Out()

	publishedRootId, err := c.workspaceDB.Workspace(&c.Ctx,
		wsr.typespace, wsr.namespace, wsr.workspace)
	utils.Assert(err == nil, "Failed to get rootId of the workspace.")
	wsr.refreshTo(c, publishedRootId)
	wsr.publishedRootId = publishedRootId
}

func publishWorkspaceRoot(c *ctx, baseLayer quantumfs.ObjectKey,
	hardlinks map[HardlinkId]linkEntry) quantumfs.ObjectKey {

	defer c.funcIn("publishWorkspaceRoot").Out()

	workspaceRoot := quantumfs.NewWorkspaceRoot()
	workspaceRoot.SetBaseLayer(baseLayer)
	workspaceRoot.SetHardlinkEntry(publishHardlinkMap(c, hardlinks))

	bytes := workspaceRoot.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newRootId, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new workspace root")
	}

	return newRootId
}

func (wsr *WorkspaceRoot) publish(c *ctx) {
	defer c.funcIn("WorkspaceRoot::publish").Out()

	wsr.lock.RLock()
	defer wsr.lock.RUnlock()
	// Ensure wsr lock is held because wsr.hardlinks needs to be protected
	defer wsr.linkLock.RLock().RUnlock()

	// Upload the workspaceroot object
	newRootId := publishWorkspaceRoot(c, wsr.baseLayerId, wsr.hardlinks)

	// Update workspace rootId
	if !newRootId.IsEqualTo(wsr.publishedRootId) {
		rootId, err := c.workspaceDB.AdvanceWorkspace(&c.Ctx, wsr.typespace,
			wsr.namespace, wsr.workspace, wsr.publishedRootId, newRootId)

		if err != nil {
			workspacePath := wsr.typespace + "/" + wsr.namespace + "/" +
				wsr.workspace

			c.wlog("rootID update failure, wsdb %s, new %s, wsr %s: %s",
				rootId.Text(), newRootId.Text(),
				wsr.publishedRootId.Text(), err.Error())
			c.wlog("Another quantumfs instance is writing to %s, %s",
				workspacePath,
				"your changes will be lost. "+
					"Unable to sync to datastore - save your"+
					" work somewhere else.")

			// Lock the user out of the workspace
			defer c.qfs.mutabilityLock.Lock().Unlock()
			c.qfs.workspaceMutability[workspacePath] = 0 +
				workspaceImmutableUntilRestart

			return
		}

		c.dlog("Advanced rootId %s -> %s", wsr.publishedRootId.Text(),
			rootId.Text())
		wsr.publishedRootId = rootId
	}
}

func (wsr *WorkspaceRoot) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("WorkspaceRoot::getChildSnapshot").Out()

	children := wsr.Directory.getChildSnapshot(c)

	// Fill in correct data for .
	wsr.fillWorkspaceAttrReal(c, &children[0].attr)
	children[0].fuseType = children[0].attr.Mode

	// Fill in correct data for ..
	func() {
		defer wsr.getParentLock().RLock().RUnlock()
		fillNamespaceAttr(c, &children[1].attr, wsr.parentId_(),
			wsr.typespace, wsr.namespace)
		children[1].fuseType = children[1].attr.Mode
	}()

	api := directoryContents{
		filename: quantumfs.ApiPath,
		fuseType: fuse.S_IFREG,
	}
	fillApiAttr(c, &api.attr)
	children = append(children, api)

	return children
}

func (wsr *WorkspaceRoot) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("WorkspaceRoot::Lookup", "name %s", name).Out()

	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		fillEntryOutCacheData(c, out)
		fillApiAttr(c, &out.Attr)
		return fuse.OK
	}

	return wsr.Directory.Lookup(c, name, out)
}

func (wsr *WorkspaceRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	defer c.funcIn("WorkspaceRoot::syncChild").Out()

	isHardlink, linkId := wsr.checkHardlink(inodeNum)

	if isHardlink {
		func() {
			defer wsr.Lock().Unlock()
			wsr.self.dirty(c)

			defer wsr.linkLock.Lock().Unlock()
			entry := wsr.hardlinks[linkId].record
			if entry == nil {
				panic("isHardlink but hardlink not set")
			}

			entry.SetID(newKey)
		}()
	} else {
		wsr.Directory.syncChild(c, inodeNum, newKey)
	}
}

func (wsr *WorkspaceRoot) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.funcIn("WorkspaceRoot::Access").Out()

	// WorkspaceRoot always allows access
	return fuse.OK
}

func (wsr *WorkspaceRoot) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("WorkspaceRoot::GetAttr").Out()
	defer wsr.RLock().RUnlock()

	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	wsr.fillWorkspaceAttrReal(c, &out.Attr)
	return fuse.OK
}

func (wsr *WorkspaceRoot) fillWorkspaceAttrReal(c *ctx, attr *fuse.Attr) {
	var numChildDirectories uint32
	defer wsr.childRecordLock.Lock().Unlock()
	for _, entry := range wsr.children.records() {
		if entry.Type() == quantumfs.ObjectTypeDirectory {
			numChildDirectories++
		}
	}
	fillAttr(attr, wsr.InodeCommon.id, numChildDirectories)

	attr.Mode = 0777 | fuse.S_IFDIR
}

func (wsr *WorkspaceRoot) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {
	defer c.FuncIn("WorkspaceRoot::markAccessed",
		"path %s CRUD %x", path, op).Out()

	utils.Assert(!utils.BitFlagsSet(uint(op),
		quantumfs.PathCreated|quantumfs.PathDeleted),
		"Cannot create and delete simultaneously")

	wsr.listLock.Lock()
	defer wsr.listLock.Unlock()
	path = "/" + path
	pathFlags, exists := wsr.accessList.Paths[path]
	if !exists {
		c.vlog("Creating new entry")
		wsr.accessList.Paths[path] = op
		return

	}

	c.vlog("Updating existing entry: %x", pathFlags)

	pathFlags |= op & (quantumfs.PathRead | quantumfs.PathUpdated)

	if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathCreated) &&
		utils.BitFlagsSet(uint(op), quantumfs.PathDeleted) {

		// Entries which were created and are then subsequently
		// deleted are removed from the accessed list under the
		// assumption they are temporary files and of no interest.
		c.vlog("Nullifying entry")
		delete(wsr.accessList.Paths, path)
		return
	} else if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathDeleted) &&
		utils.BitFlagsSet(uint(op), quantumfs.PathCreated) {

		if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathIsDir) {
			// Directories which are deleted and then recreated are not
			// recorded as either created or deleted. However, if it was
			// read, that is maintained.
			if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathRead) {
				c.vlog("Keeping delete->create directory as read")
				pathFlags = pathFlags &^ quantumfs.PathDeleted
			} else {
				c.vlog("Unmarking directory with delete->create")
				delete(wsr.accessList.Paths, path)
				return
			}
		} else {
			// Files which are deleted then recreated are recorded as
			// being neither deleted nor created, but instead truncated
			// (updated). This simplifies the case where some program
			// unlinked and then created/moved a file into place.
			c.vlog("Removing deleted from recreated file")
			pathFlags = pathFlags &^ quantumfs.PathDeleted
			pathFlags = quantumfs.PathUpdated
		}
	} else {
		// Here we only have a delete or create and simply record them.
		pathFlags |= op & (quantumfs.PathCreated | quantumfs.PathDeleted)
	}
	wsr.accessList.Paths[path] = pathFlags
}

func (wsr *WorkspaceRoot) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	c.vlog("WorkspaceRoot::markSelfAccessed doing nothing")
}

func (wsr *WorkspaceRoot) getList() quantumfs.PathsAccessed {
	wsr.listLock.Lock()
	defer wsr.listLock.Unlock()
	return wsr.accessList
}

func (wsr *WorkspaceRoot) clearList() {
	wsr.listLock.Lock()
	defer wsr.listLock.Unlock()
	wsr.accessList = quantumfs.NewPathsAccessed()
}

func (wsr *WorkspaceRoot) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("WorkspaceRoot::flush").Out()

	wsr.Directory.flush(c)
	wsr.publish(c)
	return wsr.publishedRootId
}

func (wsr *WorkspaceRoot) directChildInodes() []InodeId {
	defer wsr.Lock().Unlock()

	directChildren := wsr.Directory.directChildInodes()

	for inodeNum, _ := range wsr.inodeToLink {
		directChildren = append(directChildren, inodeNum)
	}

	return directChildren
}
