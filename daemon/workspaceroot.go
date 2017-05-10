// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

// WorkspaceRoot acts similarly to a directory except only a single object ID is used
// instead of one for each layer and that ID is directly requested from the
// WorkspaceDB instead of passed in from the parent.
type WorkspaceRoot struct {
	Directory
	typespace string
	namespace string
	workspace string
	rootId    quantumfs.ObjectKey

	listLock   sync.Mutex
	accessList map[string]bool

	// The RWMutex which backs the treeLock for all the inodes in this workspace
	// tree.
	realTreeLock sync.RWMutex

	// Hardlink support structures
	linkLock       utils.DeferableRwMutex
	hardlinks      map[HardlinkId]linkEntry
	nextHardlinkId HardlinkId
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
	wsr.rootId = rootId
	wsr.accessList = make(map[string]bool)
	wsr.treeLock_ = &wsr.realTreeLock
	utils.Assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	func() {
		defer wsr.linkLock.Lock().Unlock()
		wsr.hardlinks, wsr.nextHardlinkId = loadHardlinks(c,
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

func (wsr *WorkspaceRoot) newHardlink(c *ctx, inodeId InodeId,
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

	newId := wsr.nextHardlinkId
	wsr.nextHardlinkId++

	c.dlog("New Hardlink %d created with inodeId %d", newId, inodeId)
	newEntry := newLinkEntry(dirRecord)
	newEntry.inodeId = inodeId
	// Linking updates ctime
	newEntry.record.SetContentTime(quantumfs.NewTime(time.Now()))

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
	defer c.FuncIn("WorkspaceRoot::getHardlinkInodeIde", "linkId %d",
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

func (wsr *WorkspaceRoot) removeHardlink(c *ctx,
	linkId HardlinkId) (record quantumfs.DirectoryRecord, inodeId InodeId) {

	defer c.FuncIn("WorkspaceRoot::removeHardlink", "link %d", linkId).Out()

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[linkId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
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
	entry quantumfs.HardlinkEntry) (map[HardlinkId]linkEntry, HardlinkId) {

	defer c.funcIn("loadHardlinks").Out()

	hardlinks := make(map[HardlinkId]linkEntry)
	nextHardlinkId := HardlinkId(0)

	for {
		for i := 0; i < entry.NumEntries(); i++ {
			hardlink := entry.Entry(i)
			newLink := newLinkEntry(hardlink.Record())
			newLink.nlink = hardlink.Nlinks()
			id := HardlinkId(hardlink.HardlinkID())
			hardlinks[id] = newLink

			if id >= nextHardlinkId {
				nextHardlinkId = id + 1
			}
		}

		if !entry.HasNext() {
			break
		}

		buffer := c.dataStore.Get(&c.Ctx, entry.Next())
		if buffer == nil {
			panic("Missing next HardlinkEntry object")
		}

		entry = buffer.AsHardlinkEntry()
	}

	return hardlinks, nextHardlinkId
}

func publishHardlinkMap(c *ctx,
	records map[HardlinkId]linkEntry) *quantumfs.HardlinkEntry {

	defer c.funcIn("publishHardlinkMap").Out()

	// entryIdx indexes into the metadata block
	baseLayer := quantumfs.NewHardlinkEntry()
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

			baseLayer = quantumfs.NewHardlinkEntry()
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
	hardlink *quantumfs.HardlinkRecord) {

	defer c.funcIn("WorkspaceRoot::handleRemoteHardlink").Out()
	id := HardlinkId(hardlink.HardlinkID())
	if entry, exists := wsr.hardlinks[id]; !exists {
		c.vlog("Adding new hardlink entry with id %d", id)
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = hardlink.Nlinks()
		wsr.hardlinks[id] = newLink

		if id >= wsr.nextHardlinkId {
			wsr.nextHardlinkId = id + 1
		}
	} else {
		c.vlog("found mapping %d -> %s (nlink %d vs. %d)", id,
			entry.record.Filename(), hardlink.Nlinks(), entry.nlink)
		entry.nlink = hardlink.Nlinks()
		wsr.hardlinks[id] = entry
	}
}

func (wsr *WorkspaceRoot) refreshHardlinks(c *ctx, entry quantumfs.HardlinkEntry) {
	defer c.funcIn("WorkspaceRoot::refreshHardlinks").Out()
	defer wsr.linkLock.Lock().Unlock()

	remoteEntries := make(map[HardlinkId]bool, 0)

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		remoteEntries[HardlinkId(hardlink.HardlinkID())] = true
		wsr.handleRemoteHardlink(c, hardlink)
	})

	for hardlinkId, entry := range wsr.hardlinks {
		if _, exists := remoteEntries[hardlinkId]; !exists {
			c.vlog("Removing stale hardlink id %d, inode %d, nlink %d",
				hardlinkId, entry.inodeId, entry.nlink)
			wsr.removeHardlink_(hardlinkId, entry.inodeId)
		}
	}
}

func (wsr *WorkspaceRoot) refresh(c *ctx, rootId quantumfs.ObjectKey) {
	defer c.funcIn("WorkspaceRoot::refresh").Out()

	if !rootId.IsEqualTo(wsr.rootId) {
		c.vlog("Workspace Refreshing %s/%s/%s rootid: %s -> %s",
			wsr.typespace, wsr.namespace, wsr.workspace,
			wsr.rootId.String(), rootId.String())

		func() {
			defer c.qfs.mutabilityLock.Lock().Unlock()
			utils.Assert(c.qfs.workspaceMutability[wsr.workspace] !=
				workspaceMutable,
				"Cannot handle mutable workspaces yet")
		}()

		buffer := c.dataStore.Get(&c.Ctx, rootId)
		workspaceRoot := buffer.AsWorkspaceRoot()

		var addedUninstantiated, removedUninstantiated []InodeId
		func() {
			defer wsr.LockTree().Unlock()
			addedUninstantiated, removedUninstantiated =
				wsr.refresh_DOWN(c, workspaceRoot.BaseLayer())
		}()

		c.qfs.addUninstantiated(c, addedUninstantiated, wsr.inodeNum())
		c.qfs.removeUninstantiated(c, removedUninstantiated)

		wsr.refreshHardlinks(c, workspaceRoot.HardlinkEntry())

		wsr.rootId = rootId
	}
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
	if !newRootId.IsEqualTo(wsr.rootId) {
		rootId, err := c.workspaceDB.AdvanceWorkspace(&c.Ctx, wsr.typespace,
			wsr.namespace, wsr.workspace, wsr.rootId, newRootId)

		if err != nil {
			workspacePath := wsr.typespace + "/" + wsr.namespace + "/" +
				wsr.workspace

			c.wlog("rootID update failure, wsdb %s, new %s, wsr %s: %s",
				rootId.Text(), newRootId.Text(),
				wsr.rootId.Text(), err.Error())
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

		c.dlog("Advanced rootId %s -> %s", wsr.rootId.Text(),
			rootId.Text())
		wsr.rootId = rootId
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
	for _, entry := range wsr.children.recordCopies(c) {
		if entry.Type() == quantumfs.ObjectTypeDirectory {
			numChildDirectories++
		}
	}
	fillAttr(attr, wsr.InodeCommon.id, numChildDirectories)

	attr.Mode = 0777 | fuse.S_IFDIR
}

func (wsr *WorkspaceRoot) markAccessed(c *ctx, path string, created bool) {
	defer c.FuncIn("WorkspaceRoot::markAccessed", "path %s created %t", path,
		created).Out()

	wsr.listLock.Lock()
	defer wsr.listLock.Unlock()
	if wsr.accessList == nil {
		wsr.accessList = make(map[string]bool)
	}
	path = "/" + path
	previouslyCreated, exists := wsr.accessList[path]
	if !exists || (!previouslyCreated && created) {
		wsr.accessList[path] = created
	}
}

func (wsr *WorkspaceRoot) markSelfAccessed(c *ctx, created bool) {
	c.vlog("WorkspaceRoot::markSelfAccessed doing nothing")
}

func (wsr *WorkspaceRoot) getList() map[string]bool {
	wsr.listLock.Lock()
	defer wsr.listLock.Unlock()
	return wsr.accessList
}

func (wsr *WorkspaceRoot) clearList() {
	wsr.listLock.Lock()
	defer wsr.listLock.Unlock()
	wsr.accessList = make(map[string]bool)
}

func (wsr *WorkspaceRoot) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("WorkspaceRoot::flush").Out()

	wsr.Directory.flush(c)
	wsr.publish(c)
	return wsr.rootId
}

func (wsr *WorkspaceRoot) directChildInodes(c *ctx) []InodeId {
	defer wsr.Lock().Unlock()

	directChildren := wsr.Directory.directChildInodes(c)

	for inodeNum, _ := range wsr.inodeToLink {
		directChildren = append(directChildren, inodeNum)
	}

	return directChildren
}
