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

	// TODO: Refactor hardlink management into a child class that workspaceroot
	// passes functions through to. This is to ensure that wsr is usable for
	// hardlink management even if it's only a "thin" one used in Merging
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
		inodeNum, typespace, namespace).out()

	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(c *ctx, typespace string, namespace string, workspace string,
	parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("WorkspaceRoot::newWorkspaceRoot", "%s/%s/%s", typespace,
		namespace, workspace).out()

	rootId, err := c.workspaceDB.Workspace(&c.Ctx,
		typespace, namespace, workspace)
	utils.Assert(err == nil, "BUG: 175630 - handle workspace API errors")
	c.vlog("Workspace Loading %s/%s/%s %s",
		typespace, namespace, workspace, rootId.String())

	rtn, uninstantiated := instantiateWorkspaceRoot(c, rootId, parent.inodeNum(),
		inodeNum, workspace)
	rtn.typespace = typespace
	rtn.namespace = namespace
	rtn.workspace = workspace

	return rtn, uninstantiated
}

func instantiateWorkspaceRoot(c *ctx, rootId quantumfs.ObjectKey, parent InodeId,
	inodeNum InodeId, name string) (*WorkspaceRoot, []InodeId) {

	defer c.funcIn("instantiateWorkspaceRoot").out()

	var wsr WorkspaceRoot
	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	defer wsr.Lock().Unlock()

	wsr.self = &wsr
	wsr.rootId = rootId
	wsr.accessList = make(map[string]bool)
	wsr.treeLock_ = &wsr.realTreeLock
	utils.Assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	func () {
		defer wsr.linkLock.Lock().Unlock()
		wsr.hardlinks, wsr.nextHardlinkId = loadHardlinks(c,
			workspaceRoot.HardlinkEntry())
		wsr.inodeToLink = make(map[InodeId]HardlinkId)
	} ()
	uninstantiated := initDirectory(c, name, &wsr.Directory, &wsr,
		workspaceRoot.BaseLayer(), inodeNum, parent,
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
	defer c.funcIn("WorkspaceRoot::dirtyChild").out()

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

	defer c.FuncIn("WorkspaceRoot::newHardlink", "inode %d", inodeId).out()

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

	defer c.FuncIn("WorkspaceRoot::instantiateChild", "inode %d", inodeNum).out()

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
		return recordToInode(c, inodeNum, hardlinkRecord, &wsr.Directory)
	}

	// This isn't a hardlink, so proceed as normal
	return wsr.Directory.instantiateChild(c, inodeNum)
}

func (wsr *WorkspaceRoot) getHardlinkInodeId(c *ctx, linkId HardlinkId) InodeId {
	defer c.FuncIn("WorkspaceRoot::getHardlinkInodeIde", "linkId %d",
		linkId).out()
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

	defer c.FuncIn("WorkspaceRoot::removeHardlink", "link %d", linkId).out()

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

	defer c.funcIn("WorkspaceRoot::loadHardlinks").out()

	hardlinks := make(map[HardlinkId]linkEntry)
	nextHardlinkId := HardlinkId(0)

	for {
		for i := 0; i < entry.NumEntries(); i++ {
			hardlink := entry.Entry(i)
			newLink := newLinkEntry(hardlink.Record())
			newLink.nlink = hardlink.Nlinks()
			id := HardlinkId(hardlink.HardlinkID())
			hardlinks[id] = newLink

			if id >= nextHardlinkId{
				nextHardlinkId = id + 1
			}
		}

		if entry.Next() == quantumfs.EmptyDirKey || entry.NumEntries() == 0 {
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

	defer c.funcIn("publishHardlinkMap").out()

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

func publishWorkspaceRoot(c *ctx, baseLayer quantumfs.ObjectKey,
	hardlinks map[HardlinkId]linkEntry) quantumfs.ObjectKey {

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
	defer c.funcIn("WorkspaceRoot::publish").out()

	wsr.lock.RLock()
	defer wsr.lock.RUnlock()
	// Ensure wsr lock is held because wsr.hardlinks needs to be protected
	defer wsr.linkLock.RLock().RUnlock()

	// Upload the workspaceroot object
	newRootId := publishWorkspaceRoot(c, wsr.rootId, wsr.hardlinks)

	// Update workspace rootId
	if newRootId != wsr.rootId {
		rootId, err := c.workspaceDB.AdvanceWorkspace(&c.Ctx, wsr.typespace,
			wsr.namespace, wsr.workspace, wsr.rootId, newRootId)

		if err != nil {
			workspacePath := wsr.typespace + "/" + wsr.namespace + "/" +
				wsr.workspace

			c.elog("Unable to update: wsdb %s, new %s, wsr %s: %s.",
				rootId.String(), newRootId.String(),
				wsr.rootId.String(), err.Error())
			c.elog("Another quantumfs instance is writing to %s.",
				workspacePath)
			c.elog("Save your work elsewhere or it will be lost.")

			// Lock the user out of the workspace
			defer c.qfs.mutabilityLock.Lock().Unlock()
			c.qfs.workspaceMutability[workspacePath] = 0 +
				workspaceImmutableUntilRestart

			return
		}

		c.dlog("Advanced rootId %s -> %s", wsr.rootId.String(),
			rootId.String())
		wsr.rootId = rootId
	}
}

func (wsr *WorkspaceRoot) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("WorkspaceRoot::getChildSnapshot").out()

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

	defer c.FuncIn("WorkspaceRoot::Lookup", "name %s", name).out()

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

	defer c.funcIn("WorkspaceRoot::syncChild").out()

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

	defer c.funcIn("WorkspaceRoot::Access").out()

	// WorkspaceRoot always allows access
	return fuse.OK
}

func (wsr *WorkspaceRoot) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("WorkspaceRoot::GetAttr").out()
	defer wsr.RLock().RUnlock()

	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	wsr.fillWorkspaceAttrReal(c, &out.Attr)
	return fuse.OK
}

func (wsr *WorkspaceRoot) fillWorkspaceAttrReal(c *ctx, attr *fuse.Attr) {
	var numChildDirectories uint32
	for _, entry := range wsr.children.records() {
		if entry.Type() == quantumfs.ObjectTypeDirectoryEntry {
			numChildDirectories++
		}
	}
	fillAttr(attr, wsr.InodeCommon.id, numChildDirectories)

	attr.Mode = 0777 | fuse.S_IFDIR
}

func (wsr *WorkspaceRoot) markAccessed(c *ctx, path string, created bool) {
	defer c.FuncIn("WorkspaceRoot::markAccessed", "path %s created %t", path,
		created).out()

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
	defer c.funcIn("WorkspaceRoot::flush").out()

	wsr.Directory.flush(c)
	wsr.publish(c)
	return wsr.rootId
}

func (wsr *WorkspaceRoot) directChildInodes() []InodeId {
	defer wsr.Lock().Unlock()

	directChildren := wsr.Directory.directChildInodes()

	for inodeNum, _ := range wsr.inodeToLink {
		directChildren = append(directChildren, inodeNum)
	}

	return directChildren
}

func mergeLink(c *ctx, remote linkEntry, local linkEntry) (rtn linkEntry) {

	rtn = local

	// TODO: Replace with a set of hashes to fix duplicates and miscounts
	if remote.nlink > local.nlink {
		rtn.nlink = remote.nlink
	}

	localModTime := local.record.ModificationTime()
	if remote.record.ModificationTime() > localModTime {
		rtn.record = remote.record
		c.vlog("taking remote record for %s", local.record.Filename())
	} else {
		c.vlog("keeping local record for %s", local.record.Filename())
	}

	return rtn
}

func mergeWorkspaceRoot(c *ctx, base quantumfs.ObjectKey, remote quantumfs.ObjectKey,
	local quantumfs.ObjectKey) quantumfs.ObjectKey {

	defer c.funcIn("mergeWorkspaceRoot").out()

	baseWsr, _ := instantiateWorkspaceRoot(c, base, quantumfs.InodeIdInvalid,
		quantumfs.InodeIdInvalid, "base")
	remoteWsr, _ := instantiateWorkspaceRoot(c, remote,
		quantumfs.InodeIdInvalid, quantumfs.InodeIdInvalid, "remote")
	localWsr, _ := instantiateWorkspaceRoot(c, local, quantumfs.InodeIdInvalid,
		quantumfs.InodeIdInvalid, "local")

	// We assume that local is a newer version of base
	for k, v := range remoteWsr.hardlinks {
		_, baseExists := baseWsr.hardlinks[k]
		localLink, localExists := localWsr.hardlinks[k]

		if localExists {
			localWsr.hardlinks[k] = mergeLink(c, v, localLink)
		} else if !baseExists {
			localWsr.hardlinks[k] = v
		}
	}

	for k, _ := range baseWsr.hardlinks {
		_, remoteExists := remoteWsr.hardlinks[k]

		// We assume that removal takes precedence over modification
		if !remoteExists {
			delete(localWsr.hardlinks, k)
		}
	}

	if !remoteWsr.rootId.IsEqualTo(baseWsr.rootId) {
		if localWsr.rootId.IsEqualTo(baseWsr.rootId) {
			// No conflict, just take remote
			localWsr.rootId = remoteWsr.rootId
		} else {
			// three way merge
			localWsr.rootId = mergeDirectory(c, baseWsr, remoteWsr,
				localWsr, baseWsr.rootId, remoteWsr.rootId,
				localWsr.rootId)
		}
	}

	return publishWorkspaceRoot(c, localWsr.rootId, localWsr.hardlinks)
}
