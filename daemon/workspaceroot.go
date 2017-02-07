// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
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
	linkLock       DeferableRwMutex
	hardlinks      map[HardlinkId]linkEntry
	nextHardlinkId HardlinkId
	inodeToLink    map[InodeId]HardlinkId
	dirtyLinks     map[InodeId]HardlinkId
}

type linkEntry struct {
	record  *quantumfs.DirectoryRecord
	nlink   uint32
	inodeId InodeId
}

func newLinkEntry(record_ *quantumfs.DirectoryRecord) linkEntry {
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

	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(c *ctx, typespace string, namespace string, workspace string,
	parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("WorkspaceRoot::newWorkspaceRoot", "%s/%s/%s", typespace,
		namespace, workspace).out()

	var wsr WorkspaceRoot

	rootId, err := c.workspaceDB.Workspace(&c.Ctx,
		typespace, namespace, workspace)
	assert(err == nil, "BUG: 175630 - handle workspace API errors")
	c.vlog("Workspace Loading %s/%s/%s %s",
		typespace, namespace, workspace, rootId.String())

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
	assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	wsr.initHardlinks(c, workspaceRoot.HardlinkEntry())
	uninstantiated := initDirectory(c, workspace, &wsr.Directory, &wsr,
		workspaceRoot.BaseLayer(), inodeNum, parent.inodeNum(),
		&wsr.realTreeLock)

	return &wsr, uninstantiated
}

func (wsr *WorkspaceRoot) checkHardlink(inodeId InodeId) (isHardlink bool,
	id HardlinkId) {

	defer wsr.linkLock.Lock().Unlock()
	linkId, exists := wsr.inodeToLink[inodeId]
	if !exists {
		return false, 0
	}

	return true, linkId
}

func (wsr *WorkspaceRoot) dirtyChild(c *ctx, childId InodeId) {
	defer c.funcIn("WorkspaceRoot::dirtyChild").out()

	isHardlink, linkId := wsr.checkHardlink(childId)

	if isHardlink {
		defer wsr.linkLock.Lock().Unlock()

		wsr.dirtyLinks[childId] = linkId
		wsr.self.dirty(c)
	} else {
		wsr.Directory.dirtyChild(c, childId)
	}
}

func (wsr *WorkspaceRoot) nlinks(hardlinkId HardlinkId) uint32 {
	defer wsr.linkLock.Lock().Unlock()

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

	entry.nlink++
	wsr.hardlinks[linkId] = entry
}

func (wsr *WorkspaceRoot) hardlinkDec(linkId HardlinkId) {
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

	// Normally, nlink should still be at least 1
	if entry.nlink > 0 {
		wsr.hardlinks[linkId] = entry
		return
	}

	// But via races, it's possible nlink could be zero here, at which point
	// all references to this hardlink are gone and we must remove it
	wsr.removeHardlink_(linkId, entry.inodeId)
}

// Must hold the linkLock for writing
func (wsr *WorkspaceRoot) removeHardlink_(linkId HardlinkId, inodeId InodeId) {
	delete(wsr.hardlinks, linkId)

	if inodeId != quantumfs.InodeIdInvalid {
		delete(wsr.inodeToLink, inodeId)
		delete(wsr.dirtyLinks, inodeId)
	}
}

func (wsr *WorkspaceRoot) newHardlink(c *ctx, inodeId InodeId,
	record DirectoryRecordIf) *Hardlink {

	if _, isLink := record.(*Hardlink); isLink {
		panic("newHardlink called on existing hardlink")
	}

	dirRecord, isRecord := record.(*quantumfs.DirectoryRecord)
	if !isRecord {
		panic("newHardlink called on non-DirectoryRecord")
	}

	defer wsr.linkLock.Lock().Unlock()

	newId := wsr.nextHardlinkId
	wsr.nextHardlinkId++

	c.dlog("New Hardlink %d created with inodeId %d", newId, inodeId)
	newEntry := newLinkEntry(dirRecord)
	newEntry.inodeId = inodeId
	wsr.hardlinks[newId] = newEntry
	wsr.inodeToLink[inodeId] = newId

	// Fix the inode to use the wsr as direct parent
	inode := c.qfs.inodeNoInstantiate(c, inodeId)
	if inode == nil {
		c.qfs.addUninstantiated(c, []InodeId{inodeId}, wsr.inodeNum())
	} else {
		inode.setParent(wsr.inodeNum())
	}

	wsr.dirty(c)

	return newHardlink(record.Filename(), newId, wsr)
}

func (wsr *WorkspaceRoot) instantiateChild(c *ctx, inodeNum InodeId) (Inode,
	[]InodeId) {

	c.vlog("Directory::instantiateChild Enter %d", inodeNum)
	defer c.vlog("Directory::instantiateChild Exit")

	hardlinkRecord := func() *quantumfs.DirectoryRecord {
		defer wsr.linkLock.Lock().Unlock()

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
	defer wsr.linkLock.RLock().RUnlock()

	// Ensure the linkId is valid
	hardlink, exists := wsr.hardlinks[linkId]
	if !exists {
		panic("Invalid hardlinkId in system")
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

// Return a snapshot / instance so that it's concurrency safe
func (wsr *WorkspaceRoot) getHardlink(linkId HardlinkId) quantumfs.DirectoryRecord {
	defer wsr.linkLock.RLock().RUnlock()

	link, exists := wsr.hardlinks[linkId]
	if exists {
		return *(link.record)
	}

	// This function should only be called from Hardlink objects, meaning the
	// linkId really should never be invalid
	panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
}

func (wsr *WorkspaceRoot) removeHardlink(c *ctx, linkId HardlinkId,
	newParent InodeId) (record DirectoryRecordIf, inodeId InodeId,
	wasDirty bool) {

	defer c.FuncIn("Removing Hardlink", "%d for new parent %d", linkId,
		newParent).out()

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[linkId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
	}

	if link.nlink > 1 {
		// Not ready to remove hardlink yet
		return nil, quantumfs.InodeIdInvalid, false
	}

	// our return variables
	inodeId = link.inodeId
	wasDirty = false

	// ensure we have a valid inodeId to return
	if inodeId != quantumfs.InodeIdInvalid {
		_, wasDirty = wsr.dirtyLinks[inodeId]
	} else {
		// hardlink was never given an inodeId
		inodeId = c.qfs.newInodeId()
	}

	wsr.removeHardlink_(linkId, link.inodeId)
	// we're throwing link away, but be safe and clear its inodeId
	link.inodeId = quantumfs.InodeIdInvalid

	inode := c.qfs.inodeNoInstantiate(c, inodeId)
	if inode == nil {
		c.qfs.addUninstantiated(c, []InodeId{inodeId}, newParent)
	} else {
		inode.setParent(newParent)
	}

	wsr.dirty(c)

	return link.record, inodeId, wasDirty
}

// We need the wsr lock to cover setting safely
func (wsr *WorkspaceRoot) setHardlink(linkId HardlinkId,
	fnSetter func(dir *quantumfs.DirectoryRecord)) {

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[linkId]
	assert(exists, fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))

	// It's critical that our lock covers both the fetch and this change
	fnSetter(link.record)
}

func (wsr *WorkspaceRoot) initHardlinks(c *ctx, entry quantumfs.HardlinkEntry) {
	defer wsr.linkLock.Lock().Unlock()

	wsr.hardlinks = make(map[HardlinkId]linkEntry)
	wsr.inodeToLink = make(map[InodeId]HardlinkId)
	wsr.dirtyLinks = make(map[InodeId]HardlinkId)
	wsr.nextHardlinkId = 0

	for {
		for i := 0; i < entry.NumEntries(); i++ {
			hardlink := entry.Entry(i)
			newLink := newLinkEntry(hardlink.Record())
			id := HardlinkId(hardlink.HardlinkID())
			wsr.hardlinks[id] = newLink

			if id >= wsr.nextHardlinkId {
				wsr.nextHardlinkId = id + 1
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
}

func publishHardlinkMap(c *ctx,
	records map[HardlinkId]linkEntry) *quantumfs.HardlinkEntry {

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
		baseLayer.SetEntry(entryIdx, newRecord)

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	baseLayer.SetNext(nextBaseLayerId)
	return baseLayer
}

func (wsr *WorkspaceRoot) publish(c *ctx) {
	c.vlog("WorkspaceRoot::publish Enter")
	defer c.vlog("WorkspaceRoot::publish Exit")

	wsr.lock.RLock()
	defer wsr.lock.RUnlock()
	defer wsr.linkLock.RLock().RUnlock()

	// Upload the workspaceroot object
	workspaceRoot := quantumfs.NewWorkspaceRoot()
	workspaceRoot.SetBaseLayer(wsr.baseLayerId)
	// Ensure wsr lock is held because wsr.hardlinks needs to be protected
	workspaceRoot.SetHardlinkEntry(publishHardlinkMap(c, wsr.hardlinks))

	bytes := workspaceRoot.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newRootId, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new workspace root")
	}

	// Update workspace rootId
	if newRootId != wsr.rootId {
		rootId, err := c.workspaceDB.AdvanceWorkspace(&c.Ctx, wsr.typespace,
			wsr.namespace, wsr.workspace, wsr.rootId, newRootId)

		if err != nil {
			msg := fmt.Sprintf("Unexpected workspace rootID update "+
				"failure, wsdb %s, new %s, wsr %s: %s",
				rootId.String(), newRootId.String(),
				wsr.rootId.String(), err.Error())
			panic(msg)
		}

		c.dlog("Advanced rootId %s -> %s", wsr.rootId.String(),
			rootId.String())
		wsr.rootId = rootId
	}
}

func (wsr *WorkspaceRoot) getChildSnapshot(c *ctx) []directoryContents {
	children := wsr.Directory.getChildSnapshot(c)

	api := directoryContents{
		filename: quantumfs.ApiPath,
		fuseType: fuse.S_IFREG,
	}
	fillApiAttr(&api.attr)
	children = append(children, api)

	return children
}

func (wsr *WorkspaceRoot) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		fillEntryOutCacheData(c, out)
		fillApiAttr(&out.Attr)
		return fuse.OK
	}

	return wsr.Directory.Lookup(c, name, out)
}

func (wsr *WorkspaceRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("WorkspaceRoot::syncChild Enter")
	defer c.vlog("WorkspaceRoot::syncChild Exit")

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

			defer wsr.Directory.childRecordLock.Lock().Unlock()
			wsr.Directory.publish_(c)
		}()
	} else {
		wsr.Directory.syncChild(c, inodeNum, newKey)
	}

	wsr.publish(c)
}

func (wsr *WorkspaceRoot) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	c.vlog("WorkspaceRoot::GetAttr Enter")
	defer c.vlog("WorkspaceRoot::GetAttr Exit")
	defer wsr.RLock().RUnlock()

	var numChildDirectories uint32
	for _, entry := range wsr.children.records() {
		if entry.Type() == quantumfs.ObjectTypeDirectoryEntry {
			numChildDirectories++
		}
	}
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	fillAttr(&out.Attr, wsr.InodeCommon.id, numChildDirectories)

	out.Attr.Mode = 0777 | fuse.S_IFDIR
	return fuse.OK
}

func (wsr *WorkspaceRoot) markAccessed(c *ctx, path string, created bool) {
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
	return
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

func (wsr *WorkspaceRoot) isWorkspaceRoot() bool {
	return true
}
