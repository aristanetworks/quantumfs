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
	hardlinks      map[uint64]*quantumfs.DirectoryRecord
	nextHardlinkId uint64
	hardlinkInode  map[uint64]InodeId
	dirtyLinks     map[InodeId]InodeId
}

// Fetching the number of child directories for all the workspaces within a namespace
// is relatively expensive and not terribly useful. Instead fake it and assume a
// normal number here.
func fillWorkspaceAttrFake(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	workspace string) {

	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(c *ctx, parentName string, name string,
	parent Inode, inodeNum InodeId) Inode {

	defer c.funcIn("WorkspaceRoot::newWorkspaceRoot").out()

	var wsr WorkspaceRoot

	rootId, err := c.workspaceDB.Workspace(&c.Ctx, parentName, name)
	assert(err == nil, "BUG: 175630 - handle workspace API errors")
	c.vlog("Workspace Loading %s/%s %s", parentName, name, rootId.String())

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	defer wsr.Lock().Unlock()

	wsr.self = &wsr
	wsr.namespace = parentName
	wsr.workspace = name
	wsr.rootId = rootId
	wsr.accessList = make(map[string]bool)
	wsr.treeLock_ = &wsr.realTreeLock
	assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	wsr.initHardlinks(c, workspaceRoot.HardlinkEntry())
	uninstantiated := initDirectory(c, name, &wsr.Directory, &wsr,
		workspaceRoot.BaseLayer(), inodeNum, parent.inodeNum(),
		&wsr.realTreeLock)

	c.qfs.addUninstantiated(c, uninstantiated, wsr.inodeNum())

	return &wsr
}

// Mark this workspace dirty
func (wsr *WorkspaceRoot) dirty(c *ctx) {
	c.qfs.activateWorkspace(c, wsr.namespace+"/"+wsr.workspace, wsr)
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
	wsr.hardlinks[newId] = dirRecord
	wsr.hardlinkInode[newId] = inodeId

	return newHardlink(newId, wsr)
}

func (wsr *WorkspaceRoot) getHardlinkInodeId(linkId uint64) InodeId {
	defer wsr.linkLock.RLock().RUnlock()

	inode, exists := wsr.hardlinkInode[linkId]
	if exists {
		return inode
	}

	return quantumfs.InodeIdInvalid
}

// Return a snapshot / instance so that it's concurrency safe
func (wsr *WorkspaceRoot) getHardlink(linkId uint64) quantumfs.DirectoryRecord {
	defer wsr.linkLock.RLock().RUnlock()

	link, exists := wsr.hardlinks[linkId]
	if exists {
		return *link
	}

	// This function should only be called from Hardlink objects, meaning the
	// linkId really should never be invalid
	panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
}

// We need the wsr lock to cover setting safely
func (wsr *WorkspaceRoot) setHardlink(linkId uint64,
	fnSetter func(dir *quantumfs.DirectoryRecord)) {

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[linkId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", linkId))
	}

	// It's critical that our lock covers both the fetch and this change
	fnSetter(link)
}

func (wsr *WorkspaceRoot) initHardlinks(c *ctx, entry quantumfs.HardlinkEntry) {
	defer wsr.linkLock.Lock().Unlock()

	wsr.hardlinks = make(map[uint64]*quantumfs.DirectoryRecord)
	wsr.hardlinkInode = make(map[uint64]InodeId)
	wsr.dirtyLinks = make(map[InodeId]InodeId)

	for {
		for i := 0; i < entry.NumEntries(); i++ {
			hardlink := entry.Entry(i)
			wsr.hardlinks[hardlink.HardlinkID()] = hardlink.Record()
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
	records map[uint64]*quantumfs.DirectoryRecord) *quantumfs.HardlinkEntry {

	// entryIdx indexes into the metadata block
	baseLayer := quantumfs.NewHardlinkEntry()
	nextBaseLayerId := quantumfs.EmptyDirKey
	var err error
	entryIdx := 0
	for hardlinkID, record := range records {
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
		newRecord.SetHardlinkID(hardlinkID)
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
		rootId, err := c.workspaceDB.AdvanceWorkspace(&c.Ctx, wsr.namespace,
			wsr.workspace, wsr.rootId, newRootId)

		if err != nil {
			msg := fmt.Sprintf("Unexpected workspace rootID update "+
				"failure, current %s: %s", rootId.String(),
				err.Error())
			panic(msg)
		}

		c.dlog("Advanced rootId %s -> %s", wsr.rootId.String(),
			rootId.String())
		wsr.rootId = rootId
	}
	c.qfs.deactivateWorkspace(c, wsr.namespace+"/"+wsr.workspace, wsr)
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
	wsr.Directory.syncChild(c, inodeNum, newKey)
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
