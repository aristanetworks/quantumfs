// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// WorkspaceRoot acts similarly to a directory except only a single object ID is used
// instead of one for each layer and that ID is directly requested from the
// WorkspaceDB instead of passed in from the parent.
type WorkspaceRoot struct {
	Directory
	typespace       string
	namespace       string
	workspace       string
	publishedRootId quantumfs.ObjectKey
	nonce           quantumfs.WorkspaceNonce

	accessList *accessList

	// The RWMutex which backs the treeLock for all the inodes in this workspace
	// tree.
	realTreeLock sync.RWMutex

	// Hardlink support structures
	linkLock    utils.DeferableRwMutex
	hardlinks   map[quantumfs.FileId]linkEntry
	inodeToLink map[InodeId]quantumfs.FileId
}

type linkEntry struct {
	record  quantumfs.DirectoryRecord
	nlink   uint32
	inodeId InodeId
	paths   []string
}

type HardlinkTable interface {
	getHardlinkByInode(inodeId InodeId) (bool, quantumfs.DirectoryRecord)
	checkHardlink(inodeId InodeId) (bool, quantumfs.FileId)
	instantiateHardlink(c *ctx, inodeNum InodeId) Inode
	markHardlinkPath(c *ctx, path string, fileId quantumfs.FileId)
	findHardlinkInodeId(c *ctx, fileId quantumfs.FileId, inodeId InodeId) InodeId
	removeHardlink(c *ctx,
		fileId quantumfs.FileId) (record quantumfs.DirectoryRecord,
		inodeId InodeId)
	hardlinkExists(c *ctx, fileId quantumfs.FileId) bool
	hardlinkDec(fileId quantumfs.FileId) bool
	hardlinkInc(fileId quantumfs.FileId)
	newHardlink(c *ctx, inodeId InodeId,
		record quantumfs.DirectoryRecord) *Hardlink
	getHardlink(fileId quantumfs.FileId) (valid bool,
		record quantumfs.DirectoryRecord)
	updateHardlinkInodeId(c *ctx, fileId quantumfs.FileId, inodeId InodeId)
	setHardlink(fileId quantumfs.FileId,
		fnSetter func(dir quantumfs.DirectoryRecord))
	nlinks(fileId quantumfs.FileId) uint32
	claimAsChild_(inode Inode)
	getWorkspaceRoot() *WorkspaceRoot
}

func newLinkEntry(record_ quantumfs.DirectoryRecord) linkEntry {
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

	defer c.FuncIn("fillWorkspaceAttrFake", "inode %d %s/%s",
		inodeNum, typespace, namespace).Out()

	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(c *ctx, typespace string, namespace string, workspace string,
	parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	workspaceName := typespace + "/" + namespace + "/" + workspace

	defer c.FuncIn("WorkspaceRoot::newWorkspaceRoot", "%s", workspaceName).Out()

	var wsr WorkspaceRoot

	rootId, nonce, err := c.workspaceDB.FetchAndSubscribeWorkspace(&c.Ctx,
		typespace, namespace, workspace)
	if err != nil {
		c.vlog("workspace %s is removed remotely", workspaceName)
		return nil, nil
	}

	c.vlog("Workspace Loading %s %s", workspaceName, rootId.String())

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	defer wsr.Lock().Unlock()

	wsr.self = &wsr
	wsr.typespace = typespace
	wsr.namespace = namespace
	wsr.workspace = workspace
	wsr.publishedRootId = rootId
	wsr.nonce = nonce
	wsr.accessList = NewAccessList()

	treeLock := TreeLock{lock: &wsr.realTreeLock,
		name: typespace + "/" + namespace + "/" + workspace}
	wsr.treeLock_ = &treeLock
	utils.Assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	func() {
		defer wsr.linkLock.Lock().Unlock()
		wsr.hardlinks = loadHardlinks(c,
			workspaceRoot.HardlinkEntry())
		wsr.inodeToLink = make(map[InodeId]quantumfs.FileId)
	}()
	uninstantiated := initDirectory(c, workspace, &wsr.Directory, &wsr,
		workspaceRoot.BaseLayer(), inodeNum, parent.inodeNum(),
		&treeLock)

	return &wsr, uninstantiated
}

func (wsr *WorkspaceRoot) checkHardlink(inodeId InodeId) (bool, quantumfs.FileId) {
	defer wsr.linkLock.RLock().RUnlock()
	fileId, exists := wsr.inodeToLink[inodeId]
	if !exists {
		return false, quantumfs.InvalidFileId
	}

	return true, fileId
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

// Must be called with inode's parentLock locked for writing
func (wsr *WorkspaceRoot) claimAsChild_(inode Inode) {
	inode.setParent_(wsr.inodeNum())
}

func (wsr *WorkspaceRoot) getWorkspaceRoot() *WorkspaceRoot {
	return wsr
}

func (wsr *WorkspaceRoot) nlinks(fileId quantumfs.FileId) uint32 {
	defer wsr.linkLock.RLock().RUnlock()

	entry, exists := wsr.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Invalid fileId in system %d", fileId))
	}

	return entry.nlink
}

func (wsr *WorkspaceRoot) hardlinkInc(fileId quantumfs.FileId) {
	defer wsr.linkLock.Lock().Unlock()

	entry, exists := wsr.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))
	}

	// Linking updates ctime
	entry.record.SetContentTime(quantumfs.NewTime(time.Now()))

	entry.nlink++
	wsr.hardlinks[fileId] = entry
}

func (wsr *WorkspaceRoot) hardlinkDec(fileId quantumfs.FileId) bool {
	defer wsr.linkLock.Lock().Unlock()

	entry, exists := wsr.hardlinks[fileId]
	if !exists {
		panic(fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))
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
		wsr.hardlinks[fileId] = entry
		return true
	}

	// But via races, it's possible nlink could be zero here, at which point
	// all references to this hardlink are gone and we must remove it
	wsr.removeHardlink_(fileId, entry.inodeId)
	return false
}

// Must hold the linkLock for writing
func (wsr *WorkspaceRoot) removeHardlink_(fileId quantumfs.FileId, inodeId InodeId) {
	delete(wsr.hardlinks, fileId)

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

	defer wsr.linkLock.Lock().Unlock()

	newEntry := newLinkEntry(record)
	newEntry.inodeId = inodeId
	// Linking updates ctime
	newEntry.record.SetContentTime(quantumfs.NewTime(time.Now()))
	newEntry.record.SetFilename("")

	fileId := record.FileId()
	utils.Assert(fileId != quantumfs.InvalidFileId, "invalid fileId")
	wsr.hardlinks[fileId] = newEntry
	wsr.inodeToLink[inodeId] = fileId

	// Don't reparent the inode, the caller must do so while holding the inode's
	// parent lock
	wsr.dirty(c)

	return newHardlink(record.Filename(), fileId, quantumfs.NewTime(time.Now()),
		wsr)
}

func (wsr *WorkspaceRoot) instantiateHardlink(c *ctx, inodeId InodeId) Inode {
	defer c.FuncIn("WorkspaceRoot::instantiateHardlink",
		"inode %d", inodeId).Out()

	hardlinkRecord := func() quantumfs.DirectoryRecord {
		defer wsr.linkLock.RLock().RUnlock()

		id, exists := wsr.inodeToLink[inodeId]
		if !exists {
			return nil
		}

		c.dlog("Instantiating hardlink %d", id)
		return wsr.hardlinks[id].record
	}()
	if hardlinkRecord == nil {
		return nil
	}
	if inode := c.qfs.inodeNoInstantiate(c, inodeId); inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeId)
		return inode
	}
	inode, _ := wsr.Directory.recordToChild(c, inodeId, hardlinkRecord)
	return inode
}

func (wsr *WorkspaceRoot) instantiateChild(c *ctx, inodeId InodeId) (Inode,
	[]InodeId) {

	defer c.FuncIn("WorkspaceRoot::instantiateChild", "inode %d", inodeId).Out()

	inode := wsr.instantiateHardlink(c, inodeId)
	if inode != nil {
		return inode, nil
	}
	// This isn't a hardlink, so proceed as normal
	return wsr.Directory.instantiateChild(c, inodeId)
}

func (wsr *WorkspaceRoot) findHardlinkInodeId(c *ctx,
	fileId quantumfs.FileId, inodeId InodeId) InodeId {

	defer c.FuncIn("WorkspaceRoot::findHardlinkInodeId", "%d inode %d",
		fileId, inodeId).Out()
	defer wsr.linkLock.Lock().Unlock()

	hardlink, exists := wsr.hardlinks[fileId]
	if !exists {
		return inodeId
	}
	if hardlink.inodeId != quantumfs.InodeIdInvalid {
		if inodeId != quantumfs.InodeIdInvalid {
			utils.Assert(inodeId == hardlink.inodeId,
				"requested hardlink inodeId %d exists as %d",
				inodeId, hardlink.inodeId)
		}
		return hardlink.inodeId
	}

	if inodeId != quantumfs.InodeIdInvalid {
		return inodeId
	}

	inodeId = c.qfs.newInodeId()
	hardlink.inodeId = inodeId
	wsr.hardlinks[fileId] = hardlink
	wsr.inodeToLink[inodeId] = fileId

	return inodeId
}

// Ensure we don't return the vanilla record, enclose it in a hardlink wrapper so
// that the wrapper can correctly pick and choose attributes like nlink
func (wsr *WorkspaceRoot) getHardlinkByInode(inodeId InodeId) (valid bool,
	record quantumfs.DirectoryRecord) {

	defer wsr.linkLock.RLock().RUnlock()

	fileId, exists := wsr.inodeToLink[inodeId]
	if !exists {
		return false, nil
	}

	link, exists := wsr.hardlinks[fileId]
	if !exists {
		return false, nil
	}

	return true, newHardlink(link.record.Filename(), fileId, quantumfs.Time(0),
		wsr)
}

func (wsr *WorkspaceRoot) getHardlink(fileId quantumfs.FileId) (valid bool,
	record quantumfs.DirectoryRecord) {

	defer wsr.linkLock.RLock().RUnlock()

	link, exists := wsr.hardlinks[fileId]
	if exists {
		return true, link.record
	}

	return false, nil
}

func (wsr *WorkspaceRoot) getInodeIdNoInstantiate(c *ctx,
	fileId quantumfs.FileId) InodeId {

	defer c.FuncIn("WorkspaceRoot::getInodeIdNoInstantiate", "fileId %d",
		fileId).Out()
	defer wsr.linkLock.Lock().Unlock()

	hardlink, exists := wsr.hardlinks[fileId]
	if !exists {
		return quantumfs.InodeIdInvalid
	}
	return hardlink.inodeId
}

func (wsr *WorkspaceRoot) updateHardlinkInodeId(c *ctx, fileId quantumfs.FileId,
	inodeId InodeId) {

	defer c.FuncIn("WorkspaceRoot::updateHardlinkInodeId", "%d: %d",
		fileId, inodeId).Out()
	defer wsr.linkLock.Lock().Unlock()

	hardlink, exists := wsr.hardlinks[fileId]
	utils.Assert(exists, "Hardlink id %d does not exist.", fileId)

	utils.Assert(hardlink.inodeId == quantumfs.InodeIdInvalid,
		"Hardlink id %d already has associated inodeid %d",
		fileId, hardlink.inodeId)
	hardlink.inodeId = inodeId
	wsr.hardlinks[fileId] = hardlink
	wsr.inodeToLink[inodeId] = fileId
}

func (wsr *WorkspaceRoot) hardlinkExists(c *ctx, fileId quantumfs.FileId) bool {
	defer wsr.linkLock.Lock().Unlock()

	_, exists := wsr.hardlinks[fileId]
	return exists
}

func (wsr *WorkspaceRoot) removeHardlink(c *ctx,
	fileId quantumfs.FileId) (record quantumfs.DirectoryRecord,
	inodeId InodeId) {

	defer c.FuncIn("WorkspaceRoot::removeHardlink", "link %d", fileId).Out()

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[fileId]
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

	wsr.removeHardlink_(fileId, link.inodeId)
	// we're throwing link away, but be safe and clear its inodeId
	link.inodeId = quantumfs.InodeIdInvalid
	wsr.dirty(c)

	return link.record, inodeId
}

// We need the wsr lock to cover setting safely
func (wsr *WorkspaceRoot) setHardlink(fileId quantumfs.FileId,
	fnSetter func(dir quantumfs.DirectoryRecord)) {

	defer wsr.linkLock.Lock().Unlock()

	link, exists := wsr.hardlinks[fileId]
	utils.Assert(exists, fmt.Sprintf("Hardlink fetch on invalid ID %d", fileId))

	// It's critical that our lock covers both the fetch and this change
	fnSetter(link.record)
}

func loadHardlinks(c *ctx,
	entry quantumfs.HardlinkEntry) map[quantumfs.FileId]linkEntry {

	defer c.funcIn("loadHardlinks").Out()

	hardlinks := make(map[quantumfs.FileId]linkEntry)

	foreachHardlink(c, entry, func(hardlink *quantumfs.HardlinkRecord) {
		newLink := newLinkEntry(hardlink.Record())
		newLink.nlink = hardlink.Nlinks()
		id := quantumfs.FileId(hardlink.FileId())
		hardlinks[id] = newLink
	})

	return hardlinks
}

func publishHardlinkMap(c *ctx,
	records map[quantumfs.FileId]linkEntry) *quantumfs.HardlinkEntry {

	defer c.funcIn("publishHardlinkMap").Out()

	entryNum := len(records)
	// entryIdx indexes into the metadata block
	entryNum, baseLayer := quantumfs.NewHardlinkEntry(entryNum)
	nextBaseLayerId := quantumfs.EmptyDirKey
	var err error
	entryIdx := 0

	// Sort the records by fileId so that the derieved ObjectKey is constant
	// irrespective of the order of the records in the map
	keys := make([]quantumfs.FileId, 0, len(records))
	for fileId := range records {
		keys = append(keys, fileId)
	}
	sort.Slice(keys,
		func(i, j int) bool {
			return keys[i] < keys[j]
		})
	for _, fileId := range keys {
		entry := records[fileId]
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
		newRecord.SetFileId(uint64(fileId))
		newRecord.SetRecord(record.(*quantumfs.DirectRecord))
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

// Workspace must be synced first, with the tree locked exclusively across both the
// sync and this refresh
func (wsr *WorkspaceRoot) refresh_(c *ctx) {
	defer c.funcIn("WorkspaceRoot::refresh_").Out()

	publishedRootId, nonce, err := c.workspaceDB.Workspace(&c.Ctx,
		wsr.typespace, wsr.namespace, wsr.workspace)
	utils.Assert(err == nil, "Failed to get rootId of the workspace.")
	workspaceName := wsr.fullname()
	if nonce != wsr.nonce {
		c.dlog("Not refreshing workspace %s due to mismatching "+
			"nonces %d vs %d", workspaceName, wsr.nonce, nonce)
		return
	}

	if wsr.publishedRootId.IsEqualTo(publishedRootId) {
		c.dlog("Not refreshing workspace %s as there has been no updates",
			workspaceName)
		return
	}

	c.vlog("Workspace Refreshing %s rootid: %s -> %s", workspaceName,
		wsr.publishedRootId.String(), publishedRootId.String())

	wsr.refreshTo_(c, publishedRootId)
	wsr.publishedRootId = publishedRootId
}

func publishWorkspaceRoot(c *ctx, baseLayer quantumfs.ObjectKey,
	hardlinks map[quantumfs.FileId]linkEntry) quantumfs.ObjectKey {

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

	c.vlog("Publish: %s", newRootId.String())
	return newRootId
}

func handleAdvanceError(c *ctx, wsr *WorkspaceRoot, rootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey, err error) (doneWithWorkspace bool) {

	switch err := err.(type) {
	default:
		c.wlog("Unable to AdvanceWorkspace: %s", err.Error())

		// return so that we can try again later
	case quantumfs.WorkspaceDbErr:
		if err.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND {
			c.vlog("Workspace is deleted. Will not retry flushing.")
			return true
		} else if err.Code == quantumfs.WSDB_OUT_OF_DATE {
			c.vlog("Workspace out of date. Will attempt merge.")
			return false
		} else {
			c.wlog("Unable to AdvanceWorkspace: %s", err.Error())
		}
	}
	return false
}

func (wsr *WorkspaceRoot) publish(c *ctx) bool {
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
			wsr.namespace, wsr.workspace, wsr.nonce, wsr.publishedRootId,
			newRootId)

		if err != nil {
			return handleAdvanceError(c, wsr, rootId, newRootId, err)
		}

		c.dlog("Advanced rootId %s -> %s", wsr.publishedRootId.String(),
			rootId.String())
		wsr.publishedRootId = rootId
	}

	return true
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

func (wsr *WorkspaceRoot) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.vlog("Invalid GetXAttrSize on WorkspaceRoot")
	return 0, fuse.ENODATA
}

func (wsr *WorkspaceRoot) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("Invalid GetXAttrData on WorkspaceRoot")
	return nil, fuse.ENODATA
}

func (wsr *WorkspaceRoot) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("Invalid ListXAttr on WorkspaceRoot")
	return []byte{}, fuse.OK
}

func (wsr *WorkspaceRoot) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("Invalid SetXAttr on WorkspaceRoot")
	return fuse.Status(syscall.ENOSPC)
}

func (wsr *WorkspaceRoot) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("Invalid RemoveXAttr on WorkspaceRoot")
	return fuse.ENODATA
}

func (wsr *WorkspaceRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, newType quantumfs.ObjectType) {

	defer c.funcIn("WorkspaceRoot::syncChild").Out()

	isHardlink, fileId := wsr.checkHardlink(inodeNum)

	if isHardlink {
		func() {
			defer wsr.Lock().Unlock()
			wsr.self.dirty(c)

			defer wsr.linkLock.Lock().Unlock()
			entry := wsr.hardlinks[fileId].record
			if entry == nil {
				panic("isHardlink but hardlink not set")
			}

			entry.SetID(newKey)

			if newType != quantumfs.ObjectTypeInvalid {
				entry.SetType(newType)
			}
		}()
	} else {
		wsr.Directory.syncChild(c, inodeNum, newKey, newType)
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

	wsr.accessList.markAccessed(c, path, op)
}

func (wsr *WorkspaceRoot) markHardlinkAccessed(c *ctx, fileId quantumfs.FileId,
	op quantumfs.PathFlags) {

	defer c.FuncIn("WorkspaceRoot::markHardlinkAccessed", "%d %x",
		fileId, op).Out()

	wsr.accessList.markHardlinkAccessed(c, fileId, op)
}

func (wsr *WorkspaceRoot) markHardlinkPath(c *ctx, path string,
	fileId quantumfs.FileId) {

	defer c.FuncIn("WorkspaceRoot::markHardlinkPath", "%s %d", path,
		fileId).Out()
	defer wsr.linkLock.Lock().Unlock()

	list := make([]string, 0)
	link, exists := wsr.hardlinks[fileId]
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
	wsr.hardlinks[fileId] = link
}

func (wsr *WorkspaceRoot) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	c.vlog("WorkspaceRoot::markSelfAccessed doing nothing")
}

func (wsr *WorkspaceRoot) getList(c *ctx) quantumfs.PathsAccessed {
	defer wsr.linkLock.Lock().Unlock()

	return wsr.accessList.generate(c, wsr.hardlinks)
}

func (wsr *WorkspaceRoot) clearList() {
	wsr.accessList.clear()
}

func (wsr *WorkspaceRoot) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("WorkspaceRoot::flush").Out()
	wsr.Directory.flush(c)
	if !wsr.publish(c) {
		if !wsr.handleFlushFailure_(c) {
			return quantumfs.ZeroKey
		}
	}
	return wsr.publishedRootId
}

// Should be called with the tree locked for read or write
func (wsr *WorkspaceRoot) handleFlushFailure_(c *ctx) bool {
	defer c.funcIn("WorkspaceRoot::handleFlushFailure_").Out()

	// If there is anything in the dirty queue, postpone handling of the
	// failure to the next time flush fails
	if c.qfs.flusher.nQueued(c, wsr.treeLock()) != 1 {
		return true
	}
	return nil == forceMerge(c, wsr)
}

func (wsr *WorkspaceRoot) directChildInodes() []InodeId {
	defer wsr.Lock().Unlock()

	directChildren := wsr.Directory.directChildInodes()

	for inodeNum, _ := range wsr.inodeToLink {
		directChildren = append(directChildren, inodeNum)
	}

	return directChildren
}

func (wsr *WorkspaceRoot) cleanup(c *ctx) {
	defer c.FuncIn("WorkspaceRoot::cleanup", "workspace %s",
		wsr.fullname()).Out()

	c.workspaceDB.UnsubscribeFrom(wsr.fullname())
}

func (wsr *WorkspaceRoot) fullname() string {
	return wsr.typespace + "/" + wsr.namespace + "/" + wsr.workspace
}
