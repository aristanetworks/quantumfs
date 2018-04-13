// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"sort"
	"syscall"

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

	realTreeState *TreeState

	hardlinkTable *HardlinkTableImpl
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
	parent Inode, inodeNum InodeId) Inode {

	workspaceName := typespace + "/" + namespace + "/" + workspace

	defer c.FuncIn("WorkspaceRoot::newWorkspaceRoot", "%s", workspaceName).Out()

	rootId, nonce, err := c.workspaceDB.FetchAndSubscribeWorkspace(&c.Ctx,
		typespace, namespace, workspace)
	if err != nil {
		c.vlog("workspace %s is removed remotely", workspaceName)
		return nil
	}

	c.vlog("Workspace Loading %s %s", workspaceName, rootId.String())

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := MutableCopy(c, buffer).AsWorkspaceRoot()

	wsr := WorkspaceRoot{
		typespace:       typespace,
		namespace:       namespace,
		workspace:       workspace,
		publishedRootId: rootId,
		nonce:           nonce,
		accessList:      NewAccessList(),
		realTreeState: &TreeState{
			name: workspaceName,
		},
	}

	defer wsr.Lock().Unlock()

	wsr.self = &wsr
	wsr.treeState_ = wsr.realTreeState

	utils.Assert(wsr.treeState() != nil, "WorkspaceRoot treeState nil at init")

	wsr.hardlinkTable = newHardlinkTable(c, &wsr, workspaceRoot.HardlinkEntry())
	initDirectory(c, workspace, &wsr.Directory,
		wsr.hardlinkTable, workspaceRoot.BaseLayer(), inodeNum,
		parent.inodeNum(), wsr.treeState())

	c.qfs.flusher.markWorkspaceUndeleted(c, workspaceName, nonce)

	return &wsr
}

func (wsr *WorkspaceRoot) dirtyChild(c *ctx, childId InodeId) {
	defer c.funcIn("WorkspaceRoot::dirtyChild").Out()

	isHardlink, _ := wsr.hardlinkTable.checkHardlink(childId)

	if isHardlink {
		wsr.self.dirty(c)
	} else {
		wsr.Directory.dirtyChild(c, childId)
	}
}

func (wsr *WorkspaceRoot) instantiateChild(c *ctx, inodeId InodeId) Inode {

	defer c.FuncIn("WorkspaceRoot::instantiateChild", "inode %d", inodeId).Out()

	inode := wsr.hardlinkTable.instantiateHardlink(c, inodeId)
	if inode != nil {
		return inode
	}
	// This isn't a hardlink, so proceed as normal
	return wsr.Directory.instantiateChild(c, inodeId)
}

func (wsr *WorkspaceRoot) finishInit(c *ctx) []InodeId {
	return wsr.Directory.finishInit(c)
}

func publishHardlinkMap(c *ctx, pub publishFn,
	records map[quantumfs.FileId]*HardlinkTableEntry) *quantumfs.HardlinkEntry {

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
		record := entry.publishableRecord
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			baseLayer.SetNumEntries(entryIdx)
			baseLayer.SetNext(nextBaseLayerId)

			buf := newBuffer(c, baseLayer.Bytes(),
				quantumfs.KeyTypeMetadata)

			nextBaseLayerId, err = pub(c, buf)
			utils.Assert(err == nil,
				"Failed to upload new baseLayer object: %v", err)

			entryNum, baseLayer = quantumfs.NewHardlinkEntry(entryNum)
			entryIdx = 0
		}

		newRecord := baseLayer.NewEntry()
		newRecord.SetFileId(uint64(fileId))
		newRecord.SetRecord(record)
		newRecord.SetNlinks(uint32(entry.nlink))
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
			entry = MutableCopy(c, buffer).AsHardlinkEntry()
		} else {
			break
		}
	}
}

// Workspace must be synced first, with the tree locked exclusively across both the
// sync and this refresh.

// The caller can opt to create a refresh context and supply it to this function
// to avoid getting it built as part of refresh_() as that would be an expensive
// operation. The caller can also choose to send a nil refresh context to ask it
// to be built as part of refresh.
func (wsr *WorkspaceRoot) refresh_(c *ctx) {
	defer c.funcIn("WorkspaceRoot::refresh_").Out()

	publishedRootId, nonce, err := c.workspaceDB.Workspace(&c.Ctx,
		wsr.typespace, wsr.namespace, wsr.workspace)
	utils.Assert(err == nil, "Failed to get rootId of the workspace.")
	workspaceName := wsr.fullname()
	if !nonce.SameIncarnation(&wsr.nonce) {
		c.dlog("Not refreshing workspace %s due to mismatching "+
			"nonces %s vs %s", workspaceName,
			wsr.nonce.String(), nonce.String())
		return
	}

	if wsr.publishedRootId.IsEqualTo(publishedRootId) {
		c.dlog("Not refreshing workspace %s as there has been no updates",
			workspaceName)
		return
	}

	rc := newRefreshContext_(c, wsr.publishedRootId, publishedRootId)
	c.vlog("Workspace Refreshing %s rootid: %s::%s -> %s::%s", workspaceName,
		wsr.publishedRootId.String(), wsr.nonce.String(),
		publishedRootId.String(), nonce.String())

	wsr.refreshTo_(c, rc)
	wsr.publishedRootId = publishedRootId
}

func publishWorkspaceRoot(c *ctx, baseLayer quantumfs.ObjectKey,
	hardlinks map[quantumfs.FileId]*HardlinkTableEntry,
	pub publishFn) quantumfs.ObjectKey {

	defer c.funcIn("publishWorkspaceRoot").Out()

	workspaceRoot := quantumfs.NewWorkspaceRoot()
	workspaceRoot.SetBaseLayer(baseLayer)
	workspaceRoot.SetHardlinkEntry(publishHardlinkMap(c, pub, hardlinks))

	bytes := workspaceRoot.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newRootId, err := pub(c, buf)
	utils.Assert(err == nil, "Failed to upload new workspace root: %v", err)

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

	defer wsr.RLock().RUnlock()

	wsr.hardlinkTable.apply(c, wsr.Directory.hardlinkDelta)

	// Ensure linklock is held because hardlinkTable needs to be protected
	defer wsr.hardlinkTable.linkLock.RLock().RUnlock()
	// Upload the workspaceroot object
	newRootId := publishWorkspaceRoot(c, wsr.baseLayerId,
		wsr.hardlinkTable.hardlinks, publishNow)

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

	if c.qfs.inLowMemoryMode {
		lowmem := directoryContents{
			filename: quantumfs.LowMemFileName,
			fuseType: fuse.S_IFREG,
		}
		fillLowMemAttr(c, &lowmem.attr)
		children = append(children, lowmem)
	}

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

	if c.qfs.inLowMemoryMode && name == quantumfs.LowMemFileName {
		out.NodeId = quantumfs.InodeIdLowMemMarker
		fillEntryOutCacheData(c, out)
		fillLowMemAttr(c, &out.Attr)
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
	newKey quantumfs.ObjectKey, hardlinkDelta *HardlinkDelta) {

	defer c.funcIn("WorkspaceRoot::syncChild").Out()

	isHardlink, fileId := wsr.hardlinkTable.checkHardlink(inodeNum)

	if isHardlink {
		func() {
			defer wsr.Lock().Unlock()
			wsr.self.dirty(c)
			wsr.hardlinkTable.setID(c, fileId, newKey)
		}()
	} else {
		wsr.Directory.syncChild(c, inodeNum, newKey, nil)
		if hardlinkDelta != nil {
			wsr.hardlinkTable.apply(c, hardlinkDelta)
		}
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

func (wsr *WorkspaceRoot) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	c.vlog("WorkspaceRoot::markSelfAccessed doing nothing")
}

func (wsr *WorkspaceRoot) getList(c *ctx) quantumfs.PathsAccessed {
	defer wsr.hardlinkTable.linkLock.Lock().Unlock()

	accessMap := make(map[quantumfs.FileId][]string,
		len(wsr.hardlinkTable.hardlinks))

	for fileId, entry := range wsr.hardlinkTable.hardlinks {
		accessMap[fileId] = entry.paths
	}

	return wsr.accessList.generate(c, accessMap)
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
	if c.qfs.flusher.nQueued(c, wsr.treeState()) != 1 {
		return true
	}
	return nil == forceMerge(c, wsr)
}

func (wsr *WorkspaceRoot) directChildInodes() []InodeId {
	defer wsr.Lock().Unlock()

	directChildren := wsr.Directory.directChildInodes()

	for inodeNum, _ := range wsr.hardlinkTable.inodeToLink {
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
