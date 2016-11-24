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

	rootId := c.workspaceDB.Workspace(&c.Ctx, parentName, name)
	c.vlog("Workspace Loading %s/%s %s", parentName, name, rootId.String())

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	wsr.self = &wsr
	wsr.namespace = parentName
	wsr.workspace = name
	wsr.rootId = rootId
	wsr.accessList = make(map[string]bool)
	wsr.treeLock_ = &wsr.realTreeLock
	assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")
	uninstantiated := initDirectory(c, name, &wsr.Directory,
		workspaceRoot.BaseLayer(), inodeNum, parent, &wsr.realTreeLock)

	c.qfs.addUninstantiated(c, uninstantiated, &wsr)

	return &wsr
}

// Mark this workspace dirty
func (wsr *WorkspaceRoot) dirty(c *ctx) {
	c.qfs.activateWorkspace(c, wsr.namespace+"/"+wsr.workspace, wsr)
}

func (wsr *WorkspaceRoot) publish(c *ctx) {
	c.vlog("WorkspaceRoot::publish Enter")
	defer c.vlog("WorkspaceRoot::publish Exit")

	// Upload the workspaceroot object
	workspaceRoot := quantumfs.NewWorkspaceRoot()
	workspaceRoot.SetBaseLayer(wsr.baseLayerId)
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
	for _, entry := range wsr.childrenRecords {
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
