// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

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
	inodeNum InodeId) Inode {

	c.vlog("WorkspaceRoot::newWorkspaceRoot Enter")
	defer c.vlog("WorkspaceRoot::newWorkspaceRoot Exit")

	var wsr WorkspaceRoot

	rootId := c.workspaceDB.Workspace(&c.Ctx, parentName, name)

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	initDirectory(c, name, &wsr.Directory, workspaceRoot.BaseLayer(),
		inodeNum, nil, &wsr.realTreeLock)
	wsr.self = &wsr
	wsr.namespace = parentName
	wsr.workspace = name
	wsr.rootId = rootId
	wsr.accessList = make(map[string]bool)
	assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")

	c.qfs.activateWorkspace(c, wsr.namespace+"/"+wsr.workspace, &wsr)
	return &wsr
}

// Mark this workspace dirty
func (wsr *WorkspaceRoot) dirty(c *ctx) {
	wsr.setDirty(true)
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
			panic("Unexpected workspace rootID update failure")
		}

		c.dlog("Advanced rootId %s -> %s", wsr.rootId.String(),
			rootId.String())
		wsr.rootId = rootId
	}
}

func (wsr *WorkspaceRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("WorkspaceRoot::syncChild Enter")
	defer c.vlog("WorkspaceRoot::syncChild Exit")
	wsr.publish(c)
}

func (wsr *WorkspaceRoot) setAccessList(c *ctx, path string, created bool) {
	if wsr.accessList == nil {
		wsr.accessList = make(map[string]bool)
	}
	wsr.accessList[path] = created
}

func (wsr *WorkspaceRoot) markAccessed(c *ctx, path string, created bool) {
	wsr.setAccessList(c, path, created)
}

func (wsr *WorkspaceRoot) markSelfAccessed(c *ctx, created bool) {
	return
}
