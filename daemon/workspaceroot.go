// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "encoding/json"
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

	object := c.dataStore.Get(&c.Ctx, rootId)
	var workspaceRoot quantumfs.WorkspaceRoot
	if err := json.Unmarshal(object.Get(), &workspaceRoot); err != nil {
		panic("Couldn't decode WorkspaceRoot Object")
	}

	initDirectory(c, &wsr.Directory, workspaceRoot.BaseLayer, inodeNum, nil,
		&wsr.realTreeLock)
	wsr.self = &wsr
	wsr.namespace = parentName
	wsr.workspace = name
	wsr.rootId = rootId
	assert(wsr.treeLock() != nil, "WorkspaceRoot treeLock nil at init")

	c.qfs.activateWorkspace(c, wsr.namespace+"/"+wsr.workspace, &wsr)
	return &wsr
}

// Mark this workspace dirty
func (wsr *WorkspaceRoot) dirty(c *ctx) {
	wsr.setDirty(true)
}

// If the WorkspaceRoot is dirty recompute the rootId and update the workspacedb
func (wsr *WorkspaceRoot) advanceRootId(c *ctx) {
	c.vlog("WorkspaceRoot::advanceRootId Enter")
	defer c.vlog("WorkspaceRoot::advanceRootId Exit")

	wsr.Directory.sync_DOWN(c)
	wsr.publish(c)
}

func (wsr *WorkspaceRoot) publish(c *ctx) {
	c.vlog("WorkspaceRoot::publish Enter")
	defer c.vlog("WorkspaceRoot::publish Exit")

	// Upload the workspaceroot object
	var workspaceRoot quantumfs.WorkspaceRoot
	workspaceRoot.BaseLayer = wsr.baseLayerId
	bytes, err := json.Marshal(workspaceRoot)
	if err != nil {
		panic("Failed to marshal workspace root")
	}

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

		c.dlog("Advanced rootId %v -> %v", wsr.rootId, rootId)
		wsr.rootId = rootId
	}
}

func (wsr *WorkspaceRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("WorkspaceRoot::syncChild Enter")
	defer c.vlog("WorkspaceRoot::syncChild Exit")
	wsr.publish(c)
}
