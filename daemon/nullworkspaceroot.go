// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/hanwen/go-fuse/fuse"

// NullWorkspaceRoot is specially designed for _null/null workspaceroot,
// which is supposed to be always empty, therefore prohibition on creating
// any types of file or directory is enforced in this type. _null/null is
// also the only instance of NullWorkspaceRoot type
type NullWorkspaceRoot struct {
	WorkspaceRoot
}

func newNullWorkspaceRoot(c *ctx, parentName string, name string,
	inodeNum InodeId) Inode {

	c.vlog("NullWorkspaceRoot::newNullWorkspaceRoot Enter")
	defer c.vlog("NullWorkspaceRoot::newNullWorkspaceRoot Exit")

	var nwsr NullWorkspaceRoot

	rootId := c.workspaceDB.Workspace(&c.Ctx, parentName, name)

	buffer := c.dataStore.Get(&c.Ctx, rootId)
	workspaceRoot := buffer.AsWorkspaceRoot()

	initDirectory(c, &nwsr.Directory, workspaceRoot.BaseLayer(), inodeNum, nil,
		&nwsr.realTreeLock)
	nwsr.self = &nwsr
	nwsr.namespace = parentName
	nwsr.workspace = name
	nwsr.rootId = rootId
	assert(nwsr.treeLock() != nil, "NullWorkspaceRoot treeLock nil at init")

	c.qfs.activateWorkspace(c, nwsr.namespace+"/"+nwsr.workspace,
		&(nwsr.WorkspaceRoot))
	return &nwsr
}

func (nwsr *NullWorkspaceRoot) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullWorkspaceRoot) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullWorkspaceRoot) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {
	return fuse.EPERM
}
