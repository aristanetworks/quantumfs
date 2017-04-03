// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// NullWorkspaceRoot is specially designed for _null/null workspaceroot,
// which is supposed to be always empty, therefore prohibit creating
// any types of file or directory. _null/null is
// also the only instance of NullWorkspaceRoot type
type NullWorkspaceRoot struct {
	WorkspaceRoot
}

func newNullWorkspaceRoot(c *ctx, typespace string, namespace string,
	workspace string, parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	inode, uninstantiated := newWorkspaceRoot(c, typespace, namespace, workspace,
		parent, inodeNum)
	wsr := *(inode.(*WorkspaceRoot))
	nwsr := NullWorkspaceRoot{
		WorkspaceRoot: wsr,
	}
	return &nwsr, uninstantiated
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

func (nwsr *NullWorkspaceRoot) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullWorkspaceRoot) Link(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullWorkspaceRoot) publish(c *ctx) {
	c.vlog("NullWorkspaceRoot::publish")
}

func (nwsr *NullWorkspaceRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	defer c.funcIn("NullWorkspaceRoot::syncChild").out()
	nwsr.Directory.syncChild(c, inodeNum, newKey)
}

func (nwsr *NullWorkspaceRoot) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("NullWorkspaceRoot::flush").out()

	nwsr.WorkspaceRoot.flush(c)
	return nwsr.rootId
}
