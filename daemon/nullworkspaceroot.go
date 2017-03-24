// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// NullSpaceNameRoot is specially designed for _null/null workspaceroot,
// which is supposed to be always empty, therefore prohibit creating
// any types of file or directory. _null/null is
// also the only instance of NullSpaceNameRoot type
type NullSpaceNameRoot struct {
	WorkspaceRoot
}

func newNullSpaceNameRoot(c *ctx, typespace string, namespace string,
	workspace string, parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	inode, uninstantiated := newWorkspaceRoot(c, typespace, namespace, workspace,
		parent, inodeNum)
	wsr := *(inode.(*WorkspaceRoot))
	nwsr := NullSpaceNameRoot{
		WorkspaceRoot: wsr,
	}
	return &nwsr, uninstantiated
}

func (nwsr *NullSpaceNameRoot) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullSpaceNameRoot) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullSpaceNameRoot) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullSpaceNameRoot) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullSpaceNameRoot) Link(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nwsr *NullSpaceNameRoot) publish(c *ctx) {
	c.vlog("NullSpaceNameRoot::publish")
}

func (nwsr *NullSpaceNameRoot) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("NullSpaceNameRoot::syncChild Enter")
	defer c.vlog("NullSpaceNameRoot::syncChild Exit")
	nwsr.Directory.syncChild(c, inodeNum, newKey)
}

func (nwsr *NullSpaceNameRoot) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("NullSpaceNameRoot::flush").out()

	nwsr.WorkspaceRoot.flush(c)
	return nwsr.rootId
}
