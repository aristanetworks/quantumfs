// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/hanwen/go-fuse/fuse"

// NullWorkspaceRoot is specially designed for _null/null workspaceroot,
// which is supposed to be always empty, therefore prohibit creating
// any types of file or directory. _null/null is
// also the only instance of NullWorkspaceRoot type
type NullWorkspaceRoot struct {
	WorkspaceRoot
}

func newNullWorkspaceRoot(c *ctx, parentName string, name string,
	inodeNum InodeId) Inode {
	nwsr := NullWorkspaceRoot{
		WorkspaceRoot: *(newWorkspaceRoot(c,
			parentName, name, inodeNum).(*WorkspaceRoot)),
	}
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

func (nwsr *NullWorkspaceRoot) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {
	return fuse.EPERM
}

func (nwsr *NullWorkspaceRoot) Link(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {
	return fuse.EPERM
}
