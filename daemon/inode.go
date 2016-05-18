// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package daemon

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

type InodeId uint64

// Inode represents a specific path in the tree which updates as the tree itself
// changes.
type Inode interface {
	GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status
	Lookup(c *ctx, name string,
		out *fuse.EntryOut) fuse.Status

	Open(c *ctx, flags uint32, mode uint32,
		out *fuse.OpenOut) fuse.Status

	OpenDir(c *ctx, flags uint32, mode uint32,
		out *fuse.OpenOut) fuse.Status

	Create(c *ctx, input *fuse.CreateIn, name string,
		out *fuse.CreateOut) fuse.Status

	SetAttr(c *ctx, attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status

	// Methods called by children
	setChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
		out *fuse.AttrOut) fuse.Status

	getChildAttr(c *ctx, inodeNum InodeId, out *fuse.AttrOut) fuse.Status

	dirty(c *ctx) // Mark this Inode dirty
	// Mark this Inode dirty because a child is dirty
	dirtyChild(c *ctx, child Inode)
	isDirty() bool // Is this Inode dirty?

	// Compute a new object key, possibly schedule the sync the object data
	// itself to the datastore
	sync(c *ctx) quantumfs.ObjectKey

	inodeNum() InodeId
}

type InodeCommon struct {
	id     InodeId
	dirty_ bool // True if this Inode or any children are dirty
}

func (inode *InodeCommon) inodeNum() InodeId {
	return inode.id
}

func (inode *InodeCommon) isDirty() bool {
	return inode.dirty_
}

func (inode *InodeCommon) dirtyChild(c *ctx, child Inode) {
	panic("Unsupported dirtyChild() call on leaf Inode")
}

// FileHandle represents a specific path at a specific point in time, even as the
// tree changes underneath it. This is used to provide consistent snapshot views into
// the tree.
type FileHandle interface {
	ReadDirPlus(c *ctx, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status
	Read(c *ctx, offset uint64, size uint32, buf []byte, nonblocking bool) (
		fuse.ReadResult, fuse.Status)

	Write(c *ctx, offset uint64, size uint32, flags uint32, buf []byte) (
		uint32, fuse.Status)
}

type FileHandleId uint64

type FileHandleCommon struct {
	id       FileHandleId
	inodeNum InodeId
}
