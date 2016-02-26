// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package main

import "github.com/hanwen/go-fuse/fuse"

// Inode represents a specific path in the tree which updates as the tree itself
// changes.
type Inode interface {
	GetAttr(out *fuse.AttrOut) (result fuse.Status)
	OpenDir(flags uint32, mode uint32, out *fuse.OpenOut) (result fuse.Status)
	Lookup(name string, out *fuse.EntryOut) fuse.Status
}

type InodeCommon struct {
	id uint64
}

// FileHandle represents a specific path at a specific point in time, even as the
// tree changes underneath it. This is used to provide consistent snapshot views into
// the tree.
type FileHandle interface {
	ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status
}

type FileHandleCommon struct {
	id       uint64
	inodeNum uint64
}
