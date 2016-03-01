// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package main

import "github.com/hanwen/go-fuse/fuse"

// Inode represents a specific path in the tree which updates as the tree itself
// changes.
type Inode interface {
	GetAttr(out *fuse.AttrOut) fuse.Status
	Lookup(name string, out *fuse.EntryOut) fuse.Status
	Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status
	OpenDir(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status
}

type InodeCommon struct {
	id uint64
}

// FileHandle represents a specific path at a specific point in time, even as the
// tree changes underneath it. This is used to provide consistent snapshot views into
// the tree.
type FileHandle interface {
	ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status
	Read(offset uint64, size uint32, buf []byte) (fuse.ReadResult, fuse.Status)
	Write(offset uint64, size uint32, flags uint32, buf []byte) (uint32, fuse.Status)
}

type FileHandleCommon struct {
	id       uint64
	inodeNum uint64
}
