// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon
	baseLayerId quantumfs.ObjectKey
}

func newDirectory(baseLayerId quantumfs.ObjectKey, inodeNum uint64) Inode {
	return &Directory{
		InodeCommon: InodeCommon{id: inodeNum},
		baseLayerId: baseLayerId,
	}
}

func (dir *Directory) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	return fuse.ENOSYS
}

func (dir *Directory) Lookup(c *ctx, context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

func (dir *Directory) Open(c *ctx, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (dir *Directory) OpenDir(c *ctx, context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (dir *Directory) Create(c *ctx, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	return fuse.ENOSYS
}

type directoryContents struct {
	filename string
	fuseType uint32 // One of fuse.S_IFDIR, S_IFREG, etc
	attr     fuse.Attr
}

func newDirectorySnapshot(c *ctx, children []directoryContents, inodeNum uint64) *directorySnapshot {
	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:       c.qfs.newFileHandleId(),
			inodeNum: inodeNum,
		},
		children: children,
	}

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children []directoryContents
}

func (ds *directorySnapshot) ReadDirPlus(c *ctx, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("ReadDirPlus directorySnapshot", input, out)
	offset := input.Offset

	// Add .
	if offset == 0 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = ds.FileHandleCommon.inodeNum
		fillEntryOutCacheData(c, details)
		fillRootAttr(c, &details.Attr, ds.FileHandleCommon.inodeNum)
	}
	offset++

	// Add ..
	if offset == 1 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = ds.FileHandleCommon.inodeNum
		fillEntryOutCacheData(c, details)
		fillRootAttr(c, &details.Attr, ds.FileHandleCommon.inodeNum)
	}
	offset++

	processed := 0
	for _, child := range ds.children {
		entry := fuse.DirEntry{
			Mode: child.fuseType,
			Name: child.filename,
		}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			break
		}

		details.NodeId = child.attr.Ino
		fillEntryOutCacheData(c, details)
		details.Attr = child.attr

		processed++
	}

	ds.children = ds.children[processed:]

	return fuse.OK
}

func (ds *directorySnapshot) Read(c *ctx, offset uint64, size uint32, buf []byte, nonblocking bool) (fuse.ReadResult, fuse.Status) {
	fmt.Println("Invalid read on directorySnapshot")
	return nil, fuse.ENOSYS
}

func (ds *directorySnapshot) Write(c *ctx, offset uint64, size uint32, flags uint32, buf []byte) (uint32, fuse.Status) {
	fmt.Println("Invalid write on directorySnapshot")
	return 0, fuse.ENOSYS
}
