// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

// This file contains all the interaction with the quantumfs API file.

import "fmt"
import "time"

import "github.com/hanwen/go-fuse/fuse"

func NewApiInode() Inode {
	api := ApiInode{
		InodeCommon: InodeCommon{id: inodeIdApi},
	}
	return &api
}

type ApiInode struct {
	InodeCommon
}

func fillApiAttr(attr *fuse.Attr) {
	attr.Ino = inodeIdApi
	attr.Size = 0
	attr.Blocks = 0

	now := time.Now()
	attr.Atime = uint64(now.Unix())
	attr.Atimensec = uint32(now.Nanosecond())
	attr.Mtime = uint64(now.Unix())
	attr.Mtimensec = uint32(now.Nanosecond())

	attr.Ctime = 1
	attr.Ctimensec = 1
	attr.Mode = 0666 | fuse.S_IFREG
	attr.Nlink = 1
	attr.Owner.Uid = 0
	attr.Owner.Gid = 0
	attr.Blksize = 4096
}

func (api *ApiInode) GetAttr(out *fuse.AttrOut) fuse.Status {
	out.AttrValid = config.cacheTimeSeconds
	out.AttrValidNsec = config.cacheTimeNsecs
	fillApiAttr(&out.Attr)
	return fuse.OK
}

func (api *ApiInode) OpenDir(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOTDIR
}

func (api *ApiInode) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	out.OpenFlags = fuse.FOPEN_NONSEEKABLE
	handle := newApiHandle()
	globalQfs.setFileHandle(handle.FileHandleCommon.id, handle)
	out.Fh = handle.FileHandleCommon.id
	return fuse.OK
}

func (api *ApiInode) Lookup(name string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

func newApiHandle() *ApiHandle {
	api := ApiHandle{
		FileHandleCommon: FileHandleCommon{
			id:       globalQfs.newFileHandleId(),
			inodeNum: inodeIdApi,
		},
	}
	return &api
}

// ApiHandle represents the user's interactions with quantumfs and is not necessarily
// synchronized with other api handles.
type ApiHandle struct {
	FileHandleCommon
}

func (api *ApiHandle) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("Invalid ReadDirPlus against ApiHandle")
	return fuse.ENOSYS
}

func (api *ApiHandle) Read(offset uint64, size uint32, buf []byte) (fuse.ReadResult, fuse.Status) {
	return nil, fuse.OK
}
