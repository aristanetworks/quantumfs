// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the File type, which represents regular files

import "fmt"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func newFile(inodeNum uint64, fileType quantumfs.ObjectType, key quantumfs.ObjectKey) *File {
	file := File{
		InodeCommon: InodeCommon{id: inodeNum},
		fileType:    fileType,
		key:         key,
	}
	return &file
}

type File struct {
	InodeCommon
	fileType quantumfs.ObjectType
	key      quantumfs.ObjectKey
}

func (fi *File) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	return fuse.ENOSYS
}

func (fi *File) OpenDir(c *ctx, context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOTDIR
}

func (fi *File) Open(c *ctx, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (fi *File) Lookup(c *ctx, context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

func (fi *File) Create(c *ctx, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	return fuse.ENOTDIR
}

func (fi *File) SetAttr(c *ctx, attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	fmt.Println("Invalid SetAttr on File")
	return fuse.ENOSYS
}

func newFileDescriptor(file *File, inodeNum uint64, fileHandleId uint64) FileHandle {
	return &FileDescriptor{
		FileHandleCommon: FileHandleCommon{
			id:       fileHandleId,
			inodeNum: inodeNum,
		},
		file: file,
	}
}

type FileDescriptor struct {
	FileHandleCommon
	file *File
}

func (fd *FileDescriptor) ReadDirPlus(c *ctx, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("Invalid ReadDirPlus against FileDescriptor")
	return fuse.ENOSYS
}

func (fd *FileDescriptor) Read(c *ctx, offset uint64, size uint32, buf []byte, nonblocking bool) (fuse.ReadResult, fuse.Status) {
	fmt.Println("Received read request on FileDescriptor")
	return nil, fuse.ENOSYS
}

func (fd *FileDescriptor) Write(c *ctx, offset uint64, size uint32, flags uint32, buf []byte) (uint32, fuse.Status) {
	fmt.Println("Received write request on FileDescriptor")
	return 0, fuse.ENOSYS
}
