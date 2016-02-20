// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The code which handles listing available workspaces as the first two levels of the
// directory hierarchy.
package main

import "fmt"
import "time"

import "github.com/hanwen/go-fuse/fuse"

func NewNamespaceList() *NamespaceList {
	nsl := NamespaceList{
		InodeCommon: InodeCommon{id: fuse.FUSE_ROOT_ID},
	}
	return &nsl
}

type NamespaceList struct {
	InodeCommon
}

func (nsl *NamespaceList) GetAttr(out *fuse.AttrOut) (result fuse.Status) {
	out.AttrValid = 1
	out.AttrValid = 1
	out.Attr.Ino = nsl.InodeCommon.id
	out.Attr.Size = 4096
	out.Attr.Blocks = 1

	now := time.Now()
	out.Attr.Atime = uint64(now.Unix())
	out.Attr.Atimensec = uint32(now.Nanosecond())
	out.Attr.Mtime = uint64(now.Unix())
	out.Attr.Mtimensec = uint32(now.Nanosecond())

	out.Attr.Ctime = 1
	out.Attr.Ctimensec = 1
	out.Attr.Mode = 0555 | fuse.S_IFDIR
	out.Attr.Nlink = 1024
	out.Attr.Owner.Uid = 0
	out.Attr.Owner.Gid = 0
	out.Attr.Blksize = 4096

	fmt.Println(out)

	return fuse.OK
}
