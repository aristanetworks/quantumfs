// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode structure
package main

import "github.com/hanwen/go-fuse/fuse"

type Inode interface {
	GetAttr(out *fuse.AttrOut) (result fuse.Status)
}

type InodeCommon struct {
	id uint64
}
