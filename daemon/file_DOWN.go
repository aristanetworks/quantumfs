// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to file.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (fi *File) link_DOWN(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on File")
	return fuse.ENOTDIR
}

func (fi *File) forget_DOWN(c *ctx) {
	defer c.flog("File::forget_DOWN").exit()

	key := fi.accessor.sync(c)
	fi.setDirty(false)
	fi.parent().syncChild(c, fi.InodeCommon.id, key)

	// Remove the inode from the map, ready to be garbage collected
	c.qfs.setInode(c, fi.id, nil)
}

func (fi *File) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	defer c.flog("File::flush_DOWN").exit()

	key := fi.accessor.sync(c)
	fi.setDirty(false)
	return key
}

func (fi *File) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (fd *FileDescriptor) Sync_DOWN(c *ctx) fuse.Status {
	defer c.flog("File::Sync_DOWN").exit()

	defer fd.file.Lock().Unlock()
	if fd.file.isDirty() {
		key := fd.file.flush_DOWN(c)
		fd.file.parent().syncChild(c, fd.file.InodeCommon.id, key)
	}

	return fuse.OK
}
