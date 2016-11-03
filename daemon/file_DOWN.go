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
	c.vlog("File::forget_DOWN Enter")
	defer c.vlog("File::forget_DOWN Exit")

	key := fi.accessor.sync(c)
	fi.setDirty(false)
	fi.parent().syncChild(c, fi.InodeCommon.id, key)

	// Remove the inode from the map, ready to be garbage collected. We also
	// re-register ourselves in the uninstantiated inode collection.
	if parent := fi.parent(); parent != fi {
		c.qfs.addUninstantiated(c, []InodeId{fi.InodeCommon.id}, fi.parent())
	}
	c.qfs.setInode(c, fi.id, nil)
}

func (fi *File) flush_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("File::flush_DOWN Enter")
	defer c.vlog("File::flush_DOWN Exit")

	key := fi.accessor.sync(c)
	fi.setDirty(false)
	return key
}

func (fi *File) Sync_DOWN(c *ctx) fuse.Status {
	return fuse.OK
}

func (fd *FileDescriptor) Sync_DOWN(c *ctx) fuse.Status {
	c.vlog("FileDescriptor::Sync_DOWN Enter")
	defer c.vlog("FileDescriptor::Sync_DOWN Exit")

	defer fd.file.Lock().Unlock()
	if fd.file.isDirty() {
		key := fd.file.flush_DOWN(c)
		fd.file.parent().syncChild(c, fd.file.InodeCommon.id, key)
	}

	return fuse.OK
}
