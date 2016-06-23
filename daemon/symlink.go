// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the Symlink type, which represents symlinks

import "errors"

import "arista.com/quantumfs"

import "github.com/hanwen/go-fuse/fuse"

func newSymlink(c *ctx, key quantumfs.ObjectKey, size uint64, inodeNum InodeId,
	parent Inode) Inode {

	symlink := Symlink{
		InodeCommon: InodeCommon{
			id:        inodeNum,
			treeLock_: parent.treeLock(),
		},
		key:    key,
		parent: parent,
	}
	symlink.self = &symlink
	assert(symlink.treeLock() != nil, "Symlink treeLock nil at init")
	return &symlink
}

type Symlink struct {
	InodeCommon
	key    quantumfs.ObjectKey
	parent Inode
}

func (link *Symlink) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	return fuse.OK
}

func (link *Symlink) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	record, err := link.parent.getChildRecord(c, link.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", link.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, link.InodeCommon.id,
		c.fuseCtx.Owner, &record)

	return fuse.OK
}

func (link *Symlink) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.elog("Invalid Lookup call on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.elog("Invalid Open call on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOTDIR
}

func (link *Symlink) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	return fuse.ENOTDIR
}

func (link *Symlink) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	return link.parent.setChildAttr(c, link.InodeCommon.id, attr, out)
}

func (link *Symlink) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.ENOTDIR
}

func (link *Symlink) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Readlink(c *ctx) ([]byte, fuse.Status) {
	data := DataStore.Get(c, link.key)
	if data == nil {
		return nil, fuse.EIO
	}

	return data.Get(), fuse.OK
}

func (link *Symlink) setChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid setChildAttr on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) getChildRecord(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	c.elog("Unsupported record fetch on Symlink")
	return quantumfs.DirectoryRecord{}, errors.New("Unsupported record fetch")
}

func (link *Symlink) dirty(c *ctx) {
	link.setDirty(true)
	link.parent.dirtyChild(c, link)
}
