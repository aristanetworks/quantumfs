// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the Symlink type, which represents symlinks

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func newSymlink(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newSymlink", "name %s", name).Out()

	symlink := Symlink{
		InodeCommon: InodeCommon{
			id:        inodeNum,
			name_:     name,
			accessed_: 0,
			treeLock_: parent.treeLock(),
		},
		key: key,
	}
	symlink.self = &symlink
	symlink.setParent(parent.inodeNum())
	utils.Assert(symlink.treeLock() != nil, "Symlink treeLock nil at init")

	if dirRecord != nil {
		buf := c.dataStore.Get(&(c.Ctx), key)
		pointedTo := buf.Get()
		size := len(pointedTo)
		dirRecord.SetSize(uint64(size))
		dirRecord.SetPermissions(modeToPermissions(0777, 0))
	}
	return &symlink, nil
}

type Symlink struct {
	InodeCommon
	key quantumfs.ObjectKey
}

func (link *Symlink) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.funcIn("Symlink::Access").Out()
	c.elog("Access called on a symlink - should have been dereferenced.")

	return fuse.OK
}

func (link *Symlink) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("Symlink::GetAttr").Out()
	record, err := link.parentGetChildRecordCopy(c, link.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", link.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, link.InodeCommon.id,
		c.fuseCtx.Owner, record)

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

	c.vlog("Symlink::OpenDir doing nothing")
	return fuse.ENOTDIR
}

func (link *Symlink) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("Symlink::Create doing nothing")
	return fuse.ENOTDIR
}

func (link *Symlink) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	defer c.funcIn("Symlink::SetAttr").Out()

	return link.parentSetChildAttr(c, link.InodeCommon.id, attr, out, false)
}

func (link *Symlink) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Symlink::Mkdir doing nothing")
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
	defer c.funcIn("Symlink::Readlink").Out()

	link.self.markSelfAccessed(c, quantumfs.PathRead)
	data := c.dataStore.Get(&c.Ctx, link.key)
	if data == nil {
		return nil, fuse.EIO
	}

	return data.Get(), fuse.OK
}

func (link *Symlink) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("Symlink::GetXAttrSize").Out()
	return link.parentGetChildXAttrSize(c, link.inodeNum(), attr)
}

func (link *Symlink) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("Symlink::GetXAttrData").Out()
	return link.parentGetChildXAttrData(c, link.inodeNum(), attr)
}

func (link *Symlink) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	defer c.funcIn("Symlink::ListXAttr").Out()
	return link.parentListChildXAttr(c, link.inodeNum())
}

func (link *Symlink) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	defer c.funcIn("Symlink::SetXAttr").Out()
	return link.parentSetChildXAttr(c, link.inodeNum(), attr, data)
}

func (link *Symlink) RemoveXAttr(c *ctx, attr string) fuse.Status {
	defer c.funcIn("Symlink::RemoveXAttr").Out()
	return link.parentRemoveChildXAttr(c, link.inodeNum(), attr)
}

func (link *Symlink) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, newType quantumfs.ObjectType) {

	c.elog("Invalid syncChild on Symlink")
}

func (link *Symlink) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.elog("Invalid instantiateChild on Symlink")
	return nil, nil
}

func (link *Symlink) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("Symlink::flush").Out()
	link.parentSyncChild(c, func() (quantumfs.ObjectKey, quantumfs.ObjectType) {
		return link.key, quantumfs.ObjectTypeSymlink
	})

	return link.key
}
