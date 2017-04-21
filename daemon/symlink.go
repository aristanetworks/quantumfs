// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the Symlink type, which represents symlinks

import "errors"
import "syscall"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"

import "github.com/hanwen/go-fuse/fuse"

func newSymlink(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newSymlink", "name %s", name).out()

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

	defer c.funcIn("Symlink::Access").out()

	link.self.markSelfAccessed(c, false)
	c.elog("Access called on a symlink - should have been dereferenced.")

	return fuse.OK
}

func (link *Symlink) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("Symlink::GetAttr").out()
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

	defer c.funcIn("Symlink::SetAttr").out()

	return link.parentSetChildAttr(c, link.InodeCommon.id, nil, attr, out,
		false)
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
	defer c.funcIn("Symlink::Readlink").out()

	link.self.markSelfAccessed(c, false)
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

	defer c.funcIn("Symlink::GetXAttrSize").out()
	return link.parentGetChildXAttrSize(c, link.inodeNum(), attr)
}

func (link *Symlink) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("Symlink::GetXAttrData").out()
	return link.parentGetChildXAttrData(c, link.inodeNum(), attr)
}

func (link *Symlink) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	defer c.funcIn("Symlink::ListXAttr").out()
	return link.parentListChildXAttr(c, link.inodeNum())
}

func (link *Symlink) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	defer c.funcIn("Symlink::SetXAttr").out()
	return link.parentSetChildXAttr(c, link.inodeNum(), attr, data)
}

func (link *Symlink) RemoveXAttr(c *ctx, attr string) fuse.Status {
	defer c.funcIn("Symlink::RemoveXAttr").out()
	return link.parentRemoveChildXAttr(c, link.inodeNum(), attr)
}

func (link *Symlink) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.elog("Invalid syncChild on Symlink")
}

func (link *Symlink) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on Symlink")
	return 0, fuse.ENODATA
}

func (link *Symlink) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on Symlink")
	return nil, fuse.ENODATA
}

func (link *Symlink) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on Symlink")
	return []byte{}, fuse.OK
}

func (link *Symlink) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on Symlink")
	return fuse.Status(syscall.ENOSPC)
}

func (link *Symlink) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on Symlink")
	return fuse.ENODATA
}

func (link *Symlink) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.elog("Invalid instantiateChild on Symlink")
	return nil, nil
}

func (link *Symlink) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	c.elog("Unsupported record fetch on Symlink")
	return &quantumfs.DirectRecord{}, errors.New("Unsupported record fetch")
}

func (link *Symlink) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("Symlink::flush").out()
	link.parentSyncChild(c, link.inodeNum(), func() quantumfs.ObjectKey {
		return link.key
	})

	return link.key
}

func mergeSymlink(c *ctx, remote quantumfs.DirectoryRecord,
	local quantumfs.DirectoryRecord) quantumfs.ObjectKey {

	defer c.FuncIn("mergeSymlink", "%s", local.Filename()).out()

	// Take the newer file
	if remote.ModificationTime() > local.ModificationTime() {
		c.vlog("taking remote copy of %s", remote.Filename())
		return remote.ID()
	}

	c.vlog("keeping local copy of %s", local.Filename())
	return local.ID()
}
