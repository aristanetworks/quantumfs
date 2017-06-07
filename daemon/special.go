// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the special type, which represents devices files, fifos and unix
// domain sockets

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func newSpecial(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("newSpecial", "name %s", name).Out()

	var filetype uint32
	var device uint32
	var err error
	if dirRecord == nil {
		// key is valid while mode and rdev are not
		filetype, device, err = quantumfs.DecodeSpecialKey(key)
		if err != nil {
			panic(fmt.Sprintf("Initializing special file failed: %v",
				err))
		}
	} else {
		// key is invalid, but mode and rdev contain the data we want and we
		// must store it in directoryRecord
		filetype = mode
		device = rdev
		c.wlog("mknod mode %x", filetype)
	}

	special := Special{
		InodeCommon: InodeCommon{
			id:        inodeNum,
			name_:     name,
			accessed_: 0,
			treeLock_: parent.treeLock(),
		},
		filetype: filetype,
		device:   device,
	}
	special.self = &special
	special.setParent(parent.inodeNum())
	utils.Assert(special.treeLock() != nil, "Special treeLock nil at init")

	if dirRecord != nil {
		dirRecord.SetID(quantumfs.EncodeSpecialKey(filetype, device))
	}
	return &special, nil
}

type Special struct {
	InodeCommon
	filetype uint32
	device   uint32
}

func (special *Special) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.funcIn("Special::Access").Out()
	return hasAccessPermission(c, special, mask, uid, gid)
}

func (special *Special) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("Special::GetAttr").Out()

	record, err := special.parentGetChildRecordCopy(c, special.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", special.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, special.InodeCommon.id,
		c.fuseCtx.Owner, record)

	return fuse.OK
}

func (special *Special) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.elog("Invalid Lookup call on Special")
	return fuse.ENOSYS
}

func (special *Special) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.elog("Invalid Open call on Special")
	return fuse.ENOSYS
}

func (special *Special) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("Special::OpenDir doing nothing")
	return fuse.ENOTDIR
}

func (special *Special) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("Special::Create doing nothing")
	return fuse.ENOTDIR
}

func (special *Special) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	defer c.funcIn("Special::SetAttr").Out()
	return special.parentSetChildAttr(c, special.InodeCommon.id,
		nil, attr, out, false)
}

func (special *Special) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Special::Mkdir doing nothing")
	return fuse.ENOTDIR
}

func (special *Special) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on Special")
	return fuse.ENOTDIR
}

func (special *Special) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on Special")
	return fuse.ENOTDIR
}

func (special *Special) Symlink(c *ctx, pointedTo string, specialName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on Special")
	return fuse.ENOTDIR
}

func (special *Special) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on Special")
	return nil, fuse.EINVAL
}

func (special *Special) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on Special")
	return fuse.ENOSYS
}

func (special *Special) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on Special")
	return fuse.ENOSYS
}

func (special *Special) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on Special")
	return fuse.ENOSYS
}

func (special *Special) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid GetXAttrSize on Special")
	return 0, fuse.ENODATA
}

func (special *Special) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid GetXAttrData on Special")
	return nil, fuse.ENODATA
}

func (special *Special) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.elog("Invalid ListXAttr on Special")
	return []byte{}, fuse.OK
}

func (special *Special) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.elog("Invalid SetXAttr on Special")
	return fuse.Status(syscall.ENOSPC)
}

func (special *Special) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.elog("Invalid RemoveXAttr on Special")
	return fuse.ENODATA
}

func (special *Special) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.elog("Invalid syncChild on Special")
}

func (special *Special) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on Special")
	return fuse.ENOSYS
}

func (special *Special) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on Special")
	return 0, fuse.ENODATA
}

func (special *Special) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on Special")
	return nil, fuse.ENODATA
}

func (special *Special) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on Special")
	return []byte{}, fuse.OK
}

func (special *Special) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on Special")
	return fuse.Status(syscall.ENOSPC)
}

func (special *Special) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on Special")
	return fuse.ENODATA
}

func (special *Special) instantiateChild(c *ctx,
	inodeNum InodeId) (Inode, []InodeId) {

	c.elog("Invalid instantiateChild on Special")
	return nil, nil
}

func (special *Special) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	c.elog("Unsupported record fetch on Special")
	return &quantumfs.DirectRecord{}, errors.New("Unsupported record fetch")
}

func (special *Special) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("Special::flush").Out()

	key := quantumfs.EncodeSpecialKey(special.filetype, special.device)

	special.parentSyncChild(c, special.inodeNum(), func() quantumfs.ObjectKey {
		return key
	})

	return key
}

func specialOverrideAttr(entry quantumfs.DirectoryRecord, attr *fuse.Attr) uint32 {
	attr.Size = 0
	attr.Blocks = utils.BlocksRoundUp(attr.Size, statBlockSize)
	attr.Nlink = entry.Nlinks()

	filetype, dev, err := quantumfs.DecodeSpecialKey(entry.ID())
	if err != nil {
		panic(fmt.Sprintf("Decoding special file failed: %v",
			err))
	}
	attr.Rdev = dev

	return filetype
}
