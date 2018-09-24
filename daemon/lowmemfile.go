// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains a simple file which indicates when QuantumFS is in a low memory
// mode to users.

import (
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func NewLowMemFile(c *ctx, treeState *TreeState, parent Inode) Inode {
	lm := LowMemFile{
		InodeCommon: InodeCommon{
			id:         quantumfs.InodeIdLowMemMarker,
			name_:      quantumfs.LowMemFileName,
			treeState_: treeState,
		},
	}
	lm.self = &lm
	lm.setParent(c, parent)
	utils.Assert(lm.treeState() != nil, "LowMemFile treeState is nil at init")
	return &lm
}

type LowMemFile struct {
	InodeCommon
}

var lowMemDescription = []byte(
	`This QuantumFS instance is in a low memory mode and performance
will be poorer than normal. Please save all work, close all
applications and use the newer QuantumFS instance.
`)

func fillLowMemAttr(c *ctx, attr *fuse.Attr) {
	attr.Ino = quantumfs.InodeIdLowMemMarker
	attr.Size = uint64(len(lowMemDescription))
	attr.Blocks = utils.BlocksRoundUp(attr.Size, statBlockSize)

	now := time.Now()
	attr.Atime = uint64(now.Unix())
	attr.Atimensec = uint32(now.Nanosecond())
	attr.Mtime = uint64(now.Unix())
	attr.Mtimensec = uint32(now.Nanosecond())

	attr.Ctime = 1
	attr.Ctimensec = 1
	attr.Mode = 0444 | fuse.S_IFREG
	attr.Nlink = 1
	attr.Owner.Uid = 0
	attr.Owner.Gid = 0
	attr.Blksize = 4096
}

func (lm *LowMemFile) dirty(c *ctx) {
	c.vlog("LowMemFile::dirty doing nothing")
	// Override the InodeCommon dirty because the LowMemFile can never be changed
	// on the filesystem itself.
}

func (lm *LowMemFile) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.FuncIn("LowMemFile::Access", "mask %d uid %d gid %d", mask, uid,
		gid).Out()

	switch mask {
	case fuse.F_OK,
		fuse.W_OK,
		fuse.R_OK:
		return fuse.OK
	case fuse.X_OK:
		return fuse.EACCES
	default:
		return fuse.EINVAL
	}
}

func (lm *LowMemFile) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("LowMemFile::GetAttr").Out()
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	fillLowMemAttr(c, &out.Attr)
	return fuse.OK
}

func (lm *LowMemFile) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("LowMemFile::OpenDir doing nothing")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("LowMemFile::Mkdir doing nothing")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	panic("LowMemFile doesn't support record fetch")
}

func (lm *LowMemFile) Unlink(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Unlink on LowMemFile")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) Rmdir(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Rmdir on LowMemFile")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer c.FuncIn("LowMemFile::Open", "flags %d mode %d", flags, mode).Out()

	out.OpenFlags = 0
	handle := newLowMemFileHandle(c, lm, lm.treeState())
	c.qfs.setFileHandle(c, handle.FileHandleCommon.id, handle)
	out.Fh = uint64(handle.FileHandleCommon.id)
	return fuse.OK
}

func (lm *LowMemFile) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Lookup on LowMemFile")
	return fuse.ENOSYS
}

func (lm *LowMemFile) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.wlog("Invalid Create on LowMemFile")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.wlog("Invalid SetAttr on LowMemFile")
	return fuse.ENOSYS
}

func (lm *LowMemFile) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Symlink on LowMemFile")
	return fuse.ENOTDIR
}

func (lm *LowMemFile) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.wlog("Invalid Readlink on LowMemFile")
	return nil, fuse.EINVAL
}

func (lm *LowMemFile) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Mknod on LowMemFile")
	return fuse.ENOSYS
}

func (lm *LowMemFile) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on LowMemFile")
	return fuse.ENOSYS
}

func (lm *LowMemFile) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.vlog("LowMemFile::GetXAttrSize doing nothing")
	return 0, fuse.ENODATA
}

func (lm *LowMemFile) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("LowMemFile::GetXAttrData doing nothing")
	return nil, fuse.ENODATA
}

func (lm *LowMemFile) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("LowMemFile::ListXAttr doing nothing")
	return []byte{}, fuse.OK
}

func (lm *LowMemFile) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("LowMemFile::SetXAttr doing nothing")
	return fuse.Status(syscall.ENOSPC)
}

func (lm *LowMemFile) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("LowMemFile::RemoveXAttr doing nothing")
	return fuse.ENODATA
}

func (lm *LowMemFile) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on LowMemFile")
	return fuse.ENOSYS
}

func (lm *LowMemFile) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on LowMemFile")
	return 0, fuse.ENODATA
}

func (lm *LowMemFile) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on LowMemFile")
	return nil, fuse.ENODATA
}

func (lm *LowMemFile) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on LowMemFile")
	return []byte{}, fuse.OK
}

func (lm *LowMemFile) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on LowMemFile")
	return fuse.Status(syscall.ENOSPC)
}

func (lm *LowMemFile) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on LowMemFile")
	return fuse.ENODATA
}

// Must be called with the instantiation lock
func (lm *LowMemFile) instantiateChild_(c *ctx, inodeNum InodeId) Inode {
	c.elog("Invalid instantiateChild_ on LowMemFile")
	return nil
}

func (lm *LowMemFile) flush(c *ctx) quantumfs.ObjectKey {
	c.vlog("LowMemFile::flush doing nothing")
	return quantumfs.EmptyBlockKey
}

func newLowMemFileHandle(c *ctx, lm *LowMemFile,
	treeState *TreeState) *LowMemFileHandle {

	defer c.funcIn("newLowMemFileHandle").Out()

	lmh := LowMemFileHandle{
		FileHandleCommon: FileHandleCommon{
			id:         c.qfs.newFileHandleId(),
			inode:      lm,
			treeState_: treeState,
		},
	}
	utils.Assert(lmh.treeState() != nil,
		"LowMemFileHandle treeState nil at init")
	return &lmh
}

type LowMemFileHandle struct {
	FileHandleCommon
}

func (lmh *LowMemFileHandle) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.wlog("Invalid ReadDirPlus against LowMemFileHandle")
	return fuse.ENOSYS
}

func (lmh *LowMemFileHandle) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	defer c.FuncIn("LowMemFileHandle::Read", "offset %d size %d nonblocking %t",
		offset, size, nonblocking).Out()

	readCount := 0
	if offset < uint64(len(lowMemDescription)) {
		readCount = copy(buf, lowMemDescription[offset:])
	}

	return fuse.ReadResultData(buf[:readCount]), fuse.OK
}

func (lmh *LowMemFileHandle) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	c.wlog("Invalid Write against LowMemFileHandle")

	return 0, fuse.EPERM
}
