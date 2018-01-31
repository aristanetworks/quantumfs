// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains a simple file which indicates when QuantumFS is in a low memory
// mode to users.

import (
	"errors"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func NewLowMemFile(treeLock *TreeLock, parent InodeId) Inode {
	lm := LowMemFile{
		InodeCommon: InodeCommon{
			id:        quantumfs.InodeIdLowMemMarker,
			name_:     quantumfs.LowMemFileName,
			treeLock_: treeLock,
		},
	}
	lm.self = &lm
	lm.setParent(parent)
	utils.Assert(lm.treeLock() != nil, "LowMemFile treeLock is nil at init")
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

func (lw *LowMemFile) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("LowMemFile::Mkdir doing nothing")
	return fuse.ENOTDIR
}

func (wsr *LowMemFile) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	c.elog("LowMemFile doesn't support record fetch")
	return nil, errors.New("Unsupported record fetch")
}

func (lw *LowMemFile) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on LowMemFile")
	return fuse.ENOTDIR
}

func (lw *LowMemFile) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on LowMemFile")
	return fuse.ENOTDIR
}

func (lw *LowMemFile) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer c.FuncIn("LowMemFile::Open", "flags %d mode %d", flags, mode).Out()

	out.OpenFlags = 0
	handle := newLowMemFileHandle(c, lw.treeLock())
	c.qfs.setFileHandle(c, handle.FileHandleCommon.id, handle)
	out.Fh = uint64(handle.FileHandleCommon.id)
	return fuse.OK
}

func (lw *LowMemFile) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Lookup on LowMemFile")
	return fuse.ENOSYS
}

func (lw *LowMemFile) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.elog("Invalid Create on LowMemFile")
	return fuse.ENOTDIR
}

func (lw *LowMemFile) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid SetAttr on LowMemFile")
	return fuse.ENOSYS
}

func (lw *LowMemFile) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on LowMemFile")
	return fuse.ENOTDIR
}

func (lw *LowMemFile) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on LowMemFile")
	return nil, fuse.EINVAL
}

func (lw *LowMemFile) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on LowMemFile")
	return fuse.ENOSYS
}

func (lw *LowMemFile) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on LowMemFile")
	return fuse.ENOSYS
}

func (lw *LowMemFile) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on LowMemFile")
	return fuse.ENOSYS
}

func (lw *LowMemFile) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.vlog("LowMemFile::GetXAttrSize doing nothing")
	return 0, fuse.ENODATA
}

func (lw *LowMemFile) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("LowMemFile::GetXAttrData doing nothing")
	return nil, fuse.ENODATA
}

func (lw *LowMemFile) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("LowMemFile::ListXAttr doing nothing")
	return []byte{}, fuse.OK
}

func (lw *LowMemFile) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("LowMemFile::SetXAttr doing nothing")
	return fuse.Status(syscall.ENOSPC)
}

func (lw *LowMemFile) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("LowMemFile::RemoveXAttr doing nothing")
	return fuse.ENODATA
}

func (lw *LowMemFile) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, newType quantumfs.ObjectType) {

	c.elog("Invalid syncChild on LowMemFile")
}

func (lw *LowMemFile) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on LowMemFile")
	return fuse.ENOSYS
}

func (lw *LowMemFile) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on LowMemFile")
	return 0, fuse.ENODATA
}

func (lw *LowMemFile) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on LowMemFile")
	return nil, fuse.ENODATA
}

func (lw *LowMemFile) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on LowMemFile")
	return []byte{}, fuse.OK
}

func (lw *LowMemFile) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on LowMemFile")
	return fuse.Status(syscall.ENOSPC)
}

func (lw *LowMemFile) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on LowMemFile")
	return fuse.ENODATA
}

func (lw *LowMemFile) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.elog("Invalid instantiateChild on LowMemFile")
	return nil, nil
}

func (lw *LowMemFile) flush(c *ctx) quantumfs.ObjectKey {
	c.vlog("LowMemFile::flush doing nothing")
	return quantumfs.EmptyBlockKey
}

func newLowMemFileHandle(c *ctx, treeLock *TreeLock) *LowMemFileHandle {
	defer c.funcIn("newLowMemFileHandle").Out()

	lwh := LowMemFileHandle{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  quantumfs.InodeIdLowMemMarker,
			treeLock_: treeLock,
		},
	}
	utils.Assert(lwh.treeLock() != nil, "LowMemFileHandle treeLock nil at init")
	return &lwh
}

type LowMemFileHandle struct {
	FileHandleCommon
}

func (lwh *LowMemFileHandle) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.elog("Invalid ReadDirPlus against LowMemFileHandle")
	return fuse.ENOSYS
}

func (lwh *LowMemFileHandle) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	defer c.FuncIn("LowMemFileHandle::Read", "offset %d size %d nonblocking %t",
		offset, size, nonblocking).Out()

	readCount := 0
	if offset < uint64(len(lowMemDescription)) {
		readCount = copy(buf, lowMemDescription[offset:])
	}

	return fuse.ReadResultData(buf[:readCount]), fuse.OK
}

func (lwh *LowMemFileHandle) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	c.elog("Invalid Write against LowMemFileHandle")

	return 0, fuse.EPERM
}
