// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// This file holds the special type, which represents devices files, fifos and unix
// domain sockets

import (
	"fmt"
	"syscall"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func newSpecial(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) Inode {

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
		filetype = mode & syscall.S_IFMT
		device = rdev
		c.wlog("mknod mode %x", filetype)
	}

	special := Special{
		InodeCommon: InodeCommon{
			id:         inodeNum,
			name_:      name,
			accessed_:  0,
			treeState_: parent.treeState(),
		},
		filetype: filetype,
		device:   device,
	}
	special.self = &special
	special.setParent(c, parent)
	utils.Assert(special.treeState() != nil, "Special treeState nil at init")

	if dirRecord != nil {
		dirRecord.SetID(quantumfs.EncodeSpecialKey(filetype, device))
	}
	return &special
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

func (special *Special) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.wlog("Invalid Lookup call on Special")
	return fuse.ENOSYS
}

func (special *Special) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.wlog("Invalid Open call on Special")
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
	c.wlog("Invalid Unlink on Special")
	return fuse.ENOTDIR
}

func (special *Special) Rmdir(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Rmdir on Special")
	return fuse.ENOTDIR
}

func (special *Special) Symlink(c *ctx, pointedTo string, specialName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Symlink on Special")
	return fuse.ENOTDIR
}

func (special *Special) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.wlog("Invalid Readlink on Special")
	return nil, fuse.EINVAL
}

func (special *Special) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Mknod on Special")
	return fuse.ENOSYS
}

func (special *Special) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.wlog("Invalid RenameChild on Special")
	return fuse.ENOSYS
}

func (special *Special) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.wlog("Invalid GetXAttrSize on Special")
	return 0, fuse.ENODATA
}

func (special *Special) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.wlog("Invalid GetXAttrData on Special")
	return nil, fuse.ENODATA
}

func (special *Special) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.wlog("Invalid ListXAttr on Special")
	return []byte{}, fuse.OK
}

func (special *Special) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.wlog("Invalid SetXAttr on Special")
	return fuse.Status(syscall.ENOSPC)
}

func (special *Special) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.wlog("Invalid RemoveXAttr on Special")
	return fuse.ENODATA
}

// Must be called with the instantiation lock
func (special *Special) instantiateChild_(c *ctx,
	inodeNum InodeId) Inode {

	c.elog("Invalid instantiateChild_ on Special")
	return nil
}

func (special *Special) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("Special::flush").Out()

	key := quantumfs.EncodeSpecialKey(special.filetype, special.device)

	special.parentSyncChild(c, func() (quantumfs.ObjectKey, *HardlinkDelta) {
		return key, nil
	})

	return key
}

func specialOverrideAttr(entry quantumfs.ImmutableDirectoryRecord,
	attr *fuse.Attr) uint32 {

	attr.Size = 0
	attr.Blocks = utils.BlocksRoundUp(attr.Size, statBlockSize)
	attr.Nlink = entry.Nlinks()

	filetype, dev, err := quantumfs.DecodeSpecialKey(underlyingID(entry))
	if err != nil {
		panic(fmt.Sprintf("Decoding special file failed: %v",
			err))
	}
	attr.Rdev = dev

	return filetype
}
