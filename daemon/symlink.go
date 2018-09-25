// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file holds the Symlink type, which represents symlinks

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func newSymlink(c *ctx, name string, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) Inode {

	defer c.FuncIn("newSymlink", "name %s", name).Out()

	symlink := Symlink{
		InodeCommon: InodeCommon{
			id:         inodeNum,
			name_:      name,
			accessed_:  0,
			treeState_: parent.treeState(),
		},
		key: key,
	}
	symlink.self = &symlink
	symlink.setParent(c, parent)
	utils.Assert(symlink.treeState() != nil, "Symlink treeState nil at init")

	if dirRecord != nil {
		dirRecord.SetPermissions(modeToPermissions(0777, 0))
	}
	return &symlink
}

type Symlink struct {
	InodeCommon
	key quantumfs.ObjectKey

	// String to hold not-yet-flushed pointed to data, if len > 0
	dirtyPointsTo string
}

func (link *Symlink) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.funcIn("Symlink::Access").Out()
	c.elog("Access called on a symlink - should have been dereferenced.")

	return fuse.OK
}

func (link *Symlink) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.wlog("Invalid Lookup call on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.wlog("Invalid Open call on Symlink")
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

	return link.parentSetChildAttr(c, link.InodeCommon.id, nil, attr, out,
		false)
}

func (link *Symlink) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Symlink::Mkdir doing nothing")
	return fuse.ENOTDIR
}

func (link *Symlink) Unlink(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Unlink on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Rmdir(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Rmdir on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Symlink on Symlink")
	return fuse.ENOTDIR
}

func (link *Symlink) Readlink(c *ctx) ([]byte, fuse.Status) {
	defer c.funcIn("Symlink::Readlink").Out()

	// Mark accessed must happen outside of the inode lock
	link.self.markSelfAccessed(c, quantumfs.PathRead)

	defer link.Lock().Unlock()

	// If we have an unflushed pointsTo, then use it
	if link.dirtyPointsTo != "" {
		return []byte(link.dirtyPointsTo), fuse.OK
	}

	data := c.dataStore.Get(&c.Ctx, link.key)
	if data == nil {
		return nil, fuse.EIO
	}

	return slowCopy(data), fuse.OK
}

func (link *Symlink) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Mknod on Symlink")
	return fuse.ENOSYS
}

func (link *Symlink) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.wlog("Invalid RenameChild on Symlink")
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

// Must be called with the instantiation lock
func (link *Symlink) instantiateChild_(c *ctx, inodeNum InodeId) Inode {
	c.elog("Invalid instantiateChild_ on Symlink")
	return nil
}

func (link *Symlink) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("Symlink::flush").Out()

	link.parentSyncChild(c, func() (quantumfs.ObjectKey, *HardlinkDelta) {
		if link.dirtyPointsTo == "" {
			// use existing key
			return link.key, nil
		}

		buf := newBuffer(c, []byte(link.dirtyPointsTo),
			quantumfs.KeyTypeMetadata)

		key, err := buf.Key(&c.Ctx)
		if err != nil {
			panic(fmt.Sprintf("Failed to upload block: %v", err))
		}
		link.key = key

		link.dirtyPointsTo = ""

		return link.key, nil
	})

	return link.key
}

func (link *Symlink) setLink(c *ctx, pointTo string) {
	defer c.FuncIn("Symlink::setLink", "%s", pointTo).Out()

	defer link.Lock().Unlock()
	link.dirtyPointsTo = pointTo
}
