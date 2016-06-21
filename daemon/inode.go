// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package daemon

import "fmt"
import "reflect"
import "sync"
import "sync/atomic"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

type InodeId uint64

// Inode represents a specific path in the tree which updates as the tree itself
// changes.
type Inode interface {
	Access(c *ctx, mask uint32, uid uint32, gid uint32) fuse.Status

	GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status

	Lookup(c *ctx, name string,
		out *fuse.EntryOut) fuse.Status

	Open(c *ctx, flags uint32, mode uint32,
		out *fuse.OpenOut) fuse.Status

	OpenDir(c *ctx, flags uint32, mode uint32,
		out *fuse.OpenOut) fuse.Status

	Create(c *ctx, input *fuse.CreateIn, name string,
		out *fuse.CreateOut) fuse.Status

	SetAttr(c *ctx, attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status

	Mkdir(c *ctx, name string, input *fuse.MkdirIn,
		out *fuse.EntryOut) fuse.Status

	Unlink(c *ctx, name string) fuse.Status

	Rmdir(c *ctx, name string) fuse.Status

	Symlink(c *ctx, pointedTo string, name string,
		out *fuse.EntryOut) fuse.Status

	Readlink(c *ctx) ([]byte, fuse.Status)

	// Methods called by children
	setChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
		out *fuse.AttrOut) fuse.Status

	getChildRecord(c *ctx, inodeNum InodeId) (quantumfs.DirectoryRecord, error)

	dirty(c *ctx) // Mark this Inode dirty
	// Mark this Inode dirty because a child is dirty
	dirtyChild(c *ctx, child Inode)
	isDirty() bool // Is this Inode dirty?

	// Compute a new object key, possibly schedule the sync the object data
	// itself to the datastore
	sync(c *ctx) quantumfs.ObjectKey

	inodeNum() InodeId
}

type InodeCommon struct {
	// These fields are constant once instantiated
	self Inode // Leaf subclass instance
	id   InodeId

	lock sync.RWMutex

	// This field is accessed using atomic instructions
	dirty_ uint32 // 1 if this Inode or any children are dirty
}

func (inode *InodeCommon) inodeNum() InodeId {
	return inode.id
}

func (inode *InodeCommon) isDirty() bool {
	if atomic.LoadUint32(&inode.dirty_) == 1 {
		return true
	} else {
		return false
	}
}

func (inode *InodeCommon) setDirty(dirty bool) {
	var val uint32
	if dirty {
		val = 1
	} else {
		val = 0
	}

	atomic.StoreUint32(&inode.dirty_, val)
}

func (inode *InodeCommon) dirtyChild(c *ctx, child Inode) {
	inodeType := reflect.TypeOf(inode)
	msg := fmt.Sprintf("Unsupported dirtyChild() call on leaf Inode: %v %v",
		inodeType, inode)
	panic(msg)
}
func (inode *InodeCommon) Lock() *sync.RWMutex {
	inode.lock.Lock()
	return &inode.lock
}

func (inode *InodeCommon) RLock() *sync.RWMutex {
	inode.lock.RLock()
	return &inode.lock
}

// FileHandle represents a specific path at a specific point in time, even as the
// tree changes underneath it. This is used to provide consistent snapshot views into
// the tree.
type FileHandle interface {
	ReadDirPlus(c *ctx, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status
	Read(c *ctx, offset uint64, size uint32, buf []byte, nonblocking bool) (
		fuse.ReadResult, fuse.Status)

	Write(c *ctx, offset uint64, size uint32, flags uint32, buf []byte) (
		uint32, fuse.Status)
}

type FileHandleId uint64

type FileHandleCommon struct {
	id       FileHandleId
	inodeNum InodeId
}
