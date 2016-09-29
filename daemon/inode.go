// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package daemon

import "fmt"
import "reflect"
import "sync"
import "sync/atomic"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

type InodeId uint64

func (v InodeId) Primitive() interface{} {
	return uint64(v)
}

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

	Mknod(c *ctx, name string, input *fuse.MknodIn,
		out *fuse.EntryOut) fuse.Status

	RenameChild(c *ctx, oldName string, newName string) fuse.Status

	MvChild(c *ctx, dstInode Inode, oldName string, newName string) fuse.Status

	GetXAttrSize(c *ctx, attr string) (size int, result fuse.Status)

	GetXAttrData(c *ctx, attr string) (data []byte, result fuse.Status)

	ListXAttr(c *ctx) (attributes []byte, result fuse.Status)

	SetXAttr(c *ctx, attr string, data []byte) fuse.Status

	RemoveXAttr(c *ctx, attr string) fuse.Status

	// Methods called by children
	setChildAttr(c *ctx, inodeNum InodeId, newType *quantumfs.ObjectType,
		attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status

	getChildRecord(c *ctx, inodeNum InodeId) (quantumfs.DirectoryRecord, error)

	// Update the key for only this child and then notify all the grandparents of
	// the cascading changes.
	syncChild(c *ctx, inodeNum InodeId, newKey quantumfs.ObjectKey)

	getChildXAttrSize(c *ctx, inodeNum InodeId,
		attr string) (size int, result fuse.Status)

	getChildXAttrData(c *ctx,
		inodeNum InodeId, attr string) (data []byte, result fuse.Status)

	listChildXAttr(c *ctx,
		inodeNum InodeId) (attributes []byte, result fuse.Status)

	setChildXAttr(c *ctx, inodeNum InodeId, attr string, data []byte) fuse.Status

	removeChildXAttr(c *ctx, inodeNum InodeId, attr string) fuse.Status

	name() string
	findPath(c *ctx) (string, bool)
	findWorkspace(c *ctx) (*WorkspaceRoot, bool)
	register(c *ctx)

	parent() Inode
	setParent(newParent Inode)

	dirty(c *ctx) // Mark this Inode dirty
	// Mark this Inode dirty because a child is dirty
	dirtyChild(c *ctx, child Inode)
	isDirty() bool // Is this Inode dirty?

	// Compute a new object key, possibly schedule the sync the object data
	// itself to the datastore
	flush_DOWN(c *ctx) quantumfs.ObjectKey
	Sync_DOWN(c *ctx) fuse.Status
	forget_DOWN(c *ctx)
	link_DOWN(c *ctx, srcInode Inode, newName string,
		out *fuse.EntryOut) fuse.Status

	inodeNum() InodeId

	treeLock() *sync.RWMutex
	LockTree() *sync.RWMutex
	RLockTree() *sync.RWMutex
}

type InodeCommon struct {
	// These fields are constant once instantiated
	self Inode // Leaf subclass instance
	id   InodeId

	name_    string // '/' if WorkspaceRoot
	accessed bool

	parentLock sync.Mutex // Protects parent_
	parent_    Inode      // nil if WorkspaceRoot

	lock sync.RWMutex

	// The treeLock is used to lock the entire workspace tree when certain
	// tree-wide operations are being performed. Primarily this is done with all
	// requests which call downward (parent to child) in the tree. This is done
	// to ensure that all Inode locks are only acquired child to parent.
	treeLock_ *sync.RWMutex

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

func (inode *InodeCommon) name() string {
	return inode.name_
}

func (inode *InodeCommon) parent() Inode {
	inode.parentLock.Lock()
	p := inode.parent_
	inode.parentLock.Unlock()

	return p
}

func (inode *InodeCommon) setParent(newParent Inode) {
	inode.parentLock.Lock()
	inode.parent_ = newParent
	inode.parentLock.Unlock()
}

func (inode *InodeCommon) treeLock() *sync.RWMutex {
	return inode.treeLock_
}

func (inode *InodeCommon) LockTree() *sync.RWMutex {
	inode.treeLock_.Lock()
	return inode.treeLock_
}

func (inode *InodeCommon) RLockTree() *sync.RWMutex {
	inode.treeLock_.RLock()
	return inode.treeLock_
}

func (inode *InodeCommon) Lock() *sync.RWMutex {
	inode.lock.Lock()
	return &inode.lock
}

func (inode *InodeCommon) RLock() *sync.RWMutex {
	inode.lock.RLock()
	return &inode.lock
}

func (inode *InodeCommon) findPath(c *ctx) (string, bool) {
	parent := inode.parent()
	if parent == nil {
		return "", false
	}

	path := inode.name()
	for parent.parent() != nil {
		path = parent.name() + "/" + path
		parent = parent.parent()
	}
	path = "/" + path
	return path, true
}

func (inode *InodeCommon) findWorkspace(c *ctx) (*WorkspaceRoot, bool) {
	parent := inode.parent()
	if parent == nil {
		c.elog("Invalid findWorkspace on directory not in workspace")
		return nil, false
	}

	for parent.parent() != nil {
		parent = parent.parent()
	}

	wsr, ok := parent.(*WorkspaceRoot)
	if !ok {
		path, _ := inode.findPath(c)
		c.elog("Incorrect finding workspaceroot" + path)
		return nil, false
	}

	return wsr, true
}

func (inode *InodeCommon) register(c *ctx) {
	if inode.accessed != true {
		path, pathok := inode.findPath(c)
		ws, wsok := inode.findWorkspace(c)

		if pathok && wsok {
			ws.accessList[path] = true
			inode.accessed = true
		}
	}

	c.elog("file is already accessed")
}

func getLockOrder(a Inode, b Inode) (lockFirst Inode, lockLast Inode) {
	// Always lock the higher number inode first
	if a.inodeNum() > b.inodeNum() {
		return a, b
	} else {
		return b, a
	}
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

	Sync_DOWN(c *ctx) fuse.Status

	treeLock() *sync.RWMutex
	LockTree() *sync.RWMutex
	RLockTree() *sync.RWMutex
}

type FileHandleId uint64

func (v FileHandleId) Primitive() interface{} {
	return uint64(v)
}

type FileHandleCommon struct {
	id        FileHandleId
	inodeNum  InodeId
	treeLock_ *sync.RWMutex
}

func (file *FileHandleCommon) treeLock() *sync.RWMutex {
	return file.treeLock_
}

func (file *FileHandleCommon) LockTree() *sync.RWMutex {
	file.treeLock_.Lock()
	return file.treeLock_
}

func (file *FileHandleCommon) RLockTree() *sync.RWMutex {
	file.treeLock_.RLock()
	return file.treeLock_
}
