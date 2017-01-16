// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package daemon

import "fmt"
import "sync"
import "sync/atomic"
import "time"

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
		attr *fuse.SetAttrIn, out *fuse.AttrOut,
		updateMtime bool) fuse.Status

	getChildRecord(c *ctx, inodeNum InodeId) (DirectoryRecordIf, error)

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

	// Instantiate the Inode for the given child on demand
	instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId)

	name() string
	setName(name string)

	accessed() bool
	markAccessed(c *ctx, path string, created bool)
	markSelfAccessed(c *ctx, created bool)

	parentId() InodeId
	parent(c *ctx) Inode
	setParent(newParent InodeId)

	// An orphaned Inode is one which is parented to itself. That is, it is
	// orphaned from the directory tree and cannot be accessed except directly by
	// the inodeNum or by an already open file handle.
	isOrphaned() bool

	dirty(c *ctx) // Mark this Inode dirty
	// Mark this Inode dirty because a child is dirty
	dirtyChild(c *ctx, child InodeId)
	isDirty() bool // Is this Inode dirty?

	// Compute a new object key, possibly schedule the sync the object data
	// itself to the datastore
	flush_DOWN(c *ctx) quantumfs.ObjectKey
	Sync_DOWN(c *ctx) fuse.Status
	link_DOWN(c *ctx, srcInode Inode, newName string,
		out *fuse.EntryOut) fuse.Status

	inodeNum() InodeId

	treeLock() *sync.RWMutex
	LockTree() *sync.RWMutex
	LockTreeWaitAtMost(ms time.Duration) *sync.RWMutex
	RLockTree() *sync.RWMutex

	isWorkspaceRoot() bool
}

type inodeHolder interface {
	childInodes() []InodeId
}

type InodeCommon struct {
	// These fields are constant once instantiated
	self Inode // Leaf subclass instance
	id   InodeId

	nameLock sync.Mutex
	name_    string // '/' if WorkspaceRoot

	accessed_ uint32

	parentLock sync.Mutex // Protects parent_
	parent_    InodeId

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

// Returns if this Inode was already dirty or not
func (inode *InodeCommon) setDirty(dirty bool) bool {
	var val uint32
	if dirty {
		val = 1
	} else {
		val = 0
	}

	old := atomic.SwapUint32(&inode.dirty_, val)
	if old == 1 {
		return true
	} else {
		return false
	}
}

func (inode *InodeCommon) dirtyChild(c *ctx, child InodeId) {
	msg := fmt.Sprintf("Unsupported dirtyChild() call on Inode %d: %v", child,
		inode)
	panic(msg)
}

func (inode *InodeCommon) name() string {
	inode.nameLock.Lock()
	defer inode.nameLock.Unlock()
	return inode.name_
}

func (inode *InodeCommon) setName(name string) {
	inode.nameLock.Lock()
	defer inode.nameLock.Unlock()
	inode.name_ = name
}

func (inode *InodeCommon) accessed() bool {
	old := atomic.SwapUint32(&(inode.accessed_), 1)

	if old == 1 {
		return true
	} else {
		return false
	}
}

func (inode *InodeCommon) parent(c *ctx) Inode {
	inode.parentLock.Lock()
	p := inode.parent_
	inode.parentLock.Unlock()

	return c.qfs.inode(c, p)
}

func (inode *InodeCommon) parentId() InodeId {
	inode.parentLock.Lock()
	p := inode.parent_
	inode.parentLock.Unlock()

	return p
}

func (inode *InodeCommon) setParent(newParent InodeId) {
	inode.parentLock.Lock()
	inode.parent_ = newParent
	inode.parentLock.Unlock()
}

func (inode *InodeCommon) isOrphaned() bool {
	return inode.inodeNum() == inode.parentId()
}

func (inode *InodeCommon) treeLock() *sync.RWMutex {
	return inode.treeLock_
}

func (inode *InodeCommon) LockTree() *sync.RWMutex {
	inode.treeLock_.Lock()
	return inode.treeLock_
}

// Attempt to exclusively grab the TreeLock. Wait at most the duration given. If the
// lock is successfully grabbed, return the lock so "defer lock.Unlock()" may be
// written. If the lock is not successfully grabbed, nil is returned.
func (inode *InodeCommon) LockTreeWaitAtMost(wait time.Duration) *sync.RWMutex {
	var lock *sync.RWMutex
	waitForTreeLock := make(chan *sync.RWMutex)
	go func() {
		waitForTreeLock <- inode.LockTree()
	}()

	select {
	case <-time.After(wait):
		// Since we've timed out we need to launch a receiver for the lock.
		// Should the lock ever eventually be acquired if we don't have
		// something waiting to release it we'll indefinitely block that
		// tree.
		go func() {
			lock := <-waitForTreeLock
			lock.Unlock()
		}()
		return nil

	case lock = <-waitForTreeLock:
	}

	return lock
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

func (inode *InodeCommon) markAccessed(c *ctx, path string, created bool) {
	if inode.isWorkspaceRoot() {
		panic("Workspaceroot didn't call .self")
	}

	if inode.isOrphaned() {
		panic("Orphaned file")
	}

	if path == "" {
		path = inode.name()
	} else {
		path = inode.name() + "/" + path
	}
	parent := inode.parent(c)
	parent.markAccessed(c, path, created)
}

func (inode *InodeCommon) markSelfAccessed(c *ctx, created bool) {
	ac := inode.accessed()
	if !created && ac {
		return
	}
	inode.self.markAccessed(c, "", created)
}

func (inode *InodeCommon) isWorkspaceRoot() bool {
	return false
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
