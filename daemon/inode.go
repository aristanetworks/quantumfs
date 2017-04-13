// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The basic Inode and FileHandle structures
package daemon

import "container/list"
import "fmt"
import "sync"
import "sync/atomic"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
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

	getChildRecordCopy(c *ctx, inodeNum InodeId) (quantumfs.DirectoryRecord,
		error)

	// Update the key for only this child
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

	// Note: parent_ must only be called with the parentLock R/W Lock-ed, and the
	// parent Inode returned must only be used while that lock is held
	parentId_() InodeId
	parent_(c *ctx) Inode
	setParent(newParent InodeId)
	setParent_(newParent InodeId)
	getParentLock() *utils.DeferableRwMutex

	// An orphaned Inode is one which is parented to itself. That is, it is
	// orphaned from the directory tree and cannot be accessed except directly by
	// the inodeNum or by an already open file handle.
	isOrphaned() bool
	isOrphaned_() bool
	deleteSelf(c *ctx, toDelete Inode,
		deleteFromParent func() (toOrphan quantumfs.DirectoryRecord,
			err fuse.Status)) fuse.Status

	parentMarkAccessed(c *ctx, path string, created bool)
	parentSyncChild(c *ctx, childId InodeId,
		publishFn func() quantumfs.ObjectKey)
	parentSetChildAttr(c *ctx, inodeNum InodeId, newType *quantumfs.ObjectType,
		attr *fuse.SetAttrIn, out *fuse.AttrOut,
		updateMtime bool) fuse.Status
	parentGetChildXAttrSize(c *ctx, inodeNum InodeId, attr string) (size int,
		result fuse.Status)
	parentGetChildXAttrData(c *ctx, inodeNum InodeId, attr string) (data []byte,
		result fuse.Status)
	parentListChildXAttr(c *ctx, inodeNum InodeId) (attributes []byte,
		result fuse.Status)
	parentSetChildXAttr(c *ctx, inodeNum InodeId, attr string,
		data []byte) fuse.Status
	parentRemoveChildXAttr(c *ctx, inodeNum InodeId, attr string) fuse.Status
	parentGetChildRecordCopy(c *ctx,
		inodeNum InodeId) (quantumfs.DirectoryRecord, error)
	parentHasAncestor(c *ctx, ancestor Inode) bool
	parentCheckLinkReparent(c *ctx, parent *Directory)

	dirty(c *ctx) // Mark this Inode dirty
	markClean()   // Mark this Inode as cleaned
	// Mark this Inode dirty because a child is dirty
	dirtyChild(c *ctx, child InodeId)

	// The kernel has forgotten about this Inode. Add yourself to the list to be
	// flushed and forgotten.
	queueToForget(c *ctx)

	// Returns this inode's place in the dirtyQueue
	dirtyElement() *list.Element

	// Compute a new object key, schedule the object data to be uploaded to the
	// datastore and update the parent with the new key.
	flush(c *ctx) quantumfs.ObjectKey

	Sync_DOWN(c *ctx) fuse.Status
	link_DOWN(c *ctx, srcInode Inode, newName string,
		out *fuse.EntryOut) fuse.Status

	inodeNum() InodeId

	Lock() utils.NeedWriteUnlock

	treeLock() *sync.RWMutex
	LockTree() *sync.RWMutex
	RLockTree() *sync.RWMutex

	isWorkspaceRoot() bool

	Merge(c *ctx, base quantumfs.DirectoryRecord,
		remote quantumfs.DirectoryRecord)
}

type inodeHolder interface {
	directChildInodes() []InodeId
}

type InodeCommon struct {
	// These fields are constant once instantiated
	self Inode // Leaf subclass instance
	id   InodeId

	nameLock sync.Mutex
	name_    string // '/' if WorkspaceRoot

	accessed_ uint32

	// Note: parentId must not be accessed or changed without the parentLock
	parentLock utils.DeferableRwMutex
	parentId   InodeId

	lock utils.DeferableRwMutex

	// The treeLock is used to lock the entire workspace tree when certain
	// tree-wide operations are being performed. Primarily this is done with all
	// requests which call downward (parent to child) in the tree. This is done
	// to ensure that all Inode locks are only acquired child to parent.
	treeLock_ *sync.RWMutex

	// This field is accessed using atomic instructions
	dirty_           uint32 // 1 if this Inode or any children are dirty
	dirtyElementLock utils.DeferableMutex
	dirtyElement_    *list.Element
}

// Must have the parentLock R/W Lock()-ed during the call and for the duration the
// id is used
func (inode *InodeCommon) parentId_() InodeId {
	return inode.parentId
}

// Must have the parentLock R/W Lock()-ed during the call and for the duration the
// returned Inode is used
func (inode *InodeCommon) parent_(c *ctx) Inode {
	defer c.funcIn("InodeCommon::parent_").out()
	parent := c.qfs.inodeNoInstantiate(c, inode.parentId)
	if parent == nil {
		c.elog("Parent was unloaded before child! %d", inode.parentId)
		parent = c.qfs.inode(c, inode.parentId)
	}

	return parent
}

func (inode *InodeCommon) parentMarkAccessed(c *ctx, path string, created bool) {
	defer c.FuncIn("InodeCommon::parentMarkAccessed", "path %s created %t", path,
		created).out()
	defer inode.parentLock.RLock().RUnlock()

	inode.parent_(c).markAccessed(c, path, created)
}

func (inode *InodeCommon) parentSyncChild(c *ctx, childId InodeId,
	publishFn func() quantumfs.ObjectKey) {

	defer c.FuncIn("InodeCommon::parentSyncChild", "%d of %d", childId,
		inode.id).out()

	defer inode.parentLock.RLock().RUnlock()

	// We want to ensure that the orphan check and the parent sync are done
	// under the same lock
	if inode.isOrphaned_() {
		c.vlog("Not flushing orphaned inode")
		return
	}

	// publish before we sync, once we know it's safe
	baseLayerId := publishFn()

	inode.parent_(c).syncChild(c, childId, baseLayerId)
}

func (inode *InodeCommon) parentSetChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("InodeCommon::parentSetChildAttr").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).setChildAttr(c, inodeNum, newType, attr, out,
		updateMtime)
}

func (inode *InodeCommon) parentGetChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("InodeCommon::parentGetChildXAttrSize").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).getChildXAttrSize(c, inodeNum, attr)
}

func (inode *InodeCommon) parentGetChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::parentGetChildXAttrData").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).getChildXAttrData(c, inodeNum, attr)
}

func (inode *InodeCommon) parentListChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::parentListChildXAttr").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).listChildXAttr(c, inodeNum)
}

func (inode *InodeCommon) parentSetChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer c.funcIn("InodeCommon::parentSetChildXAttr").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).setChildXAttr(c, inodeNum, attr, data)
}

func (inode *InodeCommon) parentRemoveChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.funcIn("InodeCommon::parentRemoveChildXAttr").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).removeChildXAttr(c, inodeNum, attr)
}

func (inode *InodeCommon) parentGetChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	defer c.funcIn("InodeCommon::parentGetChildRecordCopy").out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).getChildRecordCopy(c, inodeNum)
}

// When iterating up the directory tree, we need to lock parents as we go,
// otherwise part of the chain we've iterated past could be moved and change
// what we should have returned here
func (inode *InodeCommon) parentHasAncestor(c *ctx, ancestor Inode) bool {
	defer c.FuncIn("InodeCommon::parentHadAncestor", "%d %d", inode.inodeNum(),
		ancestor.inodeNum())

	defer inode.parentLock.RLock().RUnlock()
	if ancestor.inodeNum() == inode.parentId_() {
		return true
	}

	if inode.parentId_() == quantumfs.InodeIdInvalid {
		return false
	}

	toCheck := inode.parent_(c)
	for {
		defer toCheck.getParentLock().RLock().RUnlock()

		if ancestor.inodeNum() == toCheck.parentId_() {
			return true
		}

		if toCheck.parentId_() == quantumfs.InodeIdInvalid {
			return false
		}

		toCheck = toCheck.parent_(c)
	}
}

// Locks the parent
func (inode *InodeCommon) parentCheckLinkReparent(c *ctx, parent *Directory) {
	defer c.FuncIn("InodeCommon::parentCheckLinkReparent", "%d", inode.id).out()

	// Ensure we lock in the UP direction
	defer inode.parentLock.Lock().Unlock()
	defer parent.lock.Lock().Unlock()
	defer parent.childRecordLock.Lock().Unlock()

	// Check if this is still a child
	record := parent.children.record(inode.id)
	if record == nil || record.Type() != quantumfs.ObjectTypeHardlink {
		// no hardlink record here, nothing to do
		return
	}

	link := record.(*Hardlink)

	// This may need to be turned back into a normal file
	newRecord, inodeId := parent.wsr.removeHardlink(c, link.linkId)

	if newRecord == nil && inodeId == quantumfs.InodeIdInvalid {
		// wsr says hardlink isn't ready for removal yet
		return
	}

	// reparent the child to the given parent
	inode.parentId = parent.inodeNum()

	// Ensure that we update this version of the record with this instance
	// of the hardlink's information
	newRecord.SetFilename(link.Filename())

	// Here we do the opposite of makeHardlink DOWN - we re-insert it
	parent.children.loadChild(c, newRecord, inodeId)
	parent.dirty(c)
}

func (inode *InodeCommon) setParent(newParent InodeId) {
	defer inode.parentLock.Lock().Unlock()

	inode.parentId = newParent
}

// Must be called with parentLock locked for writing
func (inode *InodeCommon) setParent_(newParent InodeId) {
	inode.parentId = newParent
}

func (inode *InodeCommon) getParentLock() *utils.DeferableRwMutex {
	return &inode.parentLock
}

func (inode *InodeCommon) isOrphaned() bool {
	defer inode.parentLock.RLock().RUnlock()

	return inode.isOrphaned_()
}

// parentLock must be RLocked
func (inode *InodeCommon) isOrphaned_() bool {
	return inode.id == inode.parentId
}

func (inode *InodeCommon) inodeNum() InodeId {
	return inode.id
}

// Add this Inode to the dirty list
func (inode *InodeCommon) dirty(c *ctx) {
	defer c.funcIn("InodeCommon::dirty").out()
	if inode.isOrphaned() {
		c.vlog("Not dirtying inode %d because it is orphaned", inode.id)
		return
	}

	de := inode.dirtyElement()

	if de == nil {
		c.vlog("Queueing inode %d on dirty list", inode.id)
		de = c.qfs.queueDirtyInode(c, inode.self)

		// queueDirtyInode requests the dirtyElement so we cannot hold the
		// dirtyElementLock over that call.
		defer inode.dirtyElementLock.Lock().Unlock()
		inode.dirtyElement_ = de
	}
}

// Mark this Inode as having been cleaned
func (inode *InodeCommon) markClean() {
	defer inode.dirtyElementLock.Lock().Unlock()
	inode.dirtyElement_ = nil
}

func (inode *InodeCommon) dirtyChild(c *ctx, child InodeId) {
	msg := fmt.Sprintf("Unsupported dirtyChild() call on Inode %d: %v", child,
		inode)
	panic(msg)
}

func (inode *InodeCommon) queueToForget(c *ctx) {
	defer c.funcIn("InodeCommon::queueToForget").out()
	de := c.qfs.queueInodeToForget(c, inode.self)

	defer inode.dirtyElementLock.Lock().Unlock()
	inode.dirtyElement_ = de
}

func (inode *InodeCommon) dirtyElement() *list.Element {
	defer inode.dirtyElementLock.Lock().Unlock()
	return inode.dirtyElement_
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

func (inode *InodeCommon) Lock() utils.NeedWriteUnlock {
	return inode.lock.Lock()
}

func (inode *InodeCommon) RLock() utils.NeedReadUnlock {
	return inode.lock.RLock()
}

func (inode *InodeCommon) markAccessed(c *ctx, path string, created bool) {
	defer c.FuncIn("InodeCommon::markAccessed", "path %s created %t", path,
		created).out()
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
	inode.parentMarkAccessed(c, path, created)
}

func (inode *InodeCommon) markSelfAccessed(c *ctx, created bool) {
	defer c.FuncIn("InodeCommon::markSelfAccessed", "created %t", created).out()
	ac := inode.accessed()
	if !created && ac {
		return
	}
	inode.self.markAccessed(c, "", created)
}

func (inode *InodeCommon) isWorkspaceRoot() bool {
	_, isWsr := inode.self.(*WorkspaceRoot)
	return isWsr
}

// Deleting a child may require that we orphan it, and because we *must* lock from
// a child up to its parent outside of a DOWN function, deletion in the parent
// must be done after the child's lock has been acquired.
func (inode *InodeCommon) deleteSelf(c *ctx, toDelete Inode,
	deleteFromParent func() (toOrphan quantumfs.DirectoryRecord,
		err fuse.Status)) fuse.Status {

	defer c.FuncIn("InodeCommon::deleteSelf", "%d", toDelete.inodeNum()).out()

	defer inode.lock.Lock().Unlock()

	// We must perform the deletion with the lockedParent lock
	defer inode.parentLock.Lock().Unlock()

	// After we've locked the child, we can safely go UP and lock our parent
	toOrphan, err := deleteFromParent()
	if toOrphan == nil {
		// no orphan-ing desired here (hardlink or error)
		return err
	}

	if file, isFile := toDelete.(*File); isFile {
		file.setChildRecord(c, toOrphan)
	}
	// orphan ourselves
	inode.parentId = toDelete.inodeNum()
	c.vlog("Orphaned inode %d", toDelete.inodeNum())

	return fuse.OK
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
