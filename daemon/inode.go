// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// The basic Inode and FileHandle structures

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

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
	setChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
		out *fuse.AttrOut, updateMtime bool) fuse.Status

	getChildRecordCopy(c *ctx,
		inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error)

	// Update the key for only this child
	syncChild(c *ctx, inodeNum InodeId, newKey quantumfs.ObjectKey,
		newType quantumfs.ObjectType)

	setChildRecord(c *ctx, record quantumfs.DirectoryRecord)

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

	// Returns true if the inode had previously been marked accessed for the
	// passed-in read/update operation. This also stores the union of read/update
	// operations for future reference.
	accessedFor(op quantumfs.PathFlags) bool

	// Clear the cached value of accessed. This should be used if the previous
	// path of the inode has become invalid, perhaps because the inode has been
	// renamed.
	clearAccessedCache()
	markAccessed(c *ctx, path string, op quantumfs.PathFlags)
	markSelfAccessed(c *ctx, op quantumfs.PathFlags)

	absPath(c *ctx, path string) string
	absPath_(c *ctx, path string) string

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
	orphan(c *ctx, record quantumfs.DirectoryRecord)
	orphan_(c *ctx, record quantumfs.DirectoryRecord)
	deleteSelf(c *ctx,
		deleteFromParent func() (toOrphan quantumfs.DirectoryRecord,
			err fuse.Status)) fuse.Status

	parentMarkAccessed(c *ctx, path string, op quantumfs.PathFlags)
	parentSyncChild(c *ctx, publishFn func() (quantumfs.ObjectKey,
		quantumfs.ObjectType))
	parentSetChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
		out *fuse.AttrOut, updateMtime bool) fuse.Status
	parentUpdateSize(c *ctx, getSize_ func() uint64) fuse.Status
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
		inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error)
	parentHasAncestor(c *ctx, ancestor Inode) bool
	parentCheckLinkReparent(c *ctx, parent *Directory)

	dirty(c *ctx)              // Mark this Inode dirty
	markClean_() *list.Element // Mark this Inode as cleaned
	// Undo marking the inode as clean
	markUnclean_(dirtyElement *list.Element) bool
	// Mark this Inode dirty because a child is dirty
	dirtyChild(c *ctx, child InodeId)

	// The kernel has forgotten about this Inode. Add yourself to the list to be
	// flushed and forgotten.
	queueToForget(c *ctx)

	// Returns this inode's place in the dirtyQueue
	dirtyElement_() *list.Element

	// Compute a new object key, schedule the object data to be uploaded to the
	// datastore and update the parent with the new key.
	flush(c *ctx) quantumfs.ObjectKey

	Sync_DOWN(c *ctx) fuse.Status
	link_DOWN(c *ctx, srcInode Inode, newName string,
		out *fuse.EntryOut) fuse.Status

	inodeNum() InodeId

	Lock() utils.NeedWriteUnlock

	treeLock() *TreeLock
	LockTree() *TreeLock
	RLockTree() *TreeLock

	isWorkspaceRoot() bool

	// cleanup() is called when the Inode has been uninstantiated, but before the
	// final reference has been released. It should perform any deterministic
	// cleanup which is necessary, but it is possible for the Inode to be
	// accessed after cleanup() has completed.
	cleanup(c *ctx)
}

type inodeHolder interface {
	directChildInodes() []InodeId
}

type TreeLock struct {
	lock *sync.RWMutex
	name string
}

func (treelock *TreeLock) Unlock() {
	treelock.lock.Unlock()
}

func (treelock *TreeLock) RUnlock() {
	treelock.lock.RUnlock()
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
	treeLock_ *TreeLock

	// This element is protected by the flusher lock
	dirtyElement__ *list.Element

	unlinkRecord quantumfs.DirectoryRecord
	unlinkXAttr  map[string][]byte
	unlinkLock   utils.DeferableRwMutex
}

// Must have the parentLock R/W Lock()-ed during the call and for the duration the
// id is used
func (inode *InodeCommon) parentId_() InodeId {
	return inode.parentId
}

// Must have the parentLock R/W Lock()-ed during the call and for the duration the
// returned Inode is used
func (inode *InodeCommon) parent_(c *ctx) Inode {
	defer c.funcIn("InodeCommon::parent_").Out()
	parent := c.qfs.inodeNoInstantiate(c, inode.parentId)
	if parent == nil {
		c.elog("Parent (%d) was unloaded before child (%d)!",
			inode.parentId, inode.id)
		parent = c.qfs.inode(c, inode.parentId)
	}

	return parent
}

func (inode *InodeCommon) parentMarkAccessed(c *ctx, path string,
	op quantumfs.PathFlags) {

	defer c.FuncIn("InodeCommon::parentMarkAccessed", "path %s CRUD %x", path,
		op).Out()

	defer inode.parentLock.RLock().RUnlock()

	parent := inode.parent_(c)
	if wsr, isWorkspaceRoot := parent.(*WorkspaceRoot); isWorkspaceRoot {
		isHardlink, fileId := wsr.checkHardlink(inode.id)
		if isHardlink {
			wsr.markHardlinkAccessed(c, fileId, op)
			return
		}
	}

	parent.markAccessed(c, path, op)
}

func (inode *InodeCommon) parentSyncChild(c *ctx,
	publishFn func() (quantumfs.ObjectKey, quantumfs.ObjectType)) {

	defer c.FuncIn("InodeCommon::parentSyncChild", "%d", inode.id).Out()

	defer inode.parentLock.RLock().RUnlock()
	defer inode.Lock().Unlock()

	// We want to ensure that the orphan check and the parent sync are done
	// under the same lock
	if inode.isOrphaned_() {
		c.vlog("Not flushing orphaned inode")
		return
	}

	// publish before we sync, once we know it's safe
	baseLayerId, objectType := publishFn()

	inode.parent_(c).syncChild(c, inode.id, baseLayerId, objectType)
}

func (inode *InodeCommon) parentUpdateSize(c *ctx,
	getSize_ func() uint64) fuse.Status {

	defer c.funcIn("InodeCommon::parentUpdateSize").Out()

	defer inode.parentLock.RLock().RUnlock()
	defer inode.lock.Lock().Unlock()

	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = getSize_()
	return inode.parent_(c).setChildAttr(c, inode.inodeNum(), &attr, nil, true)
}

func (inode *InodeCommon) parentSetChildAttr(c *ctx, inodeNum InodeId,
	attr *fuse.SetAttrIn, out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("InodeCommon::parentSetChildAttr").Out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).setChildAttr(c, inodeNum, attr, out, updateMtime)
}

func (inode *InodeCommon) parentGetChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("InodeCommon::parentGetChildXAttrSize").Out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).getChildXAttrSize(c, inodeNum, attr)
}

func (inode *InodeCommon) parentGetChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::parentGetChildXAttrData").Out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).getChildXAttrData(c, inodeNum, attr)
}

func (inode *InodeCommon) parentListChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.funcIn("InodeCommon::parentListChildXAttr").Out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).listChildXAttr(c, inodeNum)
}

func (inode *InodeCommon) parentSetChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer c.funcIn("InodeCommon::parentSetChildXAttr").Out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).setChildXAttr(c, inodeNum, attr, data)
}

func (inode *InodeCommon) parentRemoveChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.funcIn("InodeCommon::parentRemoveChildXAttr").Out()

	defer inode.parentLock.RLock().RUnlock()
	return inode.parent_(c).removeChildXAttr(c, inodeNum, attr)
}

func (inode *InodeCommon) parentGetChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	defer c.funcIn("InodeCommon::parentGetChildRecordCopy").Out()

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
	defer c.FuncIn("InodeCommon::parentCheckLinkReparent", "%d", inode.id).Out()

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

	link := record.(*HardlinkLeg)

	// This may need to be turned back into a normal file
	newRecord, inodeId := parent.hardlinkTable.removeHardlink(c, link.FileId())

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

func (inode *InodeCommon) orphan(c *ctx, record quantumfs.DirectoryRecord) {
	defer inode.parentLock.Lock().Unlock()
	inode.orphan_(c, record)
}

// parentLock must be Locked
func (inode *InodeCommon) orphan_(c *ctx, record quantumfs.DirectoryRecord) {
	defer c.FuncIn("InodeCommon::orphan_", "inode %d", inode.inodeNum()).Out()

	inode.parentId = inode.id
	inode.setChildRecord(c, record)
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
	defer c.funcIn("InodeCommon::dirty").Out()
	defer c.qfs.flusher.lock.Lock().Unlock()
	if inode.dirtyElement__ == nil {
		c.vlog("Queueing inode %d on dirty list", inode.id)
		inode.dirtyElement__ = c.qfs.queueDirtyInode_(c, inode.self)
	}
}

// Mark this Inode as having been cleaned
// flusher lock must be locked when calling this function
func (inode *InodeCommon) markClean_() *list.Element {
	dirtyElement := inode.dirtyElement__
	inode.dirtyElement__ = nil
	return dirtyElement
}

// Undo marking this inode as clean
// flusher lock must be locked when calling this function
func (inode *InodeCommon) markUnclean_(dirtyElement *list.Element) (already bool) {
	if inode.dirtyElement__ == nil {
		inode.dirtyElement__ = dirtyElement
		return false
	}
	return true
}

func (inode *InodeCommon) dirtyChild(c *ctx, child InodeId) {
	msg := fmt.Sprintf("Unsupported dirtyChild() call on Inode %d: %v", child,
		inode)
	panic(msg)
}

func (inode *InodeCommon) queueToForget(c *ctx) {
	defer c.funcIn("InodeCommon::queueToForget").Out()

	defer c.qfs.flusher.lock.Lock().Unlock()
	de := c.qfs.queueInodeToForget_(c, inode.self)
	inode.dirtyElement__ = de
}

// flusher lock must be locked when calling this function
func (inode *InodeCommon) dirtyElement_() *list.Element {
	return inode.dirtyElement__
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

func (inode *InodeCommon) accessedFor(op quantumfs.PathFlags) bool {
	for {
		old := atomic.LoadUint32(&(inode.accessed_))
		new := old | uint32(op)

		if old == new {
			return true
		}

		if atomic.CompareAndSwapUint32(&(inode.accessed_), old, new) {
			return false
		}
	}
}

func (inode *InodeCommon) clearAccessedCache() {
	atomic.StoreUint32(&(inode.accessed_), 0)
}

func (inode *InodeCommon) treeLock() *TreeLock {
	return inode.treeLock_
}

func (inode *InodeCommon) generation() uint64 {
	return 0
}

func (inode *InodeCommon) LockTree() *TreeLock {
	inode.treeLock_.lock.Lock()
	return inode.treeLock_
}

func (inode *InodeCommon) RLockTree() *TreeLock {
	inode.treeLock_.lock.RLock()
	return inode.treeLock_
}

func (inode *InodeCommon) Lock() utils.NeedWriteUnlock {
	return inode.lock.Lock()
}

func (inode *InodeCommon) RLock() utils.NeedReadUnlock {
	return inode.lock.RLock()
}

// the inode parentLock must be locked
func (inode *InodeCommon) absPath_(c *ctx, path string) string {
	defer c.FuncIn("InodeCommon::absPath_", "path %s", path).Out()

	if path == "" {
		path = inode.name()
	} else {
		path = inode.name() + "/" + path
	}
	return inode.parent_(c).absPath(c, path)
}

func (inode *InodeCommon) absPath(c *ctx, path string) string {
	defer c.FuncIn("InodeCommon::absPath", "path %s", path).Out()
	if inode.isWorkspaceRoot() {
		return "/" + path
	}
	if inode.isOrphaned() {
		panic("Orphaned file")
	}

	defer inode.parentLock.RLock().RUnlock()
	return inode.absPath_(c, path)
}

func (inode *InodeCommon) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {
	defer c.FuncIn("InodeCommon::markAccessed", "path %s CRUD %x", path,
		op).Out()
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
	inode.parentMarkAccessed(c, path, op)
}

func (inode *InodeCommon) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	defer c.FuncIn("InodeCommon::markSelfAccessed", "CRUD %x", op).Out()
	if inode.isOrphaned() {
		c.vlog("Orphaned, not marking")
		return
	}

	if inode.accessedFor(op) {
		// This inode has already been marked accessed for these operations
		// so we can short circuit here.
		return
	}

	inode.self.markAccessed(c, "", op)
}

func (inode *InodeCommon) isWorkspaceRoot() bool {
	_, isWsr := inode.self.(*WorkspaceRoot)
	return isWsr
}

// Deleting a child may require that we orphan it, and because we *must* lock from
// a child up to its parent outside of a DOWN function, deletion in the parent
// must be done after the child's lock has been acquired.
func (inode *InodeCommon) deleteSelf(c *ctx,
	deleteFromParent func() (toOrphan quantumfs.DirectoryRecord,
		err fuse.Status)) fuse.Status {

	defer c.FuncIn("InodeCommon::deleteSelf", "%d", inode.inodeNum()).Out()
	defer inode.parentLock.Lock().Unlock()
	defer inode.lock.Lock().Unlock()

	// One of this inode's names is going away, reset the accessed cache to
	// ensure any remaining names are marked correctly.
	inode.clearAccessedCache()

	// After we've locked the child, we can safely go UP and lock our parent
	toOrphan, err := deleteFromParent()

	if toOrphan != nil {
		// toOrphan can be nil if this is a hardlink or there was an error
		inode.orphan_(c, toOrphan)
	}

	return err
}

func (inode *InodeCommon) cleanup(c *ctx) {
	defer c.funcIn("InodeCommon::cleanup").Out()
	// Most inodes have nothing to do here
}

func reload(c *ctx, hardlinkTable HardlinkTable, rc *RefreshContext, inode Inode,
	remoteRecord quantumfs.ImmutableDirectoryRecord) {

	defer c.FuncIn("reload", "%s: %d", remoteRecord.Filename(),
		remoteRecord.Type()).Out()

	switch remoteRecord.Type() {
	default:
		panic(fmt.Sprintf("Reload unsupported on files of type %d",
			remoteRecord.Type()))
	case quantumfs.ObjectTypeSpecial:
		panic("special files cannot be reloaded.")
	case quantumfs.ObjectTypeSymlink:
		panic("symlinks cannot be reloaded.")
	case quantumfs.ObjectTypeDirectory:
		subdir := inode.(*Directory)
		subdir.refresh_DOWN(c, rc, remoteRecord.ID())
	case quantumfs.ObjectTypeHardlink:
		fileId := remoteRecord.FileId()
		valid, hardlinkRecord := hardlinkTable.getHardlink(fileId)
		utils.Assert(valid, "hardlink %d not found", fileId)
		remoteRecord = hardlinkRecord
		fallthrough
	case quantumfs.ObjectTypeSmallFile:
		fallthrough
	case quantumfs.ObjectTypeMediumFile:
		fallthrough
	case quantumfs.ObjectTypeLargeFile:
		fallthrough
	case quantumfs.ObjectTypeVeryLargeFile:
		regFile := inode.(*File)
		regFile.handleAccessorTypeChange(c, remoteRecord)
	}
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

	treeLock() *TreeLock
	LockTree() *TreeLock
	RLockTree() *TreeLock
}

type FileHandleId uint64

type FileHandleCommon struct {
	id        FileHandleId
	inodeNum  InodeId
	treeLock_ *TreeLock
}

func (file *FileHandleCommon) treeLock() *TreeLock {
	return file.treeLock_
}

func (file *FileHandleCommon) LockTree() *TreeLock {
	file.treeLock_.lock.Lock()
	return file.treeLock_
}

func (file *FileHandleCommon) RLockTree() *TreeLock {
	file.treeLock_.lock.RLock()
	return file.treeLock_
}
