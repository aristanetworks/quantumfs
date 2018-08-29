// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// If dirRecord is nil, then mode, rdev and dirRecord are invalid, but the key is
// coming from a DirRecord and not passed in from create_.
//
// The return value is the newly instantiated Inode, and a list of InodeIds which
// should be added to the mux's uninstantiatedInodes collection with this new inode
// as their parent.
type InodeConstructor func(c *ctx, name string, key quantumfs.ObjectKey,
	size uint64, inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) Inode

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon

	hardlinkTable HardlinkTable
	hardlinkDelta *HardlinkDelta

	// These fields are protected by the InodeCommon.lock
	baseLayerId quantumfs.ObjectKey

	// childRecordLock protects the maps inside childMap as well as the
	// records contained within those maps themselves. This lock is not
	// the same as the Directory Inode lock because these records must be
	// accessible in instantiateChild(), which may be called indirectly
	// via qfs.inode() from a context where the Inode lock is already
	// held.
	childRecordLock utils.DeferableMutex
	children        *ChildContainer
	_generation     uint64

	childSnapshot      []directoryContents
	snapshotGeneration uint64
}

func foreachDentry(c *ctx, key quantumfs.ObjectKey,
	visitor func(quantumfs.ImmutableDirectoryRecord)) {

	for {
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic("No baseLayer object")
		}
		baseLayer := MutableCopy(c, buffer).AsDirectoryEntry()

		for i := 0; i < baseLayer.NumEntries(); i++ {
			visitor(baseLayer.Entry(i))
		}

		if baseLayer.HasNext() {
			key = baseLayer.Next()
		} else {
			break
		}
	}
}

func initDirectory(c *ctx, name string, dir *Directory,
	hardlinkTable HardlinkTable,
	baseLayerId quantumfs.ObjectKey, inodeNum InodeId,
	parent Inode, treeState *TreeState) {

	defer c.FuncIn("initDirectory",
		"baselayer from %s", baseLayerId.String()).Out()

	// Set directory data before processing the children in case the children
	// access the parent.
	dir.InodeCommon.id = inodeNum
	dir.InodeCommon.name_ = name
	dir.InodeCommon.accessed_ = 0
	dir.setParent(c, parent)
	dir.treeState_ = treeState
	dir.hardlinkTable = hardlinkTable
	dir.baseLayerId = baseLayerId
	dir.hardlinkDelta = newHardlinkDelta()

	// childRecordLock is locked initially. It will be unlocked in
	// finishInit.
	// finishInit is the bottom half of the initialization which
	// is delayed to speed up initDirectory().
	dir.childRecordLock.Lock()

	utils.Assert(dir.treeState() != nil, "Directory treeState nil at init")
}

func (dir *Directory) finishInit(c *ctx) (uninstantiated []inodePair) {
	defer c.funcIn("Directory::finishInit").Out()
	defer dir.childRecordLock.Unlock()

	utils.Assert(dir.children == nil, "children already loaded")

	wsrInode := dir.hardlinkTable.getWorkspaceRoot().inodeNum()
	// pre-set child container to a safe instance to ensure we don't leave it nil
	dir.children, uninstantiated = newChildContainer(c, dir,
		quantumfs.EmptyDirKey, wsrInode)

	// now attempt to load the children, which may panic / fail
	dir.children, uninstantiated = newChildContainer(c, dir, dir.baseLayerId,
		wsrInode)
	return uninstantiated
}

func newDirectory(c *ctx, name string, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) Inode {

	defer c.funcIn("Directory::newDirectory").Out()

	var dir Directory
	dir.self = &dir

	var hardlinkTable HardlinkTable
	switch v := parent.(type) {
	case *Directory:
		hardlinkTable = v.hardlinkTable
	case *WorkspaceRoot:
		hardlinkTable = v.hardlinkTable
	default:
		panic(fmt.Sprintf("Parent of inode %d is neither "+
			"Directory nor WorkspaceRoot", inodeNum))
	}

	initDirectory(c, name, &dir, hardlinkTable, baseLayerId, inodeNum, parent,
		parent.treeState())
	return &dir
}

func (dir *Directory) generation() uint64 {
	return atomic.LoadUint64(&dir._generation)
}

func (dir *Directory) dirty(c *ctx) {
	atomic.AddUint64(&dir._generation, 1)
	dir.InodeCommon.dirty(c)
}

func (dir *Directory) updateSize(c *ctx, result fuse.Status) {
	defer c.funcIn("Directory::updateSize").Out()

	if result != fuse.OK {
		// The last operation did not succeed, do not dirty the directory
		// nor update its size
		return
	}
	// We think we've made a change to this directory, so we should mark it dirty
	dir.self.dirty(c)

	// The parent of a WorkspaceRoot is a workspacelist and we have nothing to
	// update.
	if dir.self.isWorkspaceRoot() {
		return
	}

	dir.parentUpdateSize(c, func() uint64 {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.count()
	})
}

// Directory inode lock must be held to protect the hardlink deltas
func (dir *Directory) prepareForOrphaning_(c *ctx, name string,
	record quantumfs.DirectoryRecord) quantumfs.DirectoryRecord {

	defer c.FuncIn("Directory::prepareForOrphaning_", "%s", name).Out()
	if record.Type() != quantumfs.ObjectTypeHardlink {
		return record
	}
	newRecord := dir.hardlinkDec_(record.FileId())
	if newRecord != nil {
		// This was the last leg of the hardlink
		newRecord.SetFilename(name)
	}
	return newRecord
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx,
	name string) (toOrphan quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::delChild_").Out()

	c.vlog("Unlinking inode %s", name)

	// If this is a file we need to reparent it to itself
	record := func() quantumfs.DirectoryRecord {
		defer dir.childRecordLock.Lock().Unlock()
		detachedChild := dir.children.deleteChild(c, name)
		return dir.prepareForOrphaning_(c, name, detachedChild)
	}()

	return record
}

func fillAttrWithDirectoryRecord(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	owner fuse.Owner, entry quantumfs.ImmutableDirectoryRecord) {

	attr.Ino = uint64(inodeNum)

	entryType := underlyingType(entry)
	fileType := _objectTypeToFileType(c, entryType)

	switch fileType {
	case fuse.S_IFDIR:
		// Approximate the read size of the Directory objects in the
		// datastore. Accurately summing the size might require looking at
		// the size of several blocks for not much gain over this simple
		// linear approximately based upon the design document fixed field
		// sizes, even though the real encoding is variable length.
		attr.Size = 25 + 331*entry.Size()
		attr.Blocks = utils.BlocksRoundUp(attr.Size, statBlockSize)
		attr.Nlink = uint32(entry.Size()) + 2
	case fuse.S_IFIFO:
		fileType = specialOverrideAttr(entry, attr)
		if fileType&^syscall.S_IFMT != 0 {
			c.elog("fileType has permission bits set %x", fileType)
			fileType &= syscall.S_IFMT
		}

	default:
		c.elog("Unhandled filetype %x in fillAttrWithDirectoryRecord",
			fileType)
		fallthrough
	case fuse.S_IFREG,
		fuse.S_IFLNK:

		// This ignore the datablocks containing the file metadata, which is
		// relevant for medium, large and very large files.
		attr.Size = entry.Size()
		attr.Blocks = utils.BlocksRoundUp(entry.Size(), statBlockSize)
		attr.Nlink = entry.Nlinks()
	}

	attr.Atime = entry.ModificationTime().Seconds()
	attr.Mtime = entry.ModificationTime().Seconds()
	attr.Ctime = entry.ContentTime().Seconds()
	attr.Atimensec = entry.ModificationTime().Nanoseconds()
	attr.Mtimensec = entry.ModificationTime().Nanoseconds()
	attr.Ctimensec = entry.ContentTime().Nanoseconds()
	attr.Mode = fileType | permissionsToMode(entry.Permissions())
	if c.config.MagicOwnership {
		attr.Owner.Uid = quantumfs.SystemUid(entry.Owner(), owner.Uid)
		attr.Owner.Gid = quantumfs.SystemGid(entry.Group(), owner.Gid)
	} else {
		attr.Owner.Uid = quantumfs.SystemUid(entry.Owner(),
			quantumfs.UniversalUID)
		attr.Owner.Gid = quantumfs.SystemGid(entry.Group(),
			quantumfs.UniversalGID)
	}
	attr.Blksize = uint32(qfsBlockSize)
}

func permissionsToMode(permissions uint32) uint32 {
	var mode uint32

	if utils.BitFlagsSet(uint(permissions), quantumfs.PermExecOther) {
		mode |= syscall.S_IXOTH
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermWriteOther) {
		mode |= syscall.S_IWOTH
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermReadOther) {
		mode |= syscall.S_IROTH
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermExecGroup) {
		mode |= syscall.S_IXGRP
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermWriteGroup) {
		mode |= syscall.S_IWGRP
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermReadGroup) {
		mode |= syscall.S_IRGRP
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermExecOwner) {
		mode |= syscall.S_IXUSR
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermWriteOwner) {
		mode |= syscall.S_IWUSR
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermReadOwner) {
		mode |= syscall.S_IRUSR
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermSticky) {
		mode |= syscall.S_ISVTX
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermSGID) {
		mode |= syscall.S_ISGID
	}
	if utils.BitFlagsSet(uint(permissions), quantumfs.PermSUID) {
		mode |= syscall.S_ISUID
	}

	return mode
}

func modeToPermissions(mode uint32, umask uint32) uint32 {
	var permissions uint32
	mode = mode & ^umask

	if utils.BitFlagsSet(uint(mode), syscall.S_IXOTH) {
		permissions |= quantumfs.PermExecOther
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IWOTH) {
		permissions |= quantumfs.PermWriteOther
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IROTH) {
		permissions |= quantumfs.PermReadOther
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IXGRP) {
		permissions |= quantumfs.PermExecGroup
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IWGRP) {
		permissions |= quantumfs.PermWriteGroup
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IRGRP) {
		permissions |= quantumfs.PermReadGroup
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IXUSR) {
		permissions |= quantumfs.PermExecOwner
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IWUSR) {
		permissions |= quantumfs.PermWriteOwner
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_IRUSR) {
		permissions |= quantumfs.PermReadOwner
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_ISVTX) {
		permissions |= quantumfs.PermSticky
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_ISGID) {
		permissions |= quantumfs.PermSGID
	}
	if utils.BitFlagsSet(uint(mode), syscall.S_ISUID) {
		permissions |= quantumfs.PermSUID
	}

	return permissions
}

func publishDirectoryEntry(c *ctx, layer *quantumfs.DirectoryEntry,
	nextKey quantumfs.ObjectKey, pub publishFn) quantumfs.ObjectKey {

	defer c.funcIn("publishDirectoryEntry").Out()

	layer.SetNext(nextKey)
	bytes := layer.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newKey, err := pub(c, buf)
	utils.Assert(err == nil, "Failed to upload new baseLayer object: %v", err)

	return newKey
}

func publishDirectoryRecords(c *ctx,
	records []quantumfs.DirectoryRecord, pub publishFn) quantumfs.ObjectKey {

	defer c.funcIn("publishDirectoryRecords").Out()

	numEntries := len(records)

	// Compile the internal records into a series of blocks which can be placed
	// in the datastore.
	newBaseLayerId := quantumfs.EmptyDirKey

	numEntries, baseLayer := quantumfs.NewDirectoryEntry(numEntries)
	entryIdx := 0
	quantumfs.SortDirectoryRecords(records)

	for _, child := range records {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			c.vlog("Block full with %d entries", entryIdx)
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId, pub)
			numEntries, baseLayer =
				quantumfs.NewDirectoryEntry(numEntries)
			entryIdx = 0
		}
		baseLayer.SetEntry(entryIdx, child.Publishable())

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId, pub)
	return newBaseLayerId
}

func (dir *Directory) normalizeChild(c *ctx, inodeId InodeId,
	records [2]quantumfs.DirectoryRecord) {

	defer c.FuncIn("Directory::normalizeChild", "%d", inodeId).Out()

	defer c.qfs.instantiationLock.Lock().Unlock()
	inode := c.qfs.inodeNoInstantiate(c, inodeId)
	if inode != nil {
		defer inode.getParentLock().Lock().Unlock()
	}
	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()
	defer c.qfs.flusher.lock.Lock().Unlock()

	leg := dir.children.recordByInodeId(c, inodeId)
	if leg == nil {
		c.vlog("inode %d is no longer with us", inodeId)
		return
	}
	name := leg.Filename()

	if inode != nil && inode.isDirty_(c) {
		c.vlog("Will not normalize dirty inode %d yet", inodeId)
		return
	}

	fileId := records[0].FileId()
	func() {
		defer dir.hardlinkTable.invalidateNormalizedRecordLock(
			fileId).Unlock()

		if inode != nil {
			c.vlog("Setting parent of inode %d from %d to %d",
				inodeId, inode.parentId_(), dir.inodeNum())
			inode.setName(name)
			inode.setParent_(c, dir)
		} else {
			c.qfs.addUninstantiated(c, []inodePair{
				newInodePair(inodeId, dir.inodeNum()),
			})
		}
	}()

	c.vlog("Normalizing child %s inode %d", name, inodeId)

	inodeIdInfo := dir.children.inodeNum(name)

	// Bubble up the -1 as we are inheriting the hardlink
	// from the root now.
	dir.hardlinkDec_(fileId)
	dir.children.deleteChild(c, name)

	records[0].SetFilename(name)
	dir.children.setRecord(c, inodeIdInfo, records[0])
	dir.children.makePublishable(c, name)

	// We don't normalize dirty children above, so there should be no unpublished
	// effective changes here.
	utils.Assert(records[1] == nil, "Child with unpublished changes")
}

func (dir *Directory) getNormalizationCandidates(c *ctx) (
	result map[InodeId][2]quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::getNormalizationCandidates").Out()

	defer dir.Lock().Unlock()
	defer dir.childRecordLock.Lock().Unlock()

	result = make(map[InodeId][2]quantumfs.DirectoryRecord)
	records := dir.children.publishableRecords(c)
	for _, record := range records {
		if record.Type() != quantumfs.ObjectTypeHardlink {
			continue
		}
		publishable, effective := dir.hardlinkTable.getNormalized(
			record.FileId())
		if publishable != nil {
			inodeId := dir.children.inodeNum(record.Filename())
			result[inodeId.id] = [2]quantumfs.DirectoryRecord{
				publishable, effective}
		}
	}
	return
}

// Must hold the dir.childRecordsLock
func (dir *Directory) publish_(c *ctx) {
	defer c.FuncIn("Directory::publish_", "%s", dir.name_).Out()

	oldBaseLayer := dir.baseLayerId
	dir.baseLayerId = publishDirectoryRecords(c,
		dir.children.publishableRecords(c), publishNow)

	c.vlog("Directory key %s -> %s", oldBaseLayer.String(),
		dir.baseLayerId.String())
}

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("Directory::setChildAttr").Out()

	if c.fuseCtx != nil && utils.BitFlagsSet(uint(attr.Valid), fuse.FATTR_UID) {
		userUid := c.fuseCtx.Owner.Uid
		if userUid != 0 && userUid != attr.Owner.Uid {
			c.vlog("Non-root cannot change UID")
			return fuse.EPERM
		}
	}

	result := func() fuse.Status {
		defer dir.Lock().Unlock()
		defer dir.childRecordLock.Lock().Unlock()

		modify := func(record quantumfs.DirectoryRecord) {
			modifyEntryWithAttr(c, newType, attr, record,
				updateMtime)
		}

		entry := dir.children.recordByInodeId(c, inodeNum)
		if entry != nil && entry.Type() != quantumfs.ObjectTypeHardlink {
			dir.children.modifyChildWithFunc(c, inodeNum, modify)
			entry = dir.children.recordByInodeId(c, inodeNum)
		} else {
			// If we don't have the child, maybe we're the WSR and it's a
			// hardlink. If we aren't the WSR, then it must be a
			// hardlink.
			c.vlog("Checking hardlink table")
			dir.hardlinkTable.modifyChildWithFunc(c, inodeNum, modify)
			entry = dir.hardlinkTable.recordByInodeId(c, inodeNum)
		}

		if entry == nil {
			return fuse.ENOENT
		}

		if out != nil {
			fillAttrOutCacheData(c, out)
			fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
				c.fuseCtx.Owner, entry)
		}

		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.funcIn("Directory::Access").Out()

	return hasAccessPermission(c, dir, mask, uid, gid)
}

func (dir *Directory) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	defer c.funcIn("Directory::Lookup").Out()

	defer dir.RLock().RUnlock()

	inodeNum, inodeGen := func() (InodeId, uint64) {
		defer dir.childRecordLock.Lock().Unlock()
		rtn := dir.children.inodeNum(name)
		return rtn.id, rtn.generation
	}()
	if inodeNum == quantumfs.InodeIdInvalid {
		c.vlog("Inode not found")
		return kernelCacheNegativeEntry(c, out)
	}

	c.vlog("Directory::Lookup found inode %d", inodeNum)
	c.qfs.incrementLookupCount(c, inodeNum)

	out.NodeId = uint64(inodeNum)
	out.Generation = inodeGen
	fillEntryOutCacheData(c, out)
	defer dir.childRecordLock.Lock().Unlock()
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		dir.children.recordByName(c, name))

	return fuse.OK
}

func (dir *Directory) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("Directory::Open doing nothing")
	return fuse.ENOSYS
}

const OpenedInodeDebug = "Opened inode %d as Fh %d"

func (dir *Directory) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("Directory::OpenDir").Out()

	err := hasPermissionOpenFlags(c, dir, flags)
	if err != fuse.OK {
		return err
	}

	ds := newDirectorySnapshot(c, dir.self.(directorySnapshotSource))
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	c.vlog(OpenedInodeDebug, dir.inodeNum(), ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func cloneChildSnapshot(original []directoryContents) []directoryContents {
	clone := make([]directoryContents, len(original))
	copy(clone, original)
	return clone
}

func (dir *Directory) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("Directory::getChildSnapshot").Out()

	dir.self.markSelfAccessed(c, quantumfs.PathRead|quantumfs.PathIsDir)

	defer dir.RLock().RUnlock()

	if dir.childSnapshot != nil && dir.snapshotGeneration == dir.generation() {
		c.vlog("Returning cached child snapshot")
		return cloneChildSnapshot(dir.childSnapshot)
	}

	c.vlog("Adding .")
	selfInfo := directoryContents{
		filename: ".",
	}

	// If we are a WorkspaceRoot then we need only add some entries,
	// WorkspaceRoot.getChildSnapShot() will overwrite the first two entries with
	// the correct data.
	if !dir.self.isWorkspaceRoot() {
		dir.parentGetChildAttr(c, dir.inodeNum(), &selfInfo.attr,
			c.fuseCtx.Owner)
		selfInfo.fuseType = selfInfo.attr.Mode
	}

	c.vlog("Adding ..")
	parentInfo := directoryContents{
		filename: "..",
	}

	if !dir.self.isWorkspaceRoot() {
		func() {
			defer dir.parentLock.RLock().RUnlock()
			parent, release := dir.parent_(c)
			defer release()

			if parent.isWorkspaceRoot() {
				wsr := parent.(*WorkspaceRoot)
				wsr.fillWorkspaceAttrReal(c, &parentInfo.attr)
			} else {
				c.vlog("Got record from grandparent")
				parent.parentGetChildAttr(c, parent.inodeNum(),
					&parentInfo.attr, c.fuseCtx.Owner)
			}
			parentInfo.fuseType = parentInfo.attr.Mode
		}()
	}

	c.vlog("Adding real children")

	defer dir.childRecordLock.Lock().Unlock()
	records := dir.children.records()

	children := make([]directoryContents, len(records)+2)
	children[0] = selfInfo
	children[1] = parentInfo

	i := 2
	for filename, entry := range records {
		children[i].filename = filename
		fillAttrWithDirectoryRecord(c, &children[i].attr,
			dir.children.inodeNum(filename).id, c.fuseCtx.Owner, entry)
		children[i].fuseType = children[i].attr.Mode
		children[i].generation = dir.children.inodeNum(filename).generation
		i++
	}

	// Sort the dentries so that their order is deterministic
	// on every invocation
	sort.Slice(children,
		func(i, j int) bool {
			if children[i].attr.Ino == children[j].attr.Ino {
				return children[i].filename < children[j].filename
			}
			return children[i].attr.Ino < children[j].attr.Ino
		})

	dir.childSnapshot = children
	dir.snapshotGeneration = dir.generation()

	return cloneChildSnapshot(children)
}

// Needs inode lock for write
func (dir *Directory) create_(c *ctx, name string, mode uint32, umask uint32,
	rdev uint32, constructor InodeConstructor, type_ quantumfs.ObjectType,
	key quantumfs.ObjectKey, out *fuse.EntryOut) Inode {

	defer c.funcIn("Directory::create_").Out()

	uid := c.fuseCtx.Owner.Uid
	gid := c.fuseCtx.Owner.Gid
	UID := quantumfs.ObjectUid(uid, uid)
	GID := quantumfs.ObjectGid(gid, gid)
	entry := createNewEntry(c, name, mode, umask, rdev,
		0, UID, GID, type_, key)
	inodeNum := c.qfs.newInodeId()
	newEntity := constructor(c, name, key, 0, inodeNum.id, dir.self,
		mode, rdev, entry)

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.setRecord(c, inodeNum, entry)
	}()

	c.qfs.setInode(c, inodeNum.id, newEntity)
	func() {
		defer c.qfs.mapMutex.Lock().Unlock()
		addInodeRef_(c, inodeNum.id)
	}()
	c.qfs.incrementLookupCount(c, inodeNum.id)

	// We want to ensure that we panic if we attempt to increment the refcount of
	// an Inode with a zero refcount as that indicates a counting issue. To do so
	// we must initialize with a non-zero refcount so incrementLookupCount()
	// above will succeed. Give back the temporary reference count here.
	newEntity.delRef(c)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum.id)
	out.Generation = inodeNum.generation
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum.id, c.fuseCtx.Owner,
		entry)

	newEntity.dirty(c)

	return newEntity
}

func (dir *Directory) childExists(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::childExists", "name %s", name).Out()

	defer dir.childRecordLock.Lock().Unlock()

	record := dir.children.recordByName(c, name)
	if record != nil {
		c.vlog("Child exists")
		return fuse.Status(syscall.EEXIST)
	}
	return fuse.OK
}

func (dir *Directory) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	defer c.funcIn("Directory::Create").Out()

	var file Inode
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		recordErr := dir.childExists(c, name)
		if recordErr != fuse.OK {
			return recordErr
		}

		err := hasDirectoryWritePerm(c, dir)
		if err != fuse.OK {
			return err
		}

		c.vlog("Creating file: '%s'", name)

		file = dir.create_(c, name, input.Mode, input.Umask, 0, newSmallFile,
			quantumfs.ObjectTypeSmallFile, quantumfs.EmptyBlockKey,
			&out.EntryOut)
		return fuse.OK
	}()

	if result != fuse.OK {
		return result
	}

	// mark the child as being accessed
	file.markSelfAccessed(c, quantumfs.PathCreated)

	dir.updateSize(c, result)

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(file.(*File), file.inodeNum(),
		fileHandleNum, file.treeState())
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	c.vlog("New file inode %d, Fh %d", file.inodeNum(), fileHandleNum)

	out.OpenOut.OpenFlags = fuse.FOPEN_KEEP_CACHE
	out.OpenOut.Fh = uint64(fileHandleNum)

	return fuse.OK
}

func (dir *Directory) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	defer c.funcIn("Directory::SetAttr").Out()

	return dir.parentSetChildAttr(c, dir.InodeCommon.id, nil, attr, out, false)
}

func (dir *Directory) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Mkdir").Out()

	var newDir Inode
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		recordErr := dir.childExists(c, name)
		if recordErr != fuse.OK {
			return recordErr
		}

		err := hasDirectoryWritePerm(c, dir)
		if err != fuse.OK {
			return err
		}

		newDir = dir.create_(c, name, input.Mode, input.Umask, 0,
			newDirectory, quantumfs.ObjectTypeDirectory,
			quantumfs.EmptyDirKey, out)
		if newDir != nil {
			newDir.finishInit(c)
		}
		return fuse.OK
	}()

	if result == fuse.OK {
		newDir.markSelfAccessed(c, quantumfs.PathCreated|
			quantumfs.PathIsDir)
	}

	dir.updateSize(c, result)
	c.vlog("Directory::Mkdir created inode %d", out.NodeId)

	return result
}

func (dir *Directory) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	defer c.funcIn("Directory::getChildAttr").Out()

	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	utils.Assert(record != nil, "Failed to get record for inode %d of %d",
		inodeNum, dir.inodeNum())

	fillAttrWithDirectoryRecord(c, out, inodeNum, owner, record)
}

// must have childRecordLock, fetches the child for calls that come UP from a child.
// Should not be used by functions which aren't routed from a child, as even if dir
// is wsr it should not accommodate getting hardlink records in those situations
func (dir *Directory) getRecordChildCall_(c *ctx,
	inodeNum InodeId) quantumfs.ImmutableDirectoryRecord {

	defer c.FuncIn("DirectoryRecord::getRecordChildCall_", "inode %d",
		inodeNum).Out()

	record := dir.children.recordByInodeId(c, inodeNum)
	if record != nil {
		c.vlog("Record found")
		return record
	}

	// if we don't have the child, maybe we're wsr and it's a hardlink
	if dir.self.isWorkspaceRoot() {
		c.vlog("Checking hardlink table")
		linkRecord := dir.hardlinkTable.recordByInodeId(c, inodeNum)
		if linkRecord != nil {
			c.vlog("Hardlink found")
			return linkRecord
		}
	}

	return nil
}

func (dir *Directory) foreachDirectInode(c *ctx, visitFn inodeVisitFn) {
	defer dir.childRecordLock.Lock().Unlock()

	dir.children.foreachDirectInode(c, visitFn)
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Unlink", "%s", name).Out()

	childId := dir.childInodeNum(name).id
	child, release := c.qfs.inode(c, childId)
	defer release()

	if child == nil {
		return fuse.ENOENT
	}

	result := child.deleteSelf(c, func() (quantumfs.DirectoryRecord,
		fuse.Status) {

		defer dir.Lock().Unlock()

		record, err := func() (quantumfs.ImmutableDirectoryRecord,
			fuse.Status) {

			defer dir.childRecordLock.Lock().Unlock()

			record := dir.children.recordByName(c, name)
			if record == nil {
				return nil, fuse.ENOENT
			}

			return record, fuse.OK
		}()
		if err != fuse.OK {
			return nil, err
		}

		type_ := objectTypeToFileType(c, underlyingType(record))

		if type_ == fuse.S_IFDIR {
			c.vlog("Directory::Unlink directory")
			return nil, fuse.Status(syscall.EISDIR)
		}

		err = hasDirectoryWritePermSticky(c, dir, record.Owner())
		if err != fuse.OK {
			return nil, err
		}

		return dir.delChild_(c, name), fuse.OK
	})
	if result == fuse.OK {
		dir.self.markAccessed(c, name, quantumfs.PathDeleted)
	}

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Rmdir(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Rmdir", "%s", name).Out()

	childId := dir.childInodeNum(name).id
	child, release := c.qfs.inode(c, childId)
	defer release()

	if child == nil {
		return fuse.ENOENT
	}

	result := child.deleteSelf(c, func() (quantumfs.DirectoryRecord,
		fuse.Status) {

		defer dir.Lock().Unlock()

		result := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()
			record := dir.children.recordByName(c, name)
			if record == nil {
				return fuse.ENOENT
			}

			err := hasDirectoryWritePermSticky(c, dir, record.Owner())
			if err != fuse.OK {
				return err
			}

			type_ := objectTypeToFileType(c, underlyingType(record))
			if type_ != fuse.S_IFDIR {
				return fuse.ENOTDIR
			}

			if record.Size() != 0 {
				c.vlog("directory has %d children", record.Size())
				return fuse.Status(syscall.ENOTEMPTY)
			}

			return fuse.OK
		}()
		if result != fuse.OK {
			return nil, result
		}

		return dir.delChild_(c, name), fuse.OK
	})
	if result == fuse.OK {
		dir.self.markAccessed(c, name, quantumfs.PathDeleted |
			quantumfs.PathIsDir)
	}

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Symlink").Out()

	var inode Inode
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		recordErr := dir.childExists(c, name)
		if recordErr != fuse.OK {
			return recordErr
		}

		result := hasDirectoryWritePerm(c, dir)
		if result != fuse.OK {
			return result
		}

		inode = dir.create_(c, name, 0777, 0777, 0, newSymlink,
			quantumfs.ObjectTypeSymlink, quantumfs.EmptyBlockKey, out)

		link := inode.(*Symlink)

		// Set the symlink link data
		link.setLink(c, pointedTo)
		func() {
			defer dir.childRecordLock.Lock().Unlock()
			dir.children.modifyChildWithFunc(c, inode.inodeNum(),
				func(record quantumfs.DirectoryRecord) {

					record.SetSize(uint64(len(pointedTo)))
				})
		}()

		// Update the outgoing entry size
		out.Attr.Size = uint64(len(pointedTo))
		out.Attr.Blocks = utils.BlocksRoundUp(out.Attr.Size, statBlockSize)

		return fuse.OK
	}()

	if result == fuse.OK {
		inode.markSelfAccessed(c, quantumfs.PathCreated)
	}

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.wlog("Invalid Readlink on Directory")
	return nil, fuse.EINVAL
}

var zeroSpecial quantumfs.ObjectKey

func init() {
	zeroSpecial = quantumfs.NewObjectKey(quantumfs.KeyTypeEmbedded,
		[quantumfs.ObjectKeyLength - 1]byte{})
}

func (dir *Directory) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Mknod").Out()

	var inode Inode
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		recordErr := dir.childExists(c, name)
		if recordErr != fuse.OK {
			return recordErr
		}

		err := hasDirectoryWritePerm(c, dir)
		if err != fuse.OK {
			return err
		}

		c.vlog("Directory::Mknod Mode %x", input.Mode)
		if utils.BitFlagsSet(uint(input.Mode), syscall.S_IFIFO) ||
			utils.BitFlagsSet(uint(input.Mode), syscall.S_IFSOCK) ||
			utils.BitFlagsSet(uint(input.Mode), syscall.S_IFBLK) ||
			utils.BitFlagsSet(uint(input.Mode), syscall.S_IFCHR) {

			inode = dir.create_(c, name, input.Mode, input.Umask,
				input.Rdev, newSpecial, quantumfs.ObjectTypeSpecial,
				zeroSpecial, out)
		} else if utils.BitFlagsSet(uint(input.Mode), syscall.S_IFREG) {
			inode = dir.create_(c, name, input.Mode, input.Umask, 0,
				newSmallFile, quantumfs.ObjectTypeSmallFile,
				quantumfs.EmptyBlockKey, out)
		} else {
			c.dlog("Directory::Mknod invalid type %x", input.Mode)
			return fuse.EINVAL
		}
		return fuse.OK
	}()

	if result == fuse.OK {
		inode.markSelfAccessed(c, quantumfs.PathCreated)
	}

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	defer c.FuncIn("Directory::RenameChild", "%s -> %s", oldName, newName).Out()

	fileType, overwritten, result := dir.RenameChild_(c, oldName, newName)
	if result == fuse.OK {
		if overwritten != nil {
			dir.self.markAccessed(c, overwritten.Filename(),
				markType(overwritten.Type(),
					quantumfs.PathDeleted))
		}

		// Conceptually we remove any entry in the way before we move
		// the source file in its place, so update the accessed list
		// mark in that order to ensure mark logic produces the
		// correct result.
		dir.self.markAccessed(c, oldName,
			markType(fileType, quantumfs.PathDeleted))
		dir.self.markAccessed(c, newName,
			markType(fileType, quantumfs.PathCreated))
	}

	return result
}

func (dir *Directory) RenameChild_(c *ctx, oldName string,
	newName string) (fileType quantumfs.ObjectType,
	overwrittenRecord quantumfs.ImmutableDirectoryRecord, result fuse.Status) {

	defer dir.updateSize(c, result)
	overwrittenInodeId := dir.childInodeNum(newName).id
	overwrittenInode := c.qfs.inodeNoInstantiate(c, overwrittenInodeId)
	if overwrittenInode != nil {
		defer overwrittenInode.getParentLock().Lock().Unlock()
	}
	defer dir.Lock().Unlock()

	oldInodeId, record, result := func() (InodeId,
		quantumfs.ImmutableDirectoryRecord, fuse.Status) {

		defer dir.childRecordLock.Lock().Unlock()

		dstRecord := dir.children.recordByName(c, newName)
		if dstRecord != nil &&
			dstRecord.Type() == quantumfs.ObjectTypeDirectory &&
			dstRecord.Size() != 0 {

			// We can not overwrite a non-empty directory
			return quantumfs.InodeIdInvalid, nil,
				fuse.Status(syscall.ENOTEMPTY)
		}

		record := dir.children.recordByName(c, oldName)
		if record == nil {
			return quantumfs.InodeIdInvalid, nil, fuse.ENOENT
		}

		err := hasDirectoryWritePermSticky(c, dir, record.Owner())
		if err != fuse.OK {
			return quantumfs.InodeIdInvalid, nil, err
		}

		if oldName == newName {
			return quantumfs.InodeIdInvalid, nil, fuse.OK
		}
		oldInodeId_ := dir.children.inodeNum(oldName).id
		overwrittenRecord = dir.orphanChild_(c, newName, overwrittenInode)
		dir.children.renameChild(c, oldName, newName)

		now := quantumfs.NewTime(time.Now())
		if hardlink, isHardlink := record.(*HardlinkLeg); !isHardlink {
			dir.children.modifyChildWithFunc(c, oldInodeId_,
				func(record quantumfs.DirectoryRecord) {

					record.SetContentTime(now)
				})
		} else {
			hardlink.setCreationTime(now)
			dir.hardlinkTable.modifyChildWithFunc(c, oldInodeId_,
				func(record quantumfs.DirectoryRecord) {

					record.SetContentTime(now)
				})
		}

		fileType = record.Type()

		return oldInodeId_, record, fuse.OK
	}()
	if oldName == newName || result != fuse.OK {
		return
	}

	// update the inode name
	if child := c.qfs.inodeNoInstantiate(c, oldInodeId); child != nil {
		child.setName(newName)
		child.clearAccessedCache()

		// The child has an effective record. Make sure it is not lost.
		child.dirty(c)
	} else {
		if _, isHardlink := record.(*HardlinkLeg); isHardlink {
			dir.hardlinkTable.makePublishable(c, record.FileId())
		} else {
			func() {
				defer dir.childRecordLock.Lock().Unlock()
				dir.children.makePublishable(c, newName)
			}()
		}
	}

	result = fuse.OK
	return
}

// Must hold dir and dir.childRecordLock
// Must hold the child's parentLock if inode is not nil
func (dir *Directory) orphanChild_(c *ctx, name string,
	inode Inode) (rtn quantumfs.ImmutableDirectoryRecord) {

	defer c.FuncIn("Directory::orphanChild_", "%s", name).Out()

	removedId := dir.children.inodeNum(name).id
	removedRecord := dir.children.deleteChild(c, name)
	if removedRecord == nil {
		return
	}

	rtn = removedRecord.AsImmutable()

	removedRecord = dir.prepareForOrphaning_(c, name, removedRecord)
	if removedId == quantumfs.InodeIdInvalid || removedRecord == nil {
		return
	}
	if removedRecord.Type() == quantumfs.ObjectTypeHardlink {
		c.vlog("nothing to do for the detached leg of hardlink")
		return
	}
	if inode == nil {
		c.qfs.removeUninstantiated(c, []InodeId{removedId})
	} else {
		inode.orphan_(c, removedRecord)
	}

	return
}

func (dir *Directory) childInodeNum(name string) InodeIdInfo {
	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()
	return dir.children.inodeNum(name)
}

func (dir *Directory) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) (result fuse.Status) {

	defer c.FuncIn("Directory::MvChild", "%s -> %s", oldName, newName).Out()

	fileType, overwritten, result := dir.MvChild_(c, dstInode, oldName, newName)
	if result == fuse.OK {
		dst := AsDirectory(dstInode)
		if overwritten != nil {
			dst.self.markAccessed(c, overwritten.Filename(),
				markType(overwritten.Type(),
					quantumfs.PathDeleted))
		}

		// This is the same entry just moved, so we can use the same
		// record for both the old and new paths.
		dir.self.markAccessed(c, oldName,
			markType(fileType, quantumfs.PathDeleted))
		dst.self.markAccessed(c, newName,
			markType(fileType, quantumfs.PathCreated))
	}

	return
}

func (dir *Directory) MvChild_(c *ctx, dstInode Inode, oldName string,
	newName string) (fileType quantumfs.ObjectType,
	overwritten quantumfs.ImmutableDirectoryRecord, result fuse.Status) {

	// check write permission for both directories
	result = hasDirectoryWritePerm(c, dstInode)
	if result != fuse.OK {
		return
	}

	result = func() fuse.Status {
		defer dir.childRecordLock.Lock().Unlock()

		record := dir.children.recordByName(c, oldName)
		return hasDirectoryWritePermSticky(c, dir, record.Owner())
	}()
	if result != fuse.OK {
		return
	}

	dst := asDirectory(dstInode)

	defer func() {
		dir.updateSize(c, result)
		dst.updateSize(c, result)
	}()
	childInodeId := dir.childInodeNum(oldName)
	childInode := c.qfs.inodeNoInstantiate(c, childInodeId.id)

	overwrittenInodeId := dst.childInodeNum(newName).id
	overwrittenInode := c.qfs.inodeNoInstantiate(c, overwrittenInodeId)

	c.vlog("Aquiring locks")
	if childInode != nil && overwrittenInode != nil {
		firstChild, lastChild := getLockOrder(childInode, overwrittenInode)
		defer firstChild.getParentLock().Lock().Unlock()
		defer lastChild.getParentLock().Lock().Unlock()
	} else if childInode != nil {
		defer childInode.getParentLock().Lock().Unlock()
	} else if overwrittenInode != nil {
		defer overwrittenInode.getParentLock().Lock().Unlock()
	}

	// The locking here is subtle.
	//
	// Firstly we must protect against the case where a concurrent rename
	// in the opposite direction (from dst into dir) is occurring as we
	// are renaming a file from dir into dst. If we lock naively we'll
	// end up with a lock ordering inversion and deadlock in this case.
	//
	// We prevent this by locking dir and dst in a consistent ordering
	// based upon their inode number. All multi-inode locking must call
	// getLockOrder() to facilitate this.
	firstLock, lastLock := getLockOrder(dst, dir)
	defer firstLock.Lock().Unlock()
	defer lastLock.Lock().Unlock()

	result = func() fuse.Status {
		c.vlog("Checking if destination is an empty directory")
		defer dst.childRecordLock.Lock().Unlock()

		dstRecord := dst.children.recordByName(c, newName)
		if dstRecord != nil &&
			dstRecord.Type() == quantumfs.ObjectTypeDirectory &&
			dstRecord.Size() != 0 {

			// We can not overwrite a non-empty directory
			return fuse.Status(syscall.ENOTEMPTY)
		}
		return fuse.OK
	}()
	if result != fuse.OK {
		return
	}

	// Remove from source. This is atomic because both the source and destination
	// directories are locked.
	c.vlog("Removing source")
	newEntry := func() quantumfs.DirectoryRecord {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.deleteChild(c, oldName)
	}()
	if newEntry == nil {
		c.vlog("No source!")
		return
	}

	// fix the name on the copy
	newEntry.SetFilename(newName)

	hardlink, isHardlink := newEntry.(*HardlinkLeg)
	if !isHardlink {
		newEntry.SetContentTime(quantumfs.NewTime(time.Now()))
		// Update the inode to point to the new name and
		// mark as accessed in both parents.
		if childInode != nil {
			c.vlog("Updating name and parent")
			utils.Assert(dst.inodeNum() != childInode.inodeNum(),
				"Cannot orphan child by renaming %s %d",
				newName, dst.inodeNum())
			childInode.setParent_(c, dst)
			childInode.setName(newName)
			childInode.clearAccessedCache()
		}
	} else {
		c.vlog("Updating hardlink creation time")
		hardlink.setCreationTime(quantumfs.NewTime(time.Now()))
		dst.hardlinkTable.modifyChildWithFunc(c, childInodeId.id,
			func(record quantumfs.DirectoryRecord) {

				record.SetContentTime(hardlink.creationTime())
			})
	}

	// Add to destination, possibly removing the overwritten inode
	c.vlog("Adding to destination directory")
	func() {
		defer dst.childRecordLock.Lock().Unlock()
		overwritten = dst.orphanChild_(c, newName, overwrittenInode)
		dst.children.setRecord(c, childInodeId, newEntry)

		// Being inserted means you need to be synced to be publishable. If
		// the inode is instantiated mark it dirty, otherwise mark it
		// publishable immediately.
		if childInode != nil {
			childInode.dirty(c)
		} else {
			if isHardlink {
				dst.hardlinkTable.makePublishable(c,
					newEntry.FileId())
			} else {
				dst.children.makePublishable(c, newName)
			}
		}
		dst.self.dirty(c)
	}()

	fileType = newEntry.Type()

	// Set entry in new directory. If the renamed inode is
	// uninstantiated, we swizzle the parent here. If it's a hardlink, it's
	// already matched to the workspaceroot so don't corrupt that
	if childInode == nil && !isHardlink {
		c.qfs.addUninstantiated(c, []inodePair{
			newInodePair(childInodeId.id, dir.inodeNum())})
	}

	result = fuse.OK
	return
}

func (dir *Directory) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	defer c.FuncIn("Directory::GetXAttrSize", "attr %s", attr).Out()

	return dir.parentGetChildXAttrSize(c, dir.inodeNum(), attr)
}

func (dir *Directory) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	defer c.FuncIn("Directory::GetXAttrData", "attr %s", attr).Out()

	return dir.parentGetChildXAttrData(c, dir.inodeNum(), attr)
}

func (dir *Directory) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	defer c.funcIn("Directory::ListXAttr").Out()
	return dir.parentListChildXAttr(c, dir.inodeNum())
}

func (dir *Directory) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	defer c.FuncIn("Directory::SetXAttr", "attr %s", attr).Out()
	return dir.parentSetChildXAttr(c, dir.inodeNum(), attr, data)
}

func (dir *Directory) RemoveXAttr(c *ctx, attr string) fuse.Status {
	defer c.FuncIn("Directory::RemoveXAttr", "attr %s", attr).Out()
	return dir.parentRemoveChildXAttr(c, dir.inodeNum(), attr)
}

func (dir *Directory) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, hardlinkDelta *HardlinkDelta) {

	defer c.FuncIn("Directory::syncChild", "dir inode %d child inode %d %s",
		dir.inodeNum(), inodeNum, newKey.String()).Out()

	defer dir.Lock().Unlock()
	dir.self.dirty(c)
	defer dir.childRecordLock.Lock().Unlock()

	entry := dir.getRecordChildCall_(c, inodeNum)
	if entry == nil {
		c.elog("Directory::syncChild inode %d not a valid child",
			inodeNum)
		return
	}

	dir.children.setID(c, entry.Filename(), newKey)
	dir.hardlinkDelta.populateFrom(hardlinkDelta)
}

func getRecordExtendedAttributes(c *ctx,
	attrKey quantumfs.ObjectKey) (*quantumfs.ExtendedAttributes,
	fuse.Status) {

	if attrKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		c.vlog("getRecordExtendedAttributes returning new object")
		return nil, fuse.ENOENT
	}

	buffer := c.dataStore.Get(&c.Ctx, attrKey)
	if buffer == nil {
		c.dlog("Failed to retrieve attribute list")
		return nil, fuse.EIO
	}

	attributeList := MutableCopy(c, buffer).AsExtendedAttributes()
	return &attributeList, fuse.OK
}

// Get the extended attributes object. The status is EIO on error or ENOENT if there
// are no extended attributes for that child.
func (dir *Directory) getExtendedAttributes_(c *ctx,
	inodeNum InodeId) (*quantumfs.ExtendedAttributes, fuse.Status) {

	defer c.funcIn("Directory::getExtendedAttributes_").Out()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	if record == nil {
		c.vlog("Child not found")
		return nil, fuse.EIO
	}

	return getRecordExtendedAttributes(c, record.ExtendedAttributes())
}

func (dir *Directory) getChildXAttrBuffer(c *ctx, inodeNum InodeId,
	attr string) (ImmutableBuffer, fuse.Status) {

	defer c.FuncIn("Directory::getChildXAttrBuffer", "%d %s", inodeNum,
		attr).Out()

	defer dir.RLock().RUnlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)
	if ok == fuse.ENOENT {
		return nil, fuse.ENODATA
	}

	if ok == fuse.EIO {
		return nil, fuse.EIO
	}

	key, found := attributeList.AttributeByKey(attr)
	if !found {
		c.vlog("XAttr name not found")
		return nil, fuse.ENODATA
	}

	c.vlog("Found attribute key: %s", key.String())
	buffer := c.dataStore.Get(&c.Ctx, key)

	if buffer == nil {
		c.elog("Failed to retrieve attribute datablock")
		return nil, fuse.EIO
	}

	return buffer, fuse.OK
}

func (dir *Directory) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("Directory::getChildXAttrSize").Out()

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return 0, status
	}

	return buffer.Size(), fuse.OK
}

func (dir *Directory) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("Directory::getChildXAttrData").Out()

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return []byte{}, status
	}

	return slowCopy(buffer), fuse.OK
}

func (dir *Directory) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.FuncIn("Directory::listChildXAttr", "%d", inodeNum).Out()

	defer dir.RLock().RUnlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)

	if ok == fuse.EIO {
		return nil, fuse.EIO
	}

	var nameBuffer bytes.Buffer
	if ok != fuse.ENOENT {
		for i := 0; i < attributeList.NumAttributes(); i++ {
			name, _ := attributeList.Attribute(i)
			c.vlog("Appending %s", name)
			nameBuffer.WriteString(name)
			nameBuffer.WriteByte(0)
		}
	}

	// don't append our self-defined extended attribute XAttrTypeKey to hide it

	c.vlog("Returning %d bytes", nameBuffer.Len())

	return nameBuffer.Bytes(), fuse.OK
}

func (dir *Directory) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer c.FuncIn("Directory::setChildXAttr", "%d, %s len %d", inodeNum, attr,
		len(data)).Out()

	defer dir.Lock().Unlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)
	if ok == fuse.EIO {
		return fuse.EIO
	}

	if ok == fuse.ENOENT {
		// getExtendedAttributes_() returns a shared
		// quantumfs.ExtendedAttributes instance when the file has no
		// extended attributes for performance reasons. Thus we need to
		// instantiate our own before we modify it.
		attributeList = quantumfs.NewExtendedAttributes()
	}

	var dataKey quantumfs.ObjectKey
	if len(data) == 0 {
		dataKey = quantumfs.EmptyBlockKey
	} else {
		var err error
		dataBuf := newBufferCopy(c, data, quantumfs.KeyTypeData)
		dataKey, err = dataBuf.Key(&c.Ctx)
		if err != nil {
			c.elog("Error uploading XAttr data: %v", err)
			return fuse.EIO
		}
	}

	set := false
	for i := 0; i < attributeList.NumAttributes(); i++ {
		name, _ := attributeList.Attribute(i)
		if name == attr {
			c.vlog("Overwriting existing attribute %d", i)
			attributeList.SetAttribute(i, name, dataKey)
			set = true
			break
		}
	}

	// Append attribute
	if !set {
		if attributeList.NumAttributes() >=
			quantumfs.MaxNumExtendedAttributes() {

			c.vlog("XAttr list full %d", attributeList.NumAttributes())
			return fuse.Status(syscall.ENOSPC)
		}

		c.vlog("Appending new attribute")
		attributeList.SetAttribute(attributeList.NumAttributes(), attr,
			dataKey)
		attributeList.SetNumAttributes(attributeList.NumAttributes() + 1)
	}

	buffer := newBuffer(c, attributeList.Bytes(), quantumfs.KeyTypeMetadata)
	key, err := buffer.Key(&c.Ctx)
	if err != nil {
		c.elog("Error computing extended attribute key: %v", err)
		return fuse.EIO
	}

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		record := dir.children.recordByInodeId(c, inodeNum)

		now := quantumfs.NewTime(time.Now())
		modify := func(record quantumfs.DirectoryRecord) {
			record.SetExtendedAttributes(key)
			record.SetContentTime(now)
		}

		if record != nil && record.Type() != quantumfs.ObjectTypeHardlink {
			dir.children.modifyChildWithFunc(c, inodeNum, modify)
		} else {
			// If we don't have the child, maybe we're the WSR and it's a
			// hardlink. If we aren't the WSR, then it must be a
			// hardlink.
			c.vlog("Checking hardlink table")
			dir.hardlinkTable.modifyChildWithFunc(c, inodeNum, modify)
		}
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.FuncIn("Directory::removeChildXAttr", "%d, %s", inodeNum, attr).Out()

	defer dir.Lock().Unlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)
	if ok == fuse.EIO {
		return fuse.EIO
	}

	if ok == fuse.ENOENT {
		return fuse.ENODATA
	}

	var i int
	for i = 0; i < attributeList.NumAttributes(); i++ {
		name, _ := attributeList.Attribute(i)
		if name == attr {
			c.vlog("Found attribute %d", i)
			break
		}
	}

	if i == attributeList.NumAttributes() {
		// We didn't find the attribute
		return fuse.ENODATA
	}

	var key quantumfs.ObjectKey
	if attributeList.NumAttributes() != 1 {
		// Move the last attribute over the one to be removed
		lastIndex := attributeList.NumAttributes() - 1
		lastName, lastId := attributeList.Attribute(lastIndex)
		attributeList.SetAttribute(i, lastName, lastId)
		attributeList.SetNumAttributes(lastIndex)

		buffer := newBuffer(c, attributeList.Bytes(),
			quantumfs.KeyTypeMetadata)
		var err error
		key, err = buffer.Key(&c.Ctx)
		if err != nil {
			c.elog("Error computing extended attribute key: %v", err)
			return fuse.EIO
		}
	} else {
		// We are deleting the only extended attribute. Change the
		// DirectoryRecord key only
		key = quantumfs.EmptyBlockKey
	}

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		record := dir.children.recordByInodeId(c, inodeNum)

		now := quantumfs.NewTime(time.Now())
		modify := func(record quantumfs.DirectoryRecord) {
			record.SetExtendedAttributes(key)
			record.SetContentTime(now)
		}

		if record != nil {
			dir.children.modifyChildWithFunc(c, inodeNum, modify)
		} else if dir.self.isWorkspaceRoot() {
			// if we don't have the child, maybe we're wsr and it's a
			// hardlink
			c.vlog("Checking hardlink table")
			dir.hardlinkTable.modifyChildWithFunc(c, inodeNum, modify)
		}
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) instantiateChild(c *ctx, inodeNum InodeId) Inode {
	defer c.FuncIn("Directory::instantiateChild", "Inode %d of %d", inodeNum,
		dir.inodeNum()).Out()
	defer dir.childRecordLock.Lock().Unlock()

	if inode := c.qfs.inodeNoInstantiate(c, inodeNum); inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeNum)
		return inode
	}

	entry := dir.children.recordByInodeId(c, inodeNum)
	if entry == nil {
		c.vlog("Cannot instantiate child with no record: %d", inodeNum)
		return nil
	}

	// check if the child is a hardlink
	isHardlink, _ := dir.hardlinkTable.checkHardlink(inodeNum)
	if isHardlink {
		return dir.hardlinkTable.instantiateHardlink(c, inodeNum)
	}

	// add a check incase there's an inconsistency
	if hardlink, isHardlink := entry.(*HardlinkLeg); isHardlink {
		panic(fmt.Sprintf("Hardlink not recognized by workspaceroot: %d, %d",
			inodeNum, hardlink.FileId()))
	}

	return dir.recordToChild(c, inodeNum, entry)
}

func (dir *Directory) recordToChild(c *ctx, inodeNum InodeId,
	entry quantumfs.ImmutableDirectoryRecord) Inode {

	defer c.FuncIn("DirectoryRecord::recordToChild", "name %s inode %d",
		entry.Filename(), inodeNum).Out()

	var constructor InodeConstructor
	switch entry.Type() {
	default:
		c.elog("Unknown InodeConstructor type: %d", entry.Type())
		panic("Unknown InodeConstructor type")
	case quantumfs.ObjectTypeDirectory:
		constructor = newDirectory
	case quantumfs.ObjectTypeSmallFile:
		constructor = newSmallFile
	case quantumfs.ObjectTypeMediumFile:
		constructor = newMediumFile
	case quantumfs.ObjectTypeLargeFile:
		constructor = newLargeFile
	case quantumfs.ObjectTypeVeryLargeFile:
		constructor = newVeryLargeFile
	case quantumfs.ObjectTypeSymlink:
		constructor = newSymlink
	case quantumfs.ObjectTypeSpecial:
		constructor = newSpecial
	}

	c.vlog("Instantiating child %d with key %s", inodeNum, entry.ID().String())

	return constructor(c, entry.Filename(), entry.ID(), entry.Size(), inodeNum,
		dir.self, 0, 0, nil)
}

// Do a similar work like Lookup(), but it does not interact with fuse, and returns
// the child node to the caller.
func (dir *Directory) lookupInternal(c *ctx, name string,
	entryType quantumfs.ObjectType) (child Inode, err error) {

	defer c.FuncIn("Directory::lookupInternal", "name %s", name).Out()

	defer dir.RLock().RUnlock()
	inodeNum, record, err := dir.lookupChildRecord_(c, name)
	if err != nil {
		return nil, err
	}
	if record.Type() != entryType {
		return nil, errors.New("Not Required Type")
	}
	c.vlog("Directory::lookupInternal found inode %d Name %s", inodeNum, name)
	c.qfs.incrementLookupCount(c, inodeNum)
	child, release := c.qfs.inode(c, inodeNum)
	defer release()

	if child == nil {
		c.qfs.shouldForget(c, inodeNum, 1)
	}
	return child, nil
}

// Require an Inode locked for read
func (dir *Directory) lookupChildRecord_(c *ctx, name string) (InodeId,
	quantumfs.ImmutableDirectoryRecord, error) {

	defer c.FuncIn("Directory::lookupChildRecord_", "name %s", name).Out()

	defer dir.childRecordLock.Lock().Unlock()
	record := dir.children.recordByName(c, name)
	if record == nil {
		return quantumfs.InodeIdInvalid, nil,
			errors.New("Non-existing Inode")
	}

	inodeNum := dir.children.inodeNum(name).id
	return inodeNum, record, nil
}

func createNewEntry(c *ctx, name string, mode uint32,
	umask uint32, rdev uint32, size uint64, uid quantumfs.UID,
	gid quantumfs.GID, type_ quantumfs.ObjectType,
	key quantumfs.ObjectKey) quantumfs.DirectoryRecord {

	defer c.FuncIn("createNewEntry", "name %s", name).Out()

	// set up the Inode record
	now := time.Now()
	var entry quantumfs.ThinRecord
	entry.SetFilename(name)
	entry.SetID(key)
	entry.SetType(type_)
	entry.SetPermissions(modeToPermissions(mode, umask))
	c.vlog("Directory::createNewEntry mode %o umask %o permissions %o",
		mode, umask, entry.Permissions())
	entry.SetOwner(uid)
	entry.SetGroup(gid)
	entry.SetSize(size)
	entry.SetExtendedAttributes(quantumfs.EmptyBlockKey)
	entry.SetContentTime(quantumfs.NewTime(now))
	entry.SetModificationTime(quantumfs.NewTime(now))
	entry.SetFileId(quantumfs.GenerateUniqueFileId())
	entry.SetNlinks(1)

	return &entry
}

// Needs exclusive Inode lock
func (dir *Directory) duplicateInode_(c *ctx, name string, mode uint32, umask uint32,
	rdev uint32, size uint64, uid quantumfs.UID, gid quantumfs.GID,
	type_ quantumfs.ObjectType, key quantumfs.ObjectKey) {

	defer c.FuncIn("Directory::duplicateInode_", "name %s", name).Out()

	entry := createNewEntry(c, name, mode, umask, rdev, size,
		uid, gid, type_, key)

	inodeNum := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.loadChild(c, entry)
	}()

	parent := dir.inodeNum()
	if type_ == quantumfs.ObjectTypeHardlink {
		parent = dir.hardlinkTable.getWorkspaceRoot().inodeNum()
	}
	c.qfs.addUninstantiated(c, []inodePair{newInodePair(inodeNum, parent)})

	c.qfs.noteChildCreated(c, dir.inodeNum(), name)
}

func (dir *Directory) markHardlinkPath(c *ctx, path string,
	fileId quantumfs.FileId) {

	defer c.funcIn("Directory::markHardlinkPath").Out()

	if dir.InodeCommon.isWorkspaceRoot() {
		dir.hardlinkTable.markHardlinkPath(c, path, fileId)
		return
	}

	path = dir.name() + "/" + path

	defer dir.InodeCommon.parentLock.RLock().RUnlock()
	parent, release := dir.InodeCommon.parent_(c)
	defer release()

	parentDir := asDirectory(parent)
	parentDir.markHardlinkPath(c, path, fileId)
}

func (dir *Directory) normalizeChildren(c *ctx) {
	defer c.funcIn("Directory::normalizeChildren").Out()

	// The normalization happens in three stages for a
	// hardlink entry which is ready:
	//
	// 1. HardlinkTable::invalidateNormalizedRecordLock()
	//    The inode is removed from the hardlink table and the
	//    table is locked to prevent the inode from getting instantiated.
	//
	// 2. Directory::normalizeChild()
	//    The record is copied from the hardlink table to the
	//    parent directory and the inode is adjusted to the new
	//    location.
	//
	// 3. HardlinkTable::apply()
	//    Once the parent directory's hardlinkdelta bubbles up
	//    to the root, the rest of the hardlink entry is removed
	//    from the hardlinktable.

	normalRecords := dir.getNormalizationCandidates(c)
	for inodeId, records := range normalRecords {
		dir.normalizeChild(c, inodeId, records)
	}
}

func (dir *Directory) flush(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("Directory::flush", "%d %s", dir.inodeNum(),
		dir.name_).Out()

	dir.normalizeChildren(c)
	dir.parentSyncChild(c, func() (quantumfs.ObjectKey, *HardlinkDelta) {
		defer dir.childRecordLock.Lock().Unlock()
		dir.publish_(c)
		return dir.baseLayerId, dir.hardlinkDelta
	})

	return dir.baseLayerId
}

// Directory inode lock must be held
func (dir *Directory) hardlinkInc_(fileId quantumfs.FileId) {
	dir.hardlinkDelta.inc(fileId)
	dir.hardlinkTable.hardlinkInc(fileId)
}

// Directory inode lock must be held
func (dir *Directory) hardlinkDec_(
	fileId quantumfs.FileId) (effective quantumfs.DirectoryRecord) {

	dir.hardlinkDelta.dec(fileId)
	return dir.hardlinkTable.hardlinkDec(fileId)
}

type directoryContents struct {
	// All immutable after creation
	filename   string
	fuseType   uint32 // One of fuse.S_IFDIR, S_IFREG, etc
	attr       fuse.Attr
	generation uint64
}

type directorySnapshotSource interface {
	getChildSnapshot(c *ctx) []directoryContents
	inodeNum() InodeId
	treeState() *TreeState
	generation() uint64
}

func newDirectorySnapshot(c *ctx, src directorySnapshotSource) *directorySnapshot {

	defer c.funcIn("newDirectorySnapshot").Out()

	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:         c.qfs.newFileHandleId(),
			inodeNum:   src.inodeNum(),
			treeState_: src.treeState(),
		},
		_generation: src.generation(),
		src:         src,
	}

	utils.Assert(ds.treeState() != nil,
		"directorySnapshot treeState nil at init")

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children    []directoryContents
	_generation uint64
	src         directorySnapshotSource
}

func (ds *directorySnapshot) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	defer c.funcIn("directorySnapshot::ReadDirPlus").Out()
	offset := input.Offset

	if offset == 0 {
		c.vlog("Refreshing child list")
		ds.children = ds.src.getChildSnapshot(c)
	}

	if offset > uint64(len(ds.children)) {
		return fuse.EINVAL
	}

	processed := 0
	for _, child := range ds.children[offset:] {
		entry := fuse.DirEntry{
			Mode: child.fuseType,
			Name: child.filename,
		}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			break
		}

		details.NodeId = child.attr.Ino
		details.Generation = child.generation
		if ds._generation == ds.src.generation() {
			fillEntryOutCacheData(c, details)
		} else {
			clearEntryOutCacheData(c, details)
		}
		details.Attr = child.attr

		processed++
	}
	c.qfs.incrementLookupCounts(c, ds.children[offset:int(offset)+processed])

	return fuse.OK
}

func (ds *directorySnapshot) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	c.wlog("Invalid read on directorySnapshot")
	return nil, fuse.ENOSYS
}

func (ds *directorySnapshot) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	c.wlog("Invalid write on directorySnapshot")
	return 0, fuse.ENOSYS
}
