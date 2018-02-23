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
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId)

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
}

func foreachDentry(c *ctx, key quantumfs.ObjectKey,
	visitor func(quantumfs.DirectoryRecord)) {

	for {
		c.vlog("Fetching baselayer %s", key.String())
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic("No baseLayer object")
		}
		baseLayer := buffer.AsDirectoryEntry()

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
	parent InodeId, treeLock *TreeLock) []InodeId {

	defer c.FuncIn("initDirectory",
		"baselayer from %s", baseLayerId.String()).Out()

	// Set directory data before processing the children in case the children
	// access the parent.
	dir.InodeCommon.id = inodeNum
	dir.InodeCommon.name_ = name
	dir.InodeCommon.accessed_ = 0
	dir.setParent(parent)
	dir.treeLock_ = treeLock
	dir.hardlinkTable = hardlinkTable
	dir.baseLayerId = baseLayerId
	dir.hardlinkDelta = newHardlinkDelta()

	container, uninstantiated := newChildContainer(c, dir, dir.baseLayerId)
	dir.children = container

	utils.Assert(dir.treeLock() != nil, "Directory treeLock nil at init")

	return uninstantiated
}

func newDirectory(c *ctx, name string, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

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

	uninstantiated := initDirectory(c, name, &dir, hardlinkTable,
		baseLayerId, inodeNum, parent.inodeNum(), parent.treeLock())
	return &dir, uninstantiated
}

func (dir *Directory) generation() uint64 {
	return atomic.LoadUint64(&dir._generation)
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
	atomic.AddUint64(&dir._generation, 1)

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

func (dir *Directory) prepareForOrphaning(c *ctx, name string,
	record quantumfs.DirectoryRecord) quantumfs.DirectoryRecord {

	defer c.FuncIn("Directory::prepareForOrphaning", "%s", name).Out()
	if record.Type() != quantumfs.ObjectTypeHardlink {
		return record
	}
	newRecord, _ := dir.hardlinkTable.removeHardlink(c, record.FileId())

	if newRecord != nil {
		// This was the last leg of the hardlink
		newRecord.SetFilename(name)
		return newRecord
	}
	if dir.hardlinkDec(record.FileId()) {
		// If the refcount was greater than one we shouldn't
		// reparent.
		c.vlog("Hardlink referenced elsewhere")
		return nil
	}
	return record
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx,
	name string) (toOrphan quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::delChild_").Out()

	c.dlog("Unlinking inode %s", name)

	// If this is a file we need to reparent it to itself
	record := func() quantumfs.DirectoryRecord {
		defer dir.childRecordLock.Lock().Unlock()
		detachedChild := dir.children.deleteChild(c, name)
		return dir.prepareForOrphaning(c, name, detachedChild)
	}()

	pathFlags := quantumfs.PathFlags(quantumfs.PathDeleted)
	if record != nil {
		pathFlags = markType(record.Type(), pathFlags)
	}
	dir.self.markAccessed(c, name, pathFlags)

	return record
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, childId InodeId) {
	defer c.funcIn("Directory::dirtyChild").Out()

	dir.self.dirty(c)
}

func fillAttrWithDirectoryRecord(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	owner fuse.Owner, entry_ quantumfs.ImmutableDirectoryRecord) {

	defer c.FuncIn("fillAttrWithDirectoryRecord", "inode %d", inodeNum).Out()

	// Ensure we have a flattened DirectoryRecord to ensure the type is the
	// underlying type for Hardlinks. This is required in order for
	// objectTypeToFileType() to have access to the correct type to report.
	entry := entry_.AsImmutable()

	attr.Ino = uint64(inodeNum)

	fileType := objectTypeToFileType(c, entry.Type())
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

	c.dlog("type %x permissions %o links %d",
		fileType, entry.Permissions(), attr.Nlink)

	attr.Mode = fileType | permissionsToMode(entry.Permissions())
	attr.Owner.Uid = quantumfs.SystemUid(entry.Owner(), owner.Uid)
	attr.Owner.Gid = quantumfs.SystemGid(entry.Group(), owner.Gid)
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
	quantumfs.SortDirectoryRecordsByName(records)

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
		c.vlog("Setting child %s", child.Filename())
		baseLayer.SetEntry(entryIdx, child.Publishable())

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId, pub)
	return newBaseLayerId
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

		dir.children.modifyChildWithFunc(c, inodeNum,
			func(record quantumfs.DirectoryRecord) {
				modifyEntryWithAttr(c, newType, attr, record,
					updateMtime)
			})
		entry := dir.children.recordByInodeId(c, inodeNum)

		if entry == nil && dir.self.isWorkspaceRoot() {
			// if we don't have the child, maybe we're wsr and it's a
			// hardlink
			c.vlog("Checking hardlink table")
			valid, linkRecord :=
				dir.hardlinkTable.getHardlinkByInode(inodeNum)
			if valid {
				c.vlog("Hardlink found")
				modifyEntryWithAttr(c, newType, attr, linkRecord,
					updateMtime)
				entry = linkRecord
			}
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

func (dir *Directory) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("Directory::GetAttr").Out()

	record, err := dir.parentGetChildRecordCopy(c, dir.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", dir.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, dir.InodeCommon.id,
		c.fuseCtx.Owner, record)

	return fuse.OK
}

func (dir *Directory) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	defer c.funcIn("Directory::Lookup").Out()

	checkInodeId, result := func() (InodeId, fuse.Status) {
		defer dir.RLock().RUnlock()

		checkLink, inodeNum := func() (bool, InodeId) {
			defer dir.childRecordLock.Lock().Unlock()
			record := dir.children.recordByName(c, name)
			_, isHardlink := record.(*HardlinkLeg)
			return isHardlink, dir.children.inodeNum(name)
		}()
		if inodeNum == quantumfs.InodeIdInvalid {
			c.vlog("Inode not found")
			return inodeNum, kernelCacheNegativeEntry(c, out)
		}

		c.vlog("Directory::Lookup found inode %d", inodeNum)
		c.qfs.increaseLookupCount(c, inodeNum)

		out.NodeId = uint64(inodeNum)
		fillEntryOutCacheData(c, out)
		defer dir.childRecordLock.Lock().Unlock()
		fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
			dir.children.recordByName(c, name))

		if !checkLink {
			// If we don't need to check the hardlink state, don't return
			// the inode number
			inodeNum = quantumfs.InodeIdInvalid
		}

		return inodeNum, fuse.OK
	}()

	if checkInodeId != quantumfs.InodeIdInvalid {
		// check outside of the directory lock because we're calling DOWN and
		// the child might call UP and lock us
		dir.checkHardlink(c, checkInodeId)
	}

	return result
}

func (dir *Directory) checkHardlink(c *ctx, childId InodeId) {
	defer c.FuncIn("Directory::checkHardlink", "child inode %d", childId).Out()

	child := c.qfs.inode(c, childId)
	if child != nil {
		child.parentCheckLinkReparent(c, dir)
	}
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
	c.dlog(OpenedInodeDebug, dir.inodeNum(), ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (dir *Directory) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("Directory::getChildSnapshot").Out()

	dir.self.markSelfAccessed(c, quantumfs.PathRead|quantumfs.PathIsDir)

	defer dir.RLock().RUnlock()

	children := make([]directoryContents, 0, 200) // 200 arbitrarily chosen

	c.vlog("Adding .")
	entryInfo := directoryContents{
		filename: ".",
	}

	// If we are a WorkspaceRoot then we need only add some entries,
	// WorkspaceRoot.getChildSnapShot() will overwrite the first two entries with
	// the correct data.
	if !dir.self.isWorkspaceRoot() {
		entry, err := dir.parentGetChildRecordCopy(c, dir.inodeNum())
		utils.Assert(err == nil, "Failed to get record for inode %d %s",
			dir.inodeNum(), err)

		fillAttrWithDirectoryRecord(c, &entryInfo.attr,
			dir.inodeNum(), c.fuseCtx.Owner, entry)
		entryInfo.fuseType = entryInfo.attr.Mode
	}
	children = append(children, entryInfo)

	c.vlog("Adding ..")
	entryInfo = directoryContents{
		filename: "..",
	}

	if !dir.self.isWorkspaceRoot() {
		func() {
			defer dir.parentLock.RLock().RUnlock()
			parent := dir.parent_(c)

			if parent.isWorkspaceRoot() {
				wsr := parent.(*WorkspaceRoot)
				wsr.fillWorkspaceAttrReal(c, &entryInfo.attr)
			} else {
				entry, err := parent.parentGetChildRecordCopy(c,
					parent.inodeNum())
				utils.Assert(err == nil,
					"Failed to get record for inode %d %s",
					parent.inodeNum(), err)

				c.vlog("Got record from grandparent")
				fillAttrWithDirectoryRecord(c, &entryInfo.attr,
					parent.inodeNum(), c.fuseCtx.Owner, entry)
			}
			entryInfo.fuseType = entryInfo.attr.Mode
		}()
	}
	children = append(children, entryInfo)

	c.vlog("Adding real children")

	defer dir.childRecordLock.Lock().Unlock()
	records := dir.children.records()

	for filename, entry := range records {
		entryInfo := directoryContents{
			filename: filename,
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr,
			dir.children.inodeNum(filename), c.fuseCtx.Owner, entry)
		entryInfo.fuseType = entryInfo.attr.Mode

		children = append(children, entryInfo)
	}

	// Sort the dentries so that their order is deterministic
	// on every invocation
	sort.Slice(children,
		func(i, j int) bool {
			return children[i].filename < children[j].filename
		})

	return children
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
	entry := dir.createNewEntry(c, name, mode, umask, rdev,
		0, UID, GID, type_, key)
	inodeNum := c.qfs.newInodeId()
	newEntity, uninstantiated := constructor(c, name, key, 0, inodeNum, dir.self,
		mode, rdev, entry)

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.setRecord(c, inodeNum, entry)
	}()

	c.qfs.setInode(c, inodeNum, newEntity)
	c.qfs.addUninstantiated(c, uninstantiated, inodeNum)
	c.qfs.increaseLookupCount(c, inodeNum)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner, entry)

	newEntity.dirty(c)
	pathFlags := quantumfs.PathFlags(quantumfs.PathCreated)
	if type_ == quantumfs.ObjectTypeDirectory {
		pathFlags |= quantumfs.PathIsDir
	}
	newEntity.markSelfAccessed(c, pathFlags)

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

	dir.updateSize(c, result)

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(file.(*File), file.inodeNum(),
		fileHandleNum, file.treeLock())
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

		dir.create_(c, name, input.Mode, input.Umask, 0, newDirectory,
			quantumfs.ObjectTypeDirectory, quantumfs.EmptyDirKey,
			out)
		return fuse.OK
	}()

	dir.updateSize(c, result)
	c.dlog("Directory::Mkdir created inode %d", out.NodeId)

	return result
}

// All modifications to the record must be done whilst holding the parentLock.
// If a function only wants to read, then it may suffice to grab a "snapshot" of it.
func (dir *Directory) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	defer c.funcIn("Directory::getChildRecordCopy").Out()

	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	if record != nil {
		return record.AsImmutable(), nil
	}

	return nil, errors.New("Inode given is not a child of this directory")
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
		valid, linkRecord :=
			dir.hardlinkTable.getHardlinkByInode(inodeNum)
		if valid {
			c.vlog("Hardlink found")
			return linkRecord
		}
	}

	return nil
}

func (dir *Directory) directChildInodes() []InodeId {
	defer dir.childRecordLock.Lock().Unlock()

	return dir.children.directInodes()
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Unlink", "%s", name).Out()

	childId := dir.childInodeNum(name)
	child := c.qfs.inode(c, childId)

	if child == nil {
		return fuse.ENOENT
	}

	result := child.deleteSelf(c, func() (quantumfs.DirectoryRecord,
		fuse.Status) {

		defer dir.Lock().Unlock()

		var recordCopy quantumfs.ImmutableDirectoryRecord
		err := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()

			record := dir.children.recordByName(c, name)
			if record == nil {
				return fuse.ENOENT
			}

			recordCopy = record.AsImmutable()
			return fuse.OK
		}()
		if err != fuse.OK {
			return nil, err
		}

		type_ := objectTypeToFileType(c, recordCopy.Type())

		if type_ == fuse.S_IFDIR {
			c.vlog("Directory::Unlink directory")
			return nil, fuse.Status(syscall.EISDIR)
		}

		err = hasDirectoryWritePermSticky(c, dir, recordCopy.Owner())
		if err != fuse.OK {
			return nil, err
		}

		return dir.delChild_(c, name), fuse.OK
	})

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Rmdir(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Rmdir", "%s", name).Out()

	childId := dir.childInodeNum(name)
	child := c.qfs.inode(c, childId)

	if child == nil {
		return fuse.ENOENT
	}

	result := child.deleteSelf(c, func() (quantumfs.DirectoryRecord,
		fuse.Status) {

		defer dir.Lock().Unlock()

		var inode InodeId
		result := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()
			record_ := dir.children.recordByName(c, name)
			if record_ == nil {
				return fuse.ENOENT
			}

			// Use a shallow copy of record to ensure the right type for
			// objectTypeToFileType
			record := record_.AsImmutable()

			err := hasDirectoryWritePermSticky(c, dir, record.Owner())
			if err != fuse.OK {
				return err
			}

			type_ := objectTypeToFileType(c, record.Type())
			if type_ != fuse.S_IFDIR {
				return fuse.ENOTDIR
			}

			if record.Size() != 0 {
				c.vlog("directory has %d children", record.Size())
				return fuse.Status(syscall.ENOTEMPTY)
			}

			inode = dir.children.inodeNum(name)
			return fuse.OK
		}()
		if result != fuse.OK {
			return nil, result
		}

		return dir.delChild_(c, name), fuse.OK
	})

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Symlink").Out()

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

		inode := dir.create_(c, name, 0777, 0777, 0, newSymlink,
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

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on Directory")
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

		c.dlog("Directory::Mknod Mode %x", input.Mode)
		if utils.BitFlagsSet(uint(input.Mode), syscall.S_IFIFO) ||
			utils.BitFlagsSet(uint(input.Mode), syscall.S_IFSOCK) ||
			utils.BitFlagsSet(uint(input.Mode), syscall.S_IFBLK) ||
			utils.BitFlagsSet(uint(input.Mode), syscall.S_IFCHR) {

			dir.create_(c, name, input.Mode, input.Umask, input.Rdev,
				newSpecial, quantumfs.ObjectTypeSpecial,
				zeroSpecial, out)
		} else if utils.BitFlagsSet(uint(input.Mode), syscall.S_IFREG) {
			dir.create_(c, name, input.Mode, input.Umask, 0,
				newSmallFile, quantumfs.ObjectTypeSmallFile,
				quantumfs.EmptyBlockKey, out)
		} else {
			c.dlog("Directory::Mknod invalid type %x", input.Mode)
			return fuse.EINVAL
		}
		return fuse.OK
	}()

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) RenameChild(c *ctx, oldName string,
	newName string) (result fuse.Status) {

	defer c.FuncIn("Directory::RenameChild", "%s -> %s", oldName, newName).Out()

	defer dir.updateSize(c, result)
	overwrittenInodeId := dir.childInodeNum(newName)
	overwrittenInode := c.qfs.inodeNoInstantiate(c, overwrittenInodeId)
	if overwrittenInode != nil {
		defer overwrittenInode.getParentLock().Lock().Unlock()
	}
	defer dir.Lock().Unlock()

	oldInodeId, result := func() (InodeId, fuse.Status) {
		defer dir.childRecordLock.Lock().Unlock()

		dstRecord := dir.children.recordByName(c, newName)
		if dstRecord != nil &&
			dstRecord.Type() == quantumfs.ObjectTypeDirectory &&
			dstRecord.Size() != 0 {

			// We can not overwrite a non-empty directory
			return quantumfs.InodeIdInvalid,
				fuse.Status(syscall.ENOTEMPTY)
		}

		record := dir.children.recordByName(c, oldName)
		if record == nil {
			return quantumfs.InodeIdInvalid, fuse.ENOENT
		}

		err := hasDirectoryWritePermSticky(c, dir, record.Owner())
		if err != fuse.OK {
			return quantumfs.InodeIdInvalid, err
		}

		if oldName == newName {
			return quantumfs.InodeIdInvalid, fuse.OK
		}
		oldInodeId_ := dir.children.inodeNum(oldName)
		dir.orphanChild_(c, newName, overwrittenInode)
		dir.children.renameChild(c, oldName, newName)

		// Conceptually we remove any entry in the way before we move
		// the source file in its place, so update the accessed list
		// mark in that order to ensure mark logic produces the
		// correct result.
		dir.self.markAccessed(c, oldName,
			markType(record.Type(), quantumfs.PathDeleted))
		dir.self.markAccessed(c, newName,
			markType(record.Type(), quantumfs.PathCreated))

		return oldInodeId_, fuse.OK
	}()
	if oldName == newName || result != fuse.OK {
		return
	}

	// update the inode name
	if child := c.qfs.inodeNoInstantiate(c, oldInodeId); child != nil {
		child.setName(newName)
		child.clearAccessedCache()
	}
	result = fuse.OK
	return
}

// Must hold dir and dir.childRecordLock
// Must hold the child's parentLock if inode is not nil
func (dir *Directory) orphanChild_(c *ctx, name string, inode Inode) {
	defer c.FuncIn("Directory::orphanChild_", "%s", name).Out()

	removedId := dir.children.inodeNum(name)
	removedRecord := dir.children.deleteChild(c, name)
	if removedRecord == nil {
		return
	}

	dir.self.markAccessed(c, name,
		markType(removedRecord.Type(),
			quantumfs.PathDeleted))

	removedRecord = dir.prepareForOrphaning(c, name, removedRecord)
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
}

func (dir *Directory) childInodeNum(name string) InodeId {
	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()
	return dir.children.inodeNum(name)
}

func (dir *Directory) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) (result fuse.Status) {

	defer c.FuncIn("Directory::MvChild", "%s -> %s", oldName, newName).Out()

	// check write permission for both directories
	err := hasDirectoryWritePerm(c, dstInode)
	if err != fuse.OK {
		return err
	}

	err = func() fuse.Status {
		defer dir.childRecordLock.Lock().Unlock()

		record := dir.children.recordByName(c, oldName)
		return hasDirectoryWritePermSticky(c, dir, record.Owner())
	}()
	if err != fuse.OK {
		return err
	}

	dst := asDirectory(dstInode)

	defer func() {
		dir.updateSize(c, result)
		dst.updateSize(c, result)
	}()
	childInodeId := dir.childInodeNum(oldName)
	childInode := c.qfs.inodeNoInstantiate(c, childInodeId)

	overwrittenInodeId := dst.childInodeNum(newName)
	overwrittenInode := c.qfs.inodeNoInstantiate(c, overwrittenInodeId)

	if childInode != nil {
		c.vlog("checking source for hardlink normalization")
		childInode.parentCheckLinkReparent(c, dir)
	}
	if overwrittenInode != nil {
		c.vlog("checking destination for hardlink normalization")
		overwrittenInode.parentCheckLinkReparent(c, dst)
	}

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
		// Update the inode to point to the new name and
		// mark as accessed in both parents.
		if childInode != nil {
			c.vlog("Updating name and parent")
			childInode.setParent_(dst.inodeNum())
			childInode.setName(newName)
			childInode.clearAccessedCache()
		}
	} else {
		c.vlog("Updating hardlink creation time")
		hardlink.setCreationTime(quantumfs.NewTime(time.Now()))
		newEntry.SetContentTime(hardlink.creationTime())
	}

	// Add to destination, possibly removing the overwritten inode
	c.vlog("Adding to destination directory")
	func() {
		defer dst.childRecordLock.Lock().Unlock()
		dst.orphanChild_(c, newName, overwrittenInode)
		dst.children.setRecord(c, childInodeId, newEntry)

		// Being inserted means you need to be synced to be publishable. If
		// the inode is instantiated mark it dirty, otherwise mark it
		// publishable immediately.
		if childInode != nil {
			childInode.dirty(c)
		} else {
			dst.children.makePublishable(c, newName)
		}
		dst.self.dirty(c)
	}()

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.deleteChild(c, oldName)
	}()

	// This is the same entry just moved, so we can use the same
	// record for both the old and new paths.
	dir.self.markAccessed(c, oldName,
		markType(newEntry.Type(), quantumfs.PathDeleted))
	dst.self.markAccessed(c, newName,
		markType(newEntry.Type(), quantumfs.PathCreated))

	// Set entry in new directory. If the renamed inode is
	// uninstantiated, we swizzle the parent here.
	if childInode == nil {
		c.qfs.addUninstantiated(c, []InodeId{childInodeId},
			dst.inodeNum())
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

	attributeList := buffer.AsExtendedAttributes()
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
	attr string) (quantumfs.Buffer, fuse.Status) {

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
	return buffer.Get(), fuse.OK
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
		if record != nil {
			dir.children.modifyChildWithFunc(c, inodeNum,
				func(record quantumfs.DirectoryRecord) {

					record.SetExtendedAttributes(key)
					record.SetContentTime(now)
				})
		} else if dir.self.isWorkspaceRoot() {
			// if we don't have the child, maybe we're wsr and it's a
			// hardlink
			c.vlog("Checking hardlink table")
			valid, linkRecord :=
				dir.hardlinkTable.getHardlinkByInode(inodeNum)
			if valid {
				c.vlog("Hardlink found")
				linkRecord.SetExtendedAttributes(key)
				linkRecord.SetContentTime(now)
			}
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
		if record != nil {
			dir.children.modifyChildWithFunc(c, inodeNum,
				func(record quantumfs.DirectoryRecord) {

					record.SetExtendedAttributes(key)
					record.SetContentTime(now)
				})
		} else if dir.self.isWorkspaceRoot() {
			// if we don't have the child, maybe we're wsr and it's a
			// hardlink
			c.vlog("Checking hardlink table")
			valid, linkRecord :=
				dir.hardlinkTable.getHardlinkByInode(inodeNum)
			if valid {
				c.vlog("Hardlink found")
				linkRecord.SetExtendedAttributes(key)
				linkRecord.SetContentTime(now)
			}
		}
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("Directory::instantiateChild", "Inode %d of %d", inodeNum,
		dir.inodeNum()).Out()
	defer dir.childRecordLock.Lock().Unlock()

	if inode := c.qfs.inodeNoInstantiate(c, inodeNum); inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeNum)
		return inode, nil
	}

	entry := dir.children.recordByInodeId(c, inodeNum)
	if entry == nil {
		c.elog("Cannot instantiate child with no record: %d", inodeNum)
		return nil, nil
	}

	// check if the child is a hardlink
	isHardlink, _ := dir.hardlinkTable.checkHardlink(inodeNum)
	if isHardlink {
		return dir.hardlinkTable.instantiateHardlink(c, inodeNum), nil
	}

	// add a check incase there's an inconsistency
	if hardlink, isHardlink := entry.(*HardlinkLeg); isHardlink {
		panic(fmt.Sprintf("Hardlink not recognized by workspaceroot: %d, %d",
			inodeNum, hardlink.FileId()))
	}

	return dir.recordToChild(c, inodeNum, entry)
}

func (dir *Directory) recordToChild(c *ctx, inodeNum InodeId,
	entry quantumfs.ImmutableDirectoryRecord) (Inode, []InodeId) {

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

	c.dlog("Instantiating child %d with key %s", inodeNum, entry.ID().String())

	return constructor(c, entry.Filename(), entry.ID(), entry.Size(), inodeNum,
		dir.self, 0, 0, nil)
}

// Do a similar work like Lookup(), but it does not interact with fuse, and returns
// the child node to the caller. Also, because the function probably instantiates the
// inode, it should return the boolean indicating whether this inode is instantiated
func (dir *Directory) lookupInternal(c *ctx, name string,
	entryType quantumfs.ObjectType) (child Inode, instantiated bool, err error) {

	defer c.FuncIn("Directory::lookupInternal", "name %s", name).Out()

	defer dir.RLock().RUnlock()
	inodeNum, record, err := dir.lookupChildRecord_(c, name)
	if err != nil {
		return nil, false, err
	}

	c.vlog("Directory::lookupInternal found inode %d Name %s", inodeNum, name)
	_, instantiated = c.qfs.lookupCount(c, inodeNum)
	child = c.qfs.inode(c, inodeNum)
	// Activate the lookupCount entry of currently instantiated inodes
	if !instantiated {
		c.qfs.increaseLookupCountWithNum(c, inodeNum, 0)
	}

	if record.Type() != entryType {
		return nil, instantiated, errors.New("Not Required Type")
	}
	return child, instantiated, nil
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

	inodeNum := dir.children.inodeNum(name)
	return inodeNum, record, nil
}

func (dir *Directory) createNewEntry(c *ctx, name string, mode uint32,
	umask uint32, rdev uint32, size uint64, uid quantumfs.UID,
	gid quantumfs.GID, type_ quantumfs.ObjectType,
	key quantumfs.ObjectKey) quantumfs.DirectoryRecord {

	defer c.FuncIn("DirectoryRecord::createNewEntry", "name %s", name).Out()

	// set up the Inode record
	now := time.Now()
	entry := quantumfs.NewDirectoryRecord()
	entry.SetFilename(name)
	entry.SetID(key)
	entry.SetType(type_)
	entry.SetPermissions(modeToPermissions(mode, umask))
	c.dlog("Directory::createNewEntry mode %o umask %o permissions %o",
		mode, umask, entry.Permissions())
	entry.SetOwner(uid)
	entry.SetGroup(gid)
	entry.SetSize(size)
	entry.SetExtendedAttributes(quantumfs.EmptyBlockKey)
	entry.SetContentTime(quantumfs.NewTime(now))
	entry.SetModificationTime(quantumfs.NewTime(now))
	entry.SetFileId(quantumfs.GenerateUniqueFileId())

	return entry
}

// Needs exclusive Inode lock
func (dir *Directory) duplicateInode_(c *ctx, name string, mode uint32, umask uint32,
	rdev uint32, size uint64, uid quantumfs.UID, gid quantumfs.GID,
	type_ quantumfs.ObjectType, key quantumfs.ObjectKey) {

	defer c.FuncIn("Directory::duplicateInode_", "name %s", name).Out()

	entry := dir.createNewEntry(c, name, mode, umask, rdev, size,
		uid, gid, type_, key)

	inodeNum := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.loadChild(c, entry)
	}()

	c.qfs.addUninstantiated(c, []InodeId{inodeNum}, dir.inodeNum())

	c.qfs.noteChildCreated(c, dir.inodeNum(), name)

	dir.self.markAccessed(c, name, markType(type_, quantumfs.PathCreated))
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
	parent := dir.InodeCommon.parent_(c)
	parentDir := asDirectory(parent)
	parentDir.markHardlinkPath(c, path, fileId)
}

func (dir *Directory) flush(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("Directory::flush", "%d %s", dir.inodeNum(),
		dir.name_).Out()

	dir.parentSyncChild(c, func() (quantumfs.ObjectKey, *HardlinkDelta) {
		defer dir.childRecordLock.Lock().Unlock()
		dir.publish_(c)
		return dir.baseLayerId, dir.hardlinkDelta
	})

	return dir.baseLayerId
}

func (dir *Directory) hardlinkInc(fileId quantumfs.FileId) {
	dir.hardlinkDelta.inc(fileId)
	dir.hardlinkTable.hardlinkInc(fileId)
}

func (dir *Directory) hardlinkDec(fileId quantumfs.FileId) bool {
	dir.hardlinkDelta.dec(fileId)
	return dir.hardlinkTable.hardlinkDec(fileId)
}

type directoryContents struct {
	// All immutable after creation
	filename string
	fuseType uint32 // One of fuse.S_IFDIR, S_IFREG, etc
	attr     fuse.Attr
}

type directorySnapshotSource interface {
	getChildSnapshot(c *ctx) []directoryContents
	inodeNum() InodeId
	treeLock() *TreeLock
	generation() uint64
}

func newDirectorySnapshot(c *ctx, src directorySnapshotSource) *directorySnapshot {

	defer c.funcIn("newDirectorySnapshot").Out()

	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  src.inodeNum(),
			treeLock_: src.treeLock(),
		},
		_generation: src.generation(),
		src:         src,
	}

	utils.Assert(ds.treeLock() != nil, "directorySnapshot treeLock nil at init")

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
		c.dlog("Refreshing child list")
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
		c.qfs.increaseLookupCount(c, InodeId(child.attr.Ino))
		if ds._generation == ds.src.generation() {
			fillEntryOutCacheData(c, details)
		} else {
			clearEntryOutCacheData(c, details)
		}
		details.Attr = child.attr

		processed++
	}

	return fuse.OK
}

func (ds *directorySnapshot) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	c.elog("Invalid read on directorySnapshot")
	return nil, fuse.ENOSYS
}

func (ds *directorySnapshot) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	c.elog("Invalid write on directorySnapshot")
	return 0, fuse.ENOSYS
}

func (ds *directorySnapshot) Sync(c *ctx) fuse.Status {
	c.vlog("directorySnapshot::Sync doing nothing")
	return fuse.OK
}
