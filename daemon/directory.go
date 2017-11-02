// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"bytes"
	"errors"
	"fmt"
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

	wsr *WorkspaceRoot

	// These fields are protected by the InodeCommon.lock
	baseLayerId quantumfs.ObjectKey

	// childRecordLock protects the maps inside childMap as well as the
	// records contained within those maps themselves. This lock is not
	// the same as the Directory Inode lock because these records must be
	// accessible in instantiateChild(), which may be called indirectly
	// via qfs.inode() from a context where the Inode lock is already
	// held.
	childRecordLock utils.DeferableMutex
	children        *ChildMap
}

func foreachDentry(c *ctx, key quantumfs.ObjectKey,
	visitor func(*quantumfs.DirectRecord)) {

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

func initDirectory(c *ctx, name string, dir *Directory, wsr *WorkspaceRoot,
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
	dir.wsr = wsr
	dir.baseLayerId = baseLayerId

	cmap, uninstantiated := newChildMap(c, wsr, dir.baseLayerId)
	dir.children = cmap

	utils.Assert(dir.treeLock() != nil, "Directory treeLock nil at init")

	return uninstantiated
}

func newDirectory(c *ctx, name string, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.funcIn("Directory::newDirectory").Out()

	var dir Directory
	dir.self = &dir

	var wsr *WorkspaceRoot
	switch v := parent.(type) {
	case *Directory:
		wsr = v.wsr
	case *WorkspaceRoot:
		wsr = v
	default:
		panic(fmt.Sprintf("Parent of inode %d is neither "+
			"Directory nor WorkspaceRoot", inodeNum))
	}

	uninstantiated := initDirectory(c, name, &dir, wsr, baseLayerId,
		inodeNum, parent.inodeNum(), parent.treeLock())
	return &dir, uninstantiated
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

// Needs inode lock for write
func (dir *Directory) addChild_(c *ctx, inode InodeId,
	child quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::addChild_").Out()
	defer dir.childRecordLock.Lock().Unlock()
	dir.children.loadChild(c, child, inode)
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx,
	name string) (toOrphan quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::delChild_").Out()

	c.dlog("Unlinking inode %s", name)

	// If this is a file we need to reparent it to itself
	record := func() quantumfs.DirectoryRecord {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.deleteChild(c, name, true)
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
	entry := entry_.AsImmutableDirectoryRecord()

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

	c.dlog("fillAttrWithDirectoryRecord type %x permissions %o links %d",
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
	nextKey quantumfs.ObjectKey) quantumfs.ObjectKey {

	defer c.funcIn("publishDirectoryEntry").Out()

	layer.SetNext(nextKey)
	bytes := layer.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newKey, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new baseLayer object")
	}

	return newKey
}

func publishDirectoryRecords(c *ctx,
	records []quantumfs.DirectoryRecord) quantumfs.ObjectKey {

	defer c.funcIn("publishDirectoryRecords").Out()

	numEntries := len(records)

	// Compile the internal records into a series of blocks which can be placed
	// in the datastore.
	newBaseLayerId := quantumfs.EmptyDirKey

	// childIdx indexes into dir.childrenRecords, entryIdx indexes into the
	// metadata block
	numEntries, baseLayer := quantumfs.NewDirectoryEntry(numEntries)
	entryIdx := 0
	quantumfs.SortDirectoryRecordsByName(records)

	for _, child := range records {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			c.vlog("Block full with %d entries", entryIdx)
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			numEntries, baseLayer =
				quantumfs.NewDirectoryEntry(numEntries)
			entryIdx = 0
		}

		recordCopy := child.Record()
		baseLayer.SetEntry(entryIdx, &recordCopy)

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId)
	return newBaseLayerId
}

// Must hold the dir.childRecordsLock
func (dir *Directory) publish_(c *ctx) {
	defer c.FuncIn("Directory::publish_", "%s", dir.name_).Out()

	oldBaseLayer := dir.baseLayerId
	dir.baseLayerId = publishDirectoryRecords(c, dir.children.records())

	c.vlog("Directory key %s -> %s", oldBaseLayer.String(),
		dir.baseLayerId.String())
}

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("Directory::setChildAttr").Out()

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.setOrphanChildAttr(c, inodeNum, attr, out, updateMtime)
	}

	result := func() fuse.Status {
		defer dir.Lock().Unlock()
		defer dir.childRecordLock.Lock().Unlock()

		entry := dir.getRecordChildCall_(c, inodeNum)
		if entry == nil {
			return fuse.ENOENT
		}

		modifyEntryWithAttr(c, attr, entry, updateMtime)

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
			_, isHardlink := record.(*Hardlink)
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
	c.dlog("Opened Inode %d as Fh: %d", dir.inodeNum(), ds.FileHandleCommon.id)
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

	for _, entry := range records {
		filename := entry.Filename()

		entryInfo := directoryContents{
			filename: filename,
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr,
			dir.children.inodeNum(filename), c.fuseCtx.Owner, entry)
		entryInfo.fuseType = entryInfo.attr.Mode

		children = append(children, entryInfo)
	}

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
	dir.addChild_(c, inodeNum, entry)
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

	c.vlog("New file inode %d, fileHandle %d", file.inodeNum(), fileHandleNum)

	out.OpenOut.OpenFlags = fuse.FOPEN_KEEP_CACHE
	out.OpenOut.Fh = uint64(fileHandleNum)

	return fuse.OK
}

func (dir *Directory) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	defer c.funcIn("Directory::SetAttr").Out()

	return dir.parentSetChildAttr(c, dir.InodeCommon.id, attr, out, false)
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

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.getOrphanChildRecordCopy(c, inodeNum)
	}

	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	if record != nil {
		return record.AsImmutableDirectoryRecord(), nil
	}

	return &quantumfs.DirectRecord{},
		errors.New("Inode given is not a child of this directory")
}

// must have childRecordLock, fetches the child for calls that come UP from a child.
// Should not be used by functions which aren't routed from a child, as even if dir
// is wsr it should not accommodate getting hardlink records in those situations
func (dir *Directory) getRecordChildCall_(c *ctx,
	inodeNum InodeId) quantumfs.DirectoryRecord {

	defer c.FuncIn("DirectoryRecord::getRecordChildCall_", "inode %d",
		inodeNum).Out()

	record := dir.children.record(inodeNum)
	if record != nil {
		c.vlog("Record found")
		return record
	}

	// if we don't have the child, maybe we're wsr and it's a hardlink
	if dir.self.isWorkspaceRoot() {
		c.vlog("Checking hardlink table")
		valid, linkRecord := dir.wsr.getHardlinkByInode(inodeNum)
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

			recordCopy = record.AsImmutableDirectoryRecord()
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
			record := record_.AsImmutableDirectoryRecord()

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

		buf := newBuffer(c, []byte(pointedTo), quantumfs.KeyTypeMetadata)
		key, err := buf.Key(&c.Ctx)
		if err != nil {
			c.elog("Failed to upload block: %v", err)
			return fuse.EIO
		}

		dir.create_(c, name, 0777, 0777, 0, newSymlink,
			quantumfs.ObjectTypeSymlink, key, out)
		c.vlog("Created new symlink with key: %s", key.String())
		return fuse.OK
	}()

	dir.updateSize(c, result)
	return result
}

func (dir *Directory) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on Directory")
	return nil, fuse.EINVAL
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
				quantumfs.ZeroKey, out)
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
	removedRecord := dir.children.recordByName(c, name)
	if removedRecord == nil {
		return
	}
	dir.self.markAccessed(c, name,
		markType(removedRecord.Type(),
			quantumfs.PathDeleted))
	removedId := dir.children.inodeNum(name)
	dir.children.deleteChild(c, name, true)
	if removedId == quantumfs.InodeIdInvalid {
		return
	}
	if removedRecord.Type() == quantumfs.ObjectTypeHardlink {
		c.vlog("nothing to do for the detached leg of hardlink")
		// XXX handle the case where the Record is the last leg of
		// a not-yet-normalized hardlink with nlink of 1
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

	newEntry, result := func() (quantumfs.DirectoryRecord, fuse.Status) {
		defer dir.childRecordLock.Lock().Unlock()
		record := dir.children.recordByName(c, oldName)
		if record == nil {
			return nil, fuse.ENOENT
		}

		// copy the record
		newEntry_ := record.Clone()
		return newEntry_, fuse.OK
	}()
	if result != fuse.OK {
		return
	}

	// fix the name on the copy
	newEntry.SetFilename(newName)

	hardlink, isHardlink := newEntry.(*Hardlink)
	if !isHardlink {
		// Update the inode to point to the new name and
		// mark as accessed in both parents.
		if childInode != nil {
			childInode.setParent_(dst.inodeNum())
			childInode.setName(newName)
			childInode.clearAccessedCache()
		}
	} else {
		hardlink.creationTime = quantumfs.NewTime(time.Now())
		newEntry.SetContentTime(hardlink.creationTime)
	}

	func() {
		defer dst.childRecordLock.Lock().Unlock()
		dst.orphanChild_(c, newName, overwrittenInode)
		dst.insertEntry_(c, newEntry, childInodeId, childInode)
	}()

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.deleteChild(c, oldName, true)
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

// Needs to hold childRecordLock
func (dir *Directory) insertEntry_(c *ctx, entry quantumfs.DirectoryRecord,
	inodeNum InodeId, childInode Inode) {

	defer c.FuncIn("DirectoryRecord::insertEntry_", "inode %d", inodeNum).Out()

	dir.children.loadChild(c, entry, inodeNum)

	// being inserted means you're dirty and need to be synced
	if childInode != nil {
		childInode.dirty(c)
	}
	dir.self.dirty(c)
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
	newKey quantumfs.ObjectKey, newType quantumfs.ObjectType) {

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

	entry.SetID(newKey)

	if newType != quantumfs.ObjectTypeInvalid {
		entry.SetType(newType)
	}
}

func getRecordExtendedAttributes(c *ctx,
	attrKey quantumfs.ObjectKey) (*quantumfs.ExtendedAttributes,
	fuse.Status) {

	if attrKey.IsEqualTo(quantumfs.EmptyBlockKey) {
		c.vlog("Directory::getRecordExtendedAttributes returning new object")
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

	key := attributeList.AttributeByKey(attr)
	if key == quantumfs.EmptyBlockKey {
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

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.getOrphanChildXAttrSize(c, inodeNum, attr)
	}

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return 0, status
	}

	return buffer.Size(), fuse.OK
}

func (dir *Directory) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("Directory::getChildXAttrData").Out()

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.getOrphanChildXAttrData(c, inodeNum, attr)
	}

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return []byte{}, status
	}
	return buffer.Get(), fuse.OK
}

func (dir *Directory) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.FuncIn("Directory::listChildXAttr", "%d", inodeNum).Out()

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.listOrphanChildXAttr(c, inodeNum)
	}

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

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.setOrphanChildXAttr(c, inodeNum, attr, data)
	}

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
		record := dir.getRecordChildCall_(c, inodeNum)
		record.SetExtendedAttributes(key)
		record.SetContentTime(quantumfs.NewTime(time.Now()))
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.FuncIn("Directory::removeChildXAttr", "%d, %s", inodeNum, attr).Out()

	if dir.isOrphaned() && dir.id == inodeNum {
		return dir.removeOrphanChildXAttr(c, inodeNum, attr)
	}

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
		record := dir.getRecordChildCall_(c, inodeNum)
		record.SetExtendedAttributes(key)
		record.SetContentTime(quantumfs.NewTime(time.Now()))
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

	entry := dir.children.record(inodeNum)
	if entry == nil {
		c.elog("Cannot instantiate child with no record: %d", inodeNum)
		return nil, nil
	}

	// check if the child is a hardlink
	if isHardlink, _ := dir.wsr.checkHardlink(inodeNum); isHardlink {
		return dir.wsr.instantiateChild(c, inodeNum)
	}

	// add a check incase there's an inconsistency
	if hardlink, isHardlink := entry.(*Hardlink); isHardlink {
		panic(fmt.Sprintf("Hardlink not recognized by workspaceroot: %d, %d",
			inodeNum, hardlink.fileId))
	}

	return dir.recordToChild(c, inodeNum, entry)
}

func (dir *Directory) recordToChild(c *ctx, inodeNum InodeId,
	entry quantumfs.DirectoryRecord) (Inode, []InodeId) {

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
	_, instantiated = c.qfs.lookupCount(inodeNum)
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
	quantumfs.DirectoryRecord, error) {

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
		return dir.children.loadChild(c, entry, quantumfs.InodeIdInvalid)
	}()

	c.qfs.addUninstantiated(c, []InodeId{inodeNum}, dir.inodeNum())

	go c.qfs.noteChildCreated(dir.inodeNum(), name)

	dir.self.markAccessed(c, name, markType(type_, quantumfs.PathCreated))
}

func (dir *Directory) flush(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("Directory::flush", "%d %s", dir.inodeNum(),
		dir.name_).Out()

	dir.parentSyncChild(c, func() (quantumfs.ObjectKey, quantumfs.ObjectType) {
		defer dir.childRecordLock.Lock().Unlock()
		dir.publish_(c)
		return dir.baseLayerId, quantumfs.ObjectTypeDirectory
	})

	return dir.baseLayerId
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
}

func newDirectorySnapshot(c *ctx, src directorySnapshotSource) *directorySnapshot {

	defer c.funcIn("newDirectorySnapshot").Out()

	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  src.inodeNum(),
			treeLock_: src.treeLock(),
		},
		src: src,
	}

	utils.Assert(ds.treeLock() != nil, "directorySnapshot treeLock nil at init")

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children []directoryContents
	src      directorySnapshotSource
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
		fillEntryOutCacheData(c, details)
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
