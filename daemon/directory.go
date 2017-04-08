// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bytes"
import "errors"
import "fmt"
import "syscall"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

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

func initDirectory(c *ctx, name string, dir *Directory, wsr *WorkspaceRoot,
	baseLayerId quantumfs.ObjectKey, inodeNum InodeId,
	parent InodeId, treeLock *sync.RWMutex) []InodeId {

	defer c.FuncIn("initDirectory",
		"baselayer from %s", baseLayerId.String()).out()

	// Set directory data before processing the children in case the children
	// access the parent.
	dir.InodeCommon.id = inodeNum
	dir.InodeCommon.name_ = name
	dir.InodeCommon.accessed_ = 0
	dir.setParent(parent)
	dir.treeLock_ = treeLock
	dir.wsr = wsr
	dir.baseLayerId = baseLayerId

	uninstantiated := make([]InodeId, 0)

	key := baseLayerId
	for {
		c.vlog("Fetching baselayer %s", key.String())
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic("No baseLayer object")
		}

		baseLayer := buffer.AsDirectoryEntry()

		if dir.children == nil {
			dir.children = newChildMap(baseLayer.NumEntries(), wsr, dir)
		}

		for i := 0; i < baseLayer.NumEntries(); i++ {
			childInodeNum := func() InodeId {
				defer dir.childRecordLock.Lock().Unlock()
				return dir.children.loadChild(c, baseLayer.Entry(i),
					quantumfs.InodeIdInvalid)
			}()
			c.vlog("initDirectory %d getting child %d", inodeNum,
				childInodeNum)
			uninstantiated = append(uninstantiated, childInodeNum)
		}

		if baseLayer.Next() == quantumfs.EmptyDirKey ||
			baseLayer.NumEntries() == 0 {

			break
		} else {
			key = baseLayer.Next()
		}
	}

	utils.Assert(dir.treeLock() != nil, "Directory treeLock nil at init")

	return uninstantiated
}

func newDirectory(c *ctx, name string, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.funcIn("Directory::newDirectory").out()

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

// Needs inode lock for read
func (dir *Directory) updateSize_(c *ctx) {
	defer c.funcIn("Directory::updateSize_").out()

	// We think we've made a change to this directory, so we should mark it dirty
	dir.self.dirty(c)

	// The parent of a WorkspaceRoot is a workspacelist and we have nothing to
	// update.
	if !dir.self.isWorkspaceRoot() {
		c.vlog("not workspaceRoot")
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = func() uint64 {
			defer dir.childRecordLock.Lock().Unlock()
			return dir.children.count()
		}()

		dir.parentSetChildAttr(c, dir.id, nil, &attr, nil, true)
	}
}

// Needs inode lock for write
func (dir *Directory) addChild_(c *ctx, inode InodeId,
	child quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::addChild_").out()

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.loadChild(c, child, inode)
	}()
	dir.updateSize_(c)
}

// Needs inode lock and parentLock for write
func (dir *Directory) delChild_(c *ctx,
	name string) (toOrphan quantumfs.DirectoryRecord) {

	defer c.funcIn("Directory::delChild_").out()

	c.dlog("Unlinking inode %s", name)

	// If this is a file we need to reparent it to itself
	record := func() quantumfs.DirectoryRecord {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.deleteChild(c, name)
	}()

	dir.self.markAccessed(c, name, false)

	dir.updateSize_(c)

	return record
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, childId InodeId) {
	defer c.funcIn("Directory::dirtyChild").out()

	dir.self.dirty(c)
}

func fillAttrWithDirectoryRecord(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	owner fuse.Owner, entry quantumfs.DirectoryRecord) {

	defer c.FuncIn("fillAttrWithDirectoryRecord", "inode %d", inodeNum).out()

	// Ensure we're working with a shallow copy for objectTypeToFileType
	entry = entry.ShallowCopy()

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
		c.elog("Unhandled filetype in fillAttrWithDirectoryRecord",
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

	defer c.funcIn("publishDirectoryEntry").out()

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

	defer c.funcIn("publishDirectoryRecords").out()

	// Compile the internal records into a series of blocks which can be placed
	// in the datastore.
	newBaseLayerId := quantumfs.EmptyDirKey

	// childIdx indexes into dir.childrenRecords, entryIdx indexes into the
	// metadata block
	baseLayer := quantumfs.NewDirectoryEntry()
	entryIdx := 0
	for _, child := range records {
		if entryIdx == quantumfs.MaxDirectoryRecords() {
			// This block is full, upload and create a new one
			c.vlog("Block full with %d entries", entryIdx)
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			baseLayer = quantumfs.NewDirectoryEntry()
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
	defer c.FuncIn("Directory::publish_", "%s", dir.name_).out()

	oldBaseLayer := dir.baseLayerId
	dir.baseLayerId = publishDirectoryRecords(c, dir.children.records())

	c.vlog("Directory key %s -> %s", oldBaseLayer.String(),
		dir.baseLayerId.String())
}

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("Directory::setChildAttr").out()

	result := func() fuse.Status {
		defer dir.Lock().Unlock()
		defer dir.childRecordLock.Lock().Unlock()

		entry := dir.getRecordChildCall_(c, inodeNum)
		if entry == nil {
			return fuse.ENOENT
		}

		modifyEntryWithAttr(c, newType, attr, entry, updateMtime)

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

	defer c.funcIn("Directory::Access").out()

	dir.markSelfAccessed(c, false)
	return hasAccessPermission(c, dir, mask, uid, gid)
}

func (dir *Directory) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("Directory::GetAttr").out()

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
	defer c.funcIn("Directory::Lookup").out()

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
			return inodeNum, fuse.ENOENT
		}

		c.vlog("Directory::Lookup found inode %d", inodeNum)
		dir.self.markAccessed(c, name, false)
		c.qfs.increaseLookupCount(inodeNum)

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
	defer c.FuncIn("Directory::checkHardlink", "child inode %d", childId).out()

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

	defer c.funcIn("Directory::OpenDir").out()

	err := hasPermissionOpenFlags(c, dir, flags)
	if err != fuse.OK {
		return err
	}
	dir.self.markSelfAccessed(c, false)

	ds := newDirectorySnapshot(c, dir.self.(directorySnapshotSource))
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (dir *Directory) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("Directory::getChildSnapshot").out()

	dir.self.markSelfAccessed(c, false)

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

func (dir *Directory) create_(c *ctx, name string, mode uint32, umask uint32,
	rdev uint32, constructor InodeConstructor, type_ quantumfs.ObjectType,
	key quantumfs.ObjectKey, out *fuse.EntryOut) Inode {

	defer c.funcIn("Directory::create_").out()

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
	c.qfs.increaseLookupCount(inodeNum)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner, entry)

	newEntity.dirty(c)
	newEntity.markSelfAccessed(c, true)

	return newEntity
}

func (dir *Directory) childExists(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::childExists", "name %s", name).out()

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

	defer c.funcIn("Directory::Create").out()

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

	dir.self.dirty(c)

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(file.(*File), file.inodeNum(),
		fileHandleNum, file.treeLock())
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	c.vlog("New file inode %d, fileHandle %d", file.inodeNum(), fileHandleNum)

	out.OpenOut.OpenFlags = 0
	out.OpenOut.Fh = uint64(fileHandleNum)

	return fuse.OK
}

func (dir *Directory) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	defer c.FuncIn("Directory::SetAttr", "valid %x size %d", attr.Valid,
		attr.Size).out()

	return dir.parentSetChildAttr(c, dir.InodeCommon.id, nil, attr, out,
		false)
}

func (dir *Directory) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Mkdir").out()

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
			quantumfs.ObjectTypeDirectoryEntry, quantumfs.EmptyDirKey,
			out)
		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	c.dlog("Directory::Mkdir created inode %d", out.NodeId)

	return result
}

// All modifications to the record must be done whilst holding the parentLock.
// If a function only wants to read, then it may suffice to grab a "snapshot" of it.
func (dir *Directory) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	defer c.funcIn("Directory::getChildRecord").out()

	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	if record != nil {
		return record.ShallowCopy(), nil
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
		inodeNum).out()

	record := dir.children.record(inodeNum)
	if record != nil {
		return record
	}

	// if we don't have the child, maybe we're wsr and it's a hardlink
	if dir.self.isWorkspaceRoot() {
		valid, linkRecord := dir.wsr.getHardlinkByInode(inodeNum)
		if valid {
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
	defer c.FuncIn("Directory::Unlink", "%s", name).out()

	childId := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.inodeNum(name)
	}()
	child := c.qfs.inode(c, childId)

	if child == nil {
		return fuse.ENOENT
	}

	result := child.deleteSelf(c, child, func() (quantumfs.DirectoryRecord,
		fuse.Status) {

		defer dir.Lock().Unlock()

		var recordCopy quantumfs.DirectoryRecord
		err := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()

			record := dir.children.recordByName(c, name)
			if record == nil {
				return fuse.ENOENT
			}

			recordCopy = record.ShallowCopy()
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

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) Rmdir(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Rmdir", "%s", name).out()

	childId := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.inodeNum(name)
	}()
	child := c.qfs.inode(c, childId)

	if child == nil {
		return fuse.ENOENT
	}

	result := child.deleteSelf(c, child, func() (quantumfs.DirectoryRecord,
		fuse.Status) {

		defer dir.Lock().Unlock()

		var inode InodeId
		result := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()
			record := dir.children.recordByName(c, name)
			if record == nil {
				return fuse.ENOENT
			}

			// Use a shallow copy of record to ensure the right type for
			// objectTypeToFileType
			record = record.ShallowCopy()

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

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Symlink").out()

	var key quantumfs.ObjectKey
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
		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
		c.vlog("Created new symlink with key: %s", key.String())
	}

	return result
}

func (dir *Directory) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on Directory")
	return nil, fuse.EINVAL
}

func (dir *Directory) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	defer c.funcIn("Directory::Mknod").out()

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

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	defer c.FuncIn("Directory::RenameChild", "%s -> %s", oldName, newName).out()

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		oldInodeId, oldRemoved, err := func() (InodeId, InodeId,
			fuse.Status) {

			defer dir.childRecordLock.Lock().Unlock()

			record := dir.children.recordByName(c, oldName)
			if record == nil {
				return quantumfs.InodeIdInvalid,
					quantumfs.InodeIdInvalid, fuse.ENOENT
			}

			err := hasDirectoryWritePermSticky(c, dir, record.Owner())
			if err != fuse.OK {
				return quantumfs.InodeIdInvalid,
					quantumfs.InodeIdInvalid, err
			}

			dir.self.markAccessed(c, oldName, false)

			if oldName == newName {
				// Nothing more to be done other than marking the
				// file accessed above.
				return quantumfs.InodeIdInvalid,
					quantumfs.InodeIdInvalid, fuse.OK
			}

			oldInodeId_ := dir.children.inodeNum(oldName)
			oldRemoved_ := dir.children.renameChild(c, oldName, newName)
			return oldInodeId_, oldRemoved_, fuse.OK
		}()
		if oldName == newName || err != fuse.OK {
			return err
		}

		// update the inode name
		dir.self.markAccessed(c, newName, true)
		if child := c.qfs.inodeNoInstantiate(c, oldInodeId); child != nil {
			child.setName(newName)
		}

		if oldRemoved != quantumfs.InodeIdInvalid {
			c.qfs.removeUninstantiated(c, []InodeId{oldRemoved})
		}

		dir.updateSize_(c)

		return fuse.OK
	}()

	return result
}

func sortParentChild(c *ctx, a *Directory, b *Directory) (parentDir *Directory,
	childDir *Directory) {

	defer c.funcIn("sortParentChild").out()

	if a.parentHasAncestor(c, b) {
		return b, a
	}

	// If b isn't an ancestor of a, then either a is an ancestor of b or there's
	// no relationship in which case we can return the same result
	return a, b
}

func (dir *Directory) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	defer c.FuncIn("Directory::MvChild", "%s -> %s", oldName,
		newName).out()

	// moving any file into _null/null is not permitted
	if _, ok := dstInode.(*NullWorkspaceRoot); ok {
		c.vlog("Cannot move into null workspace")
		return fuse.EPERM
	}

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

	result := func() fuse.Status {
		dst := asDirectory(dstInode)

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
		//
		// However, there is another wrinkle. It is possible to rename a file
		// from a directory into its parent. If we keep the parent locked
		// while we run dir.updateSize_(), then we'll deadlock as we try to
		// lock the parent again down the call stack.
		//
		// So we have two phases of locking. In the first phase we lock dir
		// and dst according to their inode number. Then, with both those
		// locks held we perform the bulk of the logic. Just before we start
		// releasing locks we update the parent metadata. Then we drop the
		// locks of the parent, which is fine since it is up to date. Finally
		// we update the metadata of the child before dropping its lock.
		//
		// We need to update and release the parent first so we can
		// successfully update the child. If the two directories are not
		// related in that way then we choose arbitrarily because it doesn't
		// matter.
		parent, child := sortParentChild(c, dst, dir)
		firstLock, lastLock := getLockOrder(dst, dir)
		firstLock.Lock()
		lastLock.Lock()

		defer child.lock.Unlock()

		result := func() fuse.Status {
			// we need to unlock the parent early
			defer parent.lock.Unlock()

			newEntry, oldInodeId,
				err := func() (quantumfs.DirectoryRecord, InodeId,
				fuse.Status) {

				defer dir.childRecordLock.Lock().Unlock()

				record := dir.children.recordByName(c, oldName)
				oldInodeId_ := dir.children.inodeNum(oldName)
				if record == nil {
					return nil, 0, fuse.ENOENT
				}

				// copy the record
				newEntry_ := record.Clone()
				return newEntry_, oldInodeId_, fuse.OK
			}()
			if err != fuse.OK {
				return err
			}

			// fix the name on the copy
			newEntry.SetFilename(newName)

			isHardlink, _ := dir.wsr.checkHardlink(oldInodeId)
			var childInode Inode
			if !isHardlink {
				// Update the inode to point to the new name and
				// mark as accessed in both parents.
				childInode = c.qfs.inodeNoInstantiate(c, oldInodeId)
				if childInode != nil {
					childInode.setParent(dst.inodeNum())
					childInode.setName(newName)
				}
			}
			dir.self.markAccessed(c, oldName, false)
			dst.self.markAccessed(c, newName, true)

			func() {
				defer dir.childRecordLock.Lock().Unlock()

				// Delete the target InodeId, before (possibly)
				// overwriting it.
				dst.deleteEntry_(c, newName)

				overwrittenRecord := dst.children.recordByName(c,
					newName)
				overwrittenId := dst.children.inodeNum(newName)
				if overwrittenRecord != nil {
					c.qfs.removeUninstantiated(c,
						[]InodeId{overwrittenId})
				}

				dst.insertEntry_(c, newEntry, oldInodeId, childInode)

				// Remove entry in old directory
				dir.deleteEntry_(c, oldName)
			}()

			// Set entry in new directory. If the renamed inode is
			// uninstantiated, we swizzle the parent here.
			if childInode == nil {
				c.qfs.addUninstantiated(c, []InodeId{oldInodeId},
					dst.inodeNum())
			}

			parent.updateSize_(c)

			return fuse.OK
		}()

		if result == fuse.OK {
			child.updateSize_(c)
		}
		return result
	}()

	return result
}

// Must hold childrecord lock for writing
func (dir *Directory) deleteEntry_(c *ctx, name string) {
	defer c.FuncIn("Directory::deleteEntry_", "name %s", name).out()

	if record := dir.children.recordByName(c, name); record == nil {
		// Nothing to do
		return
	}

	dir.children.deleteChild(c, name)
}

// Needs to hold childRecordLock
func (dir *Directory) insertEntry_(c *ctx, entry quantumfs.DirectoryRecord,
	inodeNum InodeId, childInode Inode) {

	defer c.FuncIn("DirectoryRecord::insertEntry_", "inode %d", inodeNum).out()

	dir.children.loadChild(c, entry, inodeNum)

	// being inserted means you're dirty and need to be synced
	if childInode != nil {
		childInode.dirty(c)
	}
	dir.self.dirty(c)
}

func (dir *Directory) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	defer c.FuncIn("Directory::GetXAttrSize", "attr %s", attr).out()

	return dir.parentGetChildXAttrSize(c, dir.inodeNum(), attr)
}

func (dir *Directory) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	defer c.FuncIn("Directory::GetXAttrData", "attr %s", attr).out()

	return dir.parentGetChildXAttrData(c, dir.inodeNum(), attr)
}

func (dir *Directory) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	defer c.funcIn("Directory::ListXAttr").out()
	return dir.parentListChildXAttr(c, dir.inodeNum())
}

func (dir *Directory) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	defer c.FuncIn("Directory::SetXAttr", "attr %s", attr).out()
	return dir.parentSetChildXAttr(c, dir.inodeNum(), attr, data)
}

func (dir *Directory) RemoveXAttr(c *ctx, attr string) fuse.Status {
	defer c.FuncIn("Directory::RemoveXAttr", "attr %s", attr).out()
	return dir.parentRemoveChildXAttr(c, dir.inodeNum(), attr)
}

func (dir *Directory) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	defer c.FuncIn("Directory::syncChild", "dir inode %d child inode %d) %s",
		dir.inodeNum(), inodeNum, newKey.String()).out()

	defer dir.Lock().Unlock()
	dir.self.dirty(c)
	defer dir.childRecordLock.Lock().Unlock()

	entry := dir.getRecordChildCall_(c, inodeNum)
	if entry == nil {
		c.wlog("Directory::syncChild inode %d not a valid child",
			inodeNum)
		return
	}

	entry.SetID(newKey)
}

// Get the extended attributes object. The status is EIO on error or ENOENT if there
// are no extended attributes for that child.
func (dir *Directory) getExtendedAttributes_(c *ctx,
	inodeNum InodeId) (*quantumfs.ExtendedAttributes, fuse.Status) {

	defer c.funcIn("Directory::getExtendedAttributes_").out()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.getRecordChildCall_(c, inodeNum)
	if record == nil {
		c.vlog("Child not found")
		return nil, fuse.EIO
	}

	if record.ExtendedAttributes().IsEqualTo(quantumfs.EmptyBlockKey) {
		c.vlog("Directory::getExtendedAttributes_ returning new object")
		return nil, fuse.ENOENT
	}

	buffer := c.dataStore.Get(&c.Ctx, record.ExtendedAttributes())
	if buffer == nil {
		c.dlog("Failed to retrieve attribute list")
		return nil, fuse.EIO
	}

	attributeList := buffer.AsExtendedAttributes()
	return &attributeList, fuse.OK
}

func (dir *Directory) getChildXAttrBuffer(c *ctx, inodeNum InodeId,
	attr string) (quantumfs.Buffer, fuse.Status) {

	defer c.FuncIn("Directory::getChildXAttrBuffer", "%d %s", inodeNum,
		attr).out()

	defer dir.RLock().RUnlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)
	if ok == fuse.ENOENT {
		return nil, fuse.ENODATA
	}

	if ok == fuse.EIO {
		return nil, fuse.EIO
	}

	for i := 0; i < attributeList.NumAttributes(); i++ {
		name, key := attributeList.Attribute(i)
		if name != attr {
			continue
		}

		c.vlog("Found attribute key: %s", key.String())
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			c.elog("Failed to retrieve attribute datablock")
			return nil, fuse.EIO
		}

		return buffer, fuse.OK
	}

	c.vlog("XAttr name not found")
	return nil, fuse.ENODATA
}

func (dir *Directory) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	defer c.funcIn("Directory::getChildXAttrSize").out()

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return 0, status
	}

	return buffer.Size(), fuse.OK
}

func (dir *Directory) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	defer c.funcIn("Directory::getChildXAttrData").out()

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return []byte{}, status
	}
	return buffer.Get(), fuse.OK
}

func (dir *Directory) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	defer c.FuncIn("Directory::listChildXAttr", "%d", inodeNum).out()

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
		len(data)).out()

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
		if attributeList.NumAttributes() >
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
		dir.getRecordChildCall_(c, inodeNum).SetExtendedAttributes(key)
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.FuncIn("Directory::removeChildXAttr", "%d, %s", inodeNum, attr).out()

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
		dir.getRecordChildCall_(c, inodeNum).SetExtendedAttributes(key)
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("Directory::instantiateChild", "Inode %d of %d", inodeNum,
		dir.inodeNum()).out()
	defer dir.childRecordLock.Lock().Unlock()

	entry := dir.children.record(inodeNum)
	if entry == nil {
		panic(fmt.Sprintf("Cannot instantiate child with no record: %d",
			inodeNum))
	}

	// check if the child is a hardlink
	if isHardlink, _ := dir.wsr.checkHardlink(inodeNum); isHardlink {
		return dir.wsr.instantiateChild(c, inodeNum)
	}

	// add a check incase there's an inconsistency
	if hardlink, isHardlink := entry.(*Hardlink); isHardlink {
		panic(fmt.Sprintf("Hardlink not recognized by workspaceroot: %d, %d",
			inodeNum, hardlink.linkId))
	}

	return dir.recordToChild(c, inodeNum, entry)
}

func (dir *Directory) recordToChild(c *ctx, inodeNum InodeId,
	entry quantumfs.DirectoryRecord) (Inode, []InodeId) {

	defer c.FuncIn("DirectoryRecord::recordToChild", "name %s inode %d",
		entry.Filename(), inodeNum).out()

	var constructor InodeConstructor
	switch entry.Type() {
	default:
		c.elog("Unknown InodeConstructor type: %d", entry.Type())
		panic("Unknown InodeConstructor type")
	case quantumfs.ObjectTypeDirectoryEntry:
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

// Do a similar work like  Lookup(), but it does not interact with fuse, and return
// the child node to the caller
func (dir *Directory) lookupInternal(c *ctx, name string,
	entryType quantumfs.ObjectType) (Inode, error) {

	defer c.FuncIn("Directory::LookupInternal", "name %s", name).out()

	defer dir.RLock().RUnlock()
	inodeNum, record, err := dir.lookupChildRecord_(c, name)
	if err != nil {
		return nil, err
	}

	c.vlog("Directory::LookupInternal found inode %d Name %s", inodeNum, name)
	child := c.qfs.inode(c, inodeNum)
	child.markSelfAccessed(c, false)

	if record.Type() != entryType {
		return nil, errors.New("Not Required Type")
	}
	return child, nil
}

// Require an Inode locked for read
func (dir *Directory) lookupChildRecord_(c *ctx, name string) (InodeId,
	quantumfs.DirectoryRecord, error) {

	defer c.FuncIn("Directory::LookupChildRecord_", "name %s", name).out()

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

	defer c.FuncIn("DirectoryRecord::createNewEntry", "name %s", name).out()

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

	return entry
}

// Needs exlusive Inode lock
func (dir *Directory) duplicateInode_(c *ctx, name string, mode uint32, umask uint32,
	rdev uint32, size uint64, uid quantumfs.UID, gid quantumfs.GID,
	type_ quantumfs.ObjectType, key quantumfs.ObjectKey) {

	defer c.FuncIn("Directory::duplicateInode_", "name %s", name).out()

	entry := dir.createNewEntry(c, name, mode, umask, rdev, size,
		uid, gid, type_, key)

	defer dir.childRecordLock.Lock().Unlock()
	inodeNum := dir.children.loadChild(c, entry, quantumfs.InodeIdInvalid)

	c.qfs.addUninstantiated(c, []InodeId{inodeNum}, dir.inodeNum())
}

func (dir *Directory) flush(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("Directory::flush", "%d %s", dir.inodeNum(),
		dir.name_).out()

	defer dir.Lock().Unlock()

	dir.parentSyncChild(c, dir.inodeNum(), func() quantumfs.ObjectKey {
		defer dir.childRecordLock.Lock().Unlock()
		dir.publish_(c)
		return dir.baseLayerId
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
	treeLock() *sync.RWMutex
}

func newDirectorySnapshot(c *ctx, src directorySnapshotSource) *directorySnapshot {

	defer c.funcIn("newDirectorySnapshot").out()

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

	defer c.funcIn("directorySnapshot::ReadDirPlus").out()
	offset := input.Offset

	if offset == 0 {
		c.dlog("Refreshing child list")
		ds.children = ds.src.getChildSnapshot(c)
	}

	processed := 0
	for _, child := range ds.children {
		entry := fuse.DirEntry{
			Mode: child.fuseType,
			Name: child.filename,
		}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			break
		}

		details.NodeId = child.attr.Ino
		c.qfs.increaseLookupCount(InodeId(child.attr.Ino))
		fillEntryOutCacheData(c, details)
		details.Attr = child.attr

		processed++
	}

	ds.children = ds.children[processed:]

	return fuse.OK
}

func (ds *directorySnapshot) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	c.elog("Invalid read on directorySnapshot")
	return nil, fuse.ENOSYS
}

func (ds *directorySnapshot) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	utils.Assert(true, "Illegal call on Directory::Write()")
	c.elog("Invalid write on directorySnapshot")
	return 0, fuse.ENOSYS
}

func (ds *directorySnapshot) Sync(c *ctx) fuse.Status {
	c.vlog("directorySnapshot::Sync doing nothing")
	return fuse.OK
}
