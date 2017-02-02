// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bytes"
import "encoding/base64"
import "encoding/binary"
import "errors"
import "fmt"
import "syscall"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

type DirectoryRecordIf interface {
	Filename() string
	SetFilename(v string)

	ID() quantumfs.ObjectKey
	SetID(v quantumfs.ObjectKey)

	Type() quantumfs.ObjectType
	SetType(v quantumfs.ObjectType)

	Permissions() uint32
	SetPermissions(v uint32)

	Owner() quantumfs.UID
	SetOwner(v quantumfs.UID)

	Group() quantumfs.GID
	SetGroup(v quantumfs.GID)

	Size() uint64
	SetSize(v uint64)

	ExtendedAttributes() quantumfs.ObjectKey
	SetExtendedAttributes(v quantumfs.ObjectKey)

	ContentTime() quantumfs.Time
	SetContentTime(v quantumfs.Time)

	ModificationTime() quantumfs.Time
	SetModificationTime(v quantumfs.Time)

	Record() *quantumfs.DirectoryRecord
}

// If dirRecord is nil, then mode, rdev and dirRecord are invalid, but the key is
// coming from a DirRecord and not passed in from create_.
//
// The return value is the newly instantiated Inode, and a list of InodeIds which
// should be added to the mux's uninstantiatedInodes collection with this new inode
// as their parent.
type InodeConstructor func(c *ctx, name string, key quantumfs.ObjectKey,
	size uint64, inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord DirectoryRecordIf) (Inode, []InodeId)

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon

	// These fields are protected by the InodeCommon.lock
	baseLayerId quantumfs.ObjectKey

	// childRecordLock protects the maps inside childMap as well as the
	// records contained within those maps themselves. This lock is not
	// the same as the Directory Inode lock because these records must be
	// accessible in instantiateChild(), which may be called indirectly
	// via qfs.inode() from a context where the Inode lock is already
	// held.
	childRecordLock DeferableMutex
	children        *ChildMap
}

// The size of the ObjectKey: 21 + 1 + 8
// The length decides the length in datastore.go: quantumfs.ExtendedKeyLength
const sourceDataLength = 30

func initDirectory(c *ctx, name string, dir *Directory,
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
			dir.children = newChildMap(baseLayer.NumEntries())
		}

		for i := 0; i < baseLayer.NumEntries(); i++ {
			childInodeNum := func() InodeId {
				defer dir.childRecordLock.Lock().Unlock()
				return dir.children.newChild(c, baseLayer.Entry(i))
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

	assert(dir.treeLock() != nil, "Directory treeLock nil at init")

	return uninstantiated
}

func newDirectory(c *ctx, name string, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord DirectoryRecordIf) (Inode, []InodeId) {

	defer c.funcIn("Directory::newDirectory").out()

	var dir Directory
	dir.self = &dir

	uninstantiated := initDirectory(c, name, &dir, baseLayerId, inodeNum,
		parent.inodeNum(), parent.treeLock())
	return &dir, uninstantiated
}

// Needs inode lock for read
func (dir *Directory) updateSize_(c *ctx) {
	defer c.funcIn("Directory::updateSize_").out()

	// The parent of a WorkspaceRoot is a workspacelist and we have nothing to
	// update.
	if !dir.self.isWorkspaceRoot() {
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = func() uint64 {
			defer dir.childRecordLock.Lock().Unlock()
			return dir.children.count()
		}()

		dir.parent(c).setChildAttr(c, dir.id, nil, &attr, nil, true)
	}
}

// Needs inode lock for write
func (dir *Directory) addChild_(c *ctx, inode InodeId, child DirectoryRecordIf) {

	defer c.funcIn("Directory::addChild_").out()

	func() {
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.setChild(c, child, inode)
	}()
	dir.updateSize_(c)
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx, inodeNum InodeId) {
	defer c.funcIn("Directory::delChild_").out()

	c.dlog("Unlinking inode %d", inodeNum)

	// If this is a file we need to reparent it to itself
	record := func() DirectoryRecordIf {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.deleteChild(inodeNum)
	}()
	if record == nil {
		c.elog("Child already deleted: %d", inodeNum)
		return
	}

	dir.self.markAccessed(c, record.Filename(), false)

	if record.Type() == quantumfs.ObjectTypeSmallFile ||
		record.Type() == quantumfs.ObjectTypeMediumFile ||
		record.Type() == quantumfs.ObjectTypeLargeFile ||
		record.Type() == quantumfs.ObjectTypeVeryLargeFile {

		inode := c.qfs.inodeNoInstantiate(c, inodeNum)
		if inode != nil {
			if file, isFile := inode.(*File); isFile {
				file.setChildRecord(c, record)
				file.setParent(file.inodeNum())
			}
		}
	}

	c.qfs.removeUninstantiated(c, []InodeId{inodeNum})
	dir.updateSize_(c)
}

func (dir *Directory) dirty(c *ctx) {
	if !dir.setDirty(true) {
		// Only go recursive if we aren't already dirty
		dir.parent(c).dirtyChild(c, dir.inodeNum())
	}
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, childId InodeId) {
	defer c.funcIn("Directory::dirtyChild").out()

	func() {
		defer dir.Lock().Unlock()
		defer dir.childRecordLock.Lock().Unlock()
		dir.children.setDirty(c, childId)
	}()
	dir.self.dirty(c)
}

func fillAttrWithDirectoryRecord(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	owner fuse.Owner, entry DirectoryRecordIf) {

	attr.Ino = uint64(inodeNum)

	fileType := objectTypeToFileType(c, entry.Type())
	switch fileType {
	case fuse.S_IFDIR:
		attr.Size = qfsBlockSize
		attr.Blocks = BlocksRoundUp(attr.Size, statBlockSize)
		attr.Nlink = uint32(entry.Size()) + 2
	case fuse.S_IFIFO:
		fileType = specialOverrideAttr(entry, attr)
	default:
		c.elog("Unhandled filetype in fillAttrWithDirectoryRecord",
			fileType)
		fallthrough
	case fuse.S_IFREG,
		fuse.S_IFLNK:

		attr.Size = entry.Size()
		attr.Blocks = BlocksRoundUp(entry.Size(), statBlockSize)
		attr.Nlink = 2 // Workaround for BUG166665
	}

	attr.Atime = entry.ModificationTime().Seconds()
	attr.Mtime = entry.ModificationTime().Seconds()
	attr.Ctime = entry.ContentTime().Seconds()
	attr.Atimensec = entry.ModificationTime().Nanoseconds()
	attr.Mtimensec = entry.ModificationTime().Nanoseconds()
	attr.Ctimensec = entry.ContentTime().Nanoseconds()

	c.dlog("fillAttrWithDirectoryRecord fileType %x permissions %o", fileType,
		entry.Permissions())

	attr.Mode = fileType | permissionsToMode(entry.Permissions())
	attr.Owner.Uid = quantumfs.SystemUid(entry.Owner(), owner.Uid)
	attr.Owner.Gid = quantumfs.SystemGid(entry.Group(), owner.Gid)
	attr.Blksize = uint32(qfsBlockSize)
}

func permissionsToMode(permissions uint32) uint32 {
	var mode uint32

	if BitFlagsSet(uint(permissions), quantumfs.PermExecOther) {
		mode |= syscall.S_IXOTH
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermWriteOther) {
		mode |= syscall.S_IWOTH
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermReadOther) {
		mode |= syscall.S_IROTH
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermExecGroup) {
		mode |= syscall.S_IXGRP
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermWriteGroup) {
		mode |= syscall.S_IWGRP
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermReadGroup) {
		mode |= syscall.S_IRGRP
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermExecOwner) {
		mode |= syscall.S_IXUSR
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermWriteOwner) {
		mode |= syscall.S_IWUSR
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermReadOwner) {
		mode |= syscall.S_IRUSR
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermSticky) {
		mode |= syscall.S_ISVTX
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermSGID) {
		mode |= syscall.S_ISGID
	}
	if BitFlagsSet(uint(permissions), quantumfs.PermSUID) {
		mode |= syscall.S_ISUID
	}

	return mode
}

func modeToPermissions(mode uint32, umask uint32) uint32 {
	var permissions uint32
	mode = mode & ^umask

	if BitFlagsSet(uint(mode), syscall.S_IXOTH) {
		permissions |= quantumfs.PermExecOther
	}
	if BitFlagsSet(uint(mode), syscall.S_IWOTH) {
		permissions |= quantumfs.PermWriteOther
	}
	if BitFlagsSet(uint(mode), syscall.S_IROTH) {
		permissions |= quantumfs.PermReadOther
	}
	if BitFlagsSet(uint(mode), syscall.S_IXGRP) {
		permissions |= quantumfs.PermExecGroup
	}
	if BitFlagsSet(uint(mode), syscall.S_IWGRP) {
		permissions |= quantumfs.PermWriteGroup
	}
	if BitFlagsSet(uint(mode), syscall.S_IRGRP) {
		permissions |= quantumfs.PermReadGroup
	}
	if BitFlagsSet(uint(mode), syscall.S_IXUSR) {
		permissions |= quantumfs.PermExecOwner
	}
	if BitFlagsSet(uint(mode), syscall.S_IWUSR) {
		permissions |= quantumfs.PermWriteOwner
	}
	if BitFlagsSet(uint(mode), syscall.S_IRUSR) {
		permissions |= quantumfs.PermReadOwner
	}
	if BitFlagsSet(uint(mode), syscall.S_ISVTX) {
		permissions |= quantumfs.PermSticky
	}
	if BitFlagsSet(uint(mode), syscall.S_ISGID) {
		permissions |= quantumfs.PermSGID
	}
	if BitFlagsSet(uint(mode), syscall.S_ISUID) {
		permissions |= quantumfs.PermSUID
	}

	return permissions
}

func publishDirectoryEntry(c *ctx, layer *quantumfs.DirectoryEntry,
	nextKey quantumfs.ObjectKey) quantumfs.ObjectKey {

	layer.SetNext(nextKey)
	bytes := layer.Bytes()

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newKey, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new baseLayer object")
	}

	return newKey
}

func publishDirectoryRecordIfs(c *ctx,
	records []DirectoryRecordIf) quantumfs.ObjectKey {

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
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			baseLayer = quantumfs.NewDirectoryEntry()
			entryIdx = 0
		}

		baseLayer.SetEntry(entryIdx, child.Record())

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId)
	return newBaseLayerId
}

// Must hold the dir.childRecordsLock
func (dir *Directory) publish_(c *ctx) quantumfs.ObjectKey {
	defer c.FuncIn("Directory::publish", "%s", dir.name_).out()

	oldBaseLayer := dir.baseLayerId
	dir.baseLayerId = publishDirectoryRecordIfs(c, dir.children.records())

	c.vlog("Directory key %s -> %s", oldBaseLayer.String(),
		dir.baseLayerId.String())

	if !dir.self.isWorkspaceRoot() {
		// TODO This violates layering and is ugly
		dir.setDirty(false)
	}

	return dir.baseLayerId
}

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	defer c.funcIn("Directory::setChildAttr").out()

	result := func() fuse.Status {
		defer dir.Lock().Unlock()
		defer dir.childRecordLock.Lock().Unlock()

		entry := dir.children.record(inodeNum)
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

	c.elog("Unsupported Access on Directory")
	return fuse.ENOSYS
}

func (dir *Directory) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("Directory::GetAttr").out()

	record, err := dir.parent(c).getChildRecord(c, dir.InodeCommon.id)
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

	defer dir.RLock().RUnlock()

	inodeNum := func() InodeId {
		defer dir.childRecordLock.Lock().Unlock()
		return dir.children.inodeNum(name)
	}()
	if inodeNum == quantumfs.InodeIdInvalid {
		return fuse.ENOENT
	}

	c.vlog("Directory::Lookup found inode %d", inodeNum)
	dir.self.markAccessed(c, name, false)
	c.qfs.increaseLookupCount(inodeNum)

	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	defer dir.childRecordLock.Lock().Unlock()
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		dir.children.recordByName(c, name))

	return fuse.OK
}

func (dir *Directory) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOSYS
}

func (dir *Directory) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("Directory::OpenDir").out()

	ds := newDirectorySnapshot(c, dir.self.(directorySnapshotSource))
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (dir *Directory) getChildSnapshot(c *ctx) []directoryContents {
	dir.self.markSelfAccessed(c, false)

	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	records := dir.children.records()
	children := make([]directoryContents, 0, len(records))
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
	UID := quantumfs.ObjectUid(c.Ctx, uid, uid)
	GID := quantumfs.ObjectGid(c.Ctx, gid, gid)
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

	newEntity.markSelfAccessed(c, true)

	return newEntity
}

func (dir *Directory) childExists(c *ctx, name string) fuse.Status {
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.children.recordByName(c, name)
	if record != nil {
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

		err := dir.hasWritePermission(c, c.fuseCtx.Owner.Uid, false)
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

	return dir.parent(c).setChildAttr(c, dir.InodeCommon.id, nil, attr, out,
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

		err := dir.hasWritePermission(c, c.fuseCtx.Owner.Uid, false)
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

func (dir *Directory) getChildRecord(c *ctx,
	inodeNum InodeId) (DirectoryRecordIf, error) {

	defer c.funcIn("Directory::getChildRecord").out()
	defer dir.RLock().RUnlock()
	defer dir.childRecordLock.Lock().Unlock()

	if record := dir.children.record(inodeNum); record != nil {
		return record, nil
	}

	return &quantumfs.DirectoryRecord{},
		errors.New("Inode given is not a child of this directory")
}

func (dir *Directory) hasWritePermission(c *ctx, fileOwner uint32,
	checkStickyBit bool) fuse.Status {

	var arg string
	if checkStickyBit {
		arg = "checkStickyBit"
	} else {
		arg = "no checkStickyBit"
	}
	defer c.FuncIn("Directory::hasWritePermission", arg).out()

	// If the directory is a workspace root, it is always permitted to modify the
	// children inodes because its permission is 777 (Hardcoded in
	// daemon/workspaceroot.go).
	if dir.self.isWorkspaceRoot() {
		c.vlog("Is WorkspaceRoot: OK")
		return fuse.OK
	}

	owner := c.fuseCtx.Owner
	dirRecord, err := dir.parent(c).getChildRecord(c, dir.InodeCommon.id)
	if err != nil {
		c.wlog("Failed to find directory record in parent")
		return fuse.ENOENT
	}
	dirOwner := quantumfs.SystemUid(dirRecord.Owner(), owner.Uid)
	dirGroup := quantumfs.SystemGid(dirRecord.Group(), owner.Gid)
	permission := dirRecord.Permissions()

	// Root permission can bypass the permission, and the root is only verified
	// by uid
	if owner.Uid == 0 {
		c.vlog("User is root: OK")
		return fuse.OK
	}

	// Verify the permission of the directory in order to delete a child
	// If the sticky bit of the directory is set, the action can only be
	// performed by file's owner, directory's owner, or root user
	if checkStickyBit && BitFlagsSet(uint(permission), uint(syscall.S_ISVTX)) &&
		owner.Uid != fileOwner && owner.Uid != dirOwner {

		c.vlog("Sticky owners don't match: FAIL")
		return fuse.EACCES
	}

	// Get whether current user is OWNER/GRP/OTHER
	var permWX uint32
	if owner.Uid == dirOwner {
		permWX = syscall.S_IWUSR | syscall.S_IXUSR
		// Check the current directory having x and w permissions
		if BitFlagsSet(uint(permission), uint(permWX)) {
			c.vlog("Has owner write: OK")
			return fuse.OK
		}
	} else if owner.Gid == dirGroup {
		permWX = syscall.S_IWGRP | syscall.S_IXGRP
		if BitFlagsSet(uint(permission), uint(permWX)) {
			c.vlog("Has group write: OK")
			return fuse.OK
		}
	} else { // all the other
		permWX = syscall.S_IWOTH | syscall.S_IXOTH
		if BitFlagsSet(uint(permission), uint(permWX)) {
			c.vlog("Has other write: OK")
			return fuse.OK
		}
	}

	c.vlog("Directory::hasWritePermission %o vs %o", permWX, permission)
	return fuse.EACCES
}

func (dir *Directory) childInodes() []InodeId {
	defer dir.childRecordLock.Lock().Unlock()

	return dir.children.inodes()
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Unlink", "%s", name).out()

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		var recordType quantumfs.ObjectType
		var owner quantumfs.UID
		var inode InodeId
		err := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()

			record := dir.children.recordByName(c, name)
			if record == nil {
				return fuse.ENOENT
			}

			recordType = record.Type()
			owner = record.Owner()
			inode = dir.children.inodeNum(record.Filename())
			return fuse.OK
		}()
		if err != fuse.OK {
			return err
		}

		type_ := objectTypeToFileType(c, recordType)
		fileOwner := quantumfs.SystemUid(owner, c.fuseCtx.Owner.Uid)

		if type_ == fuse.S_IFDIR {
			c.vlog("Directory::Unlink directory")
			return fuse.Status(syscall.EISDIR)
		}

		err = dir.hasWritePermission(c, fileOwner, true)
		if err != fuse.OK {
			return err
		}

		dir.delChild_(c, inode)
		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) Rmdir(c *ctx, name string) fuse.Status {
	defer c.FuncIn("Directory::Rmdir", "%s", name).out()

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		var inode InodeId
		result := func() fuse.Status {
			defer dir.childRecordLock.Lock().Unlock()
			record := dir.children.recordByName(c, name)
			type_ := objectTypeToFileType(c, record.Type())
			if type_ != fuse.S_IFDIR {
				return fuse.ENOTDIR
			}

			if record.Size() != 0 {
				return fuse.Status(syscall.ENOTEMPTY)
			}

			inode = dir.children.inodeNum(name)
			return fuse.OK
		}()
		if result != fuse.OK {
			return result
		}

		dir.delChild_(c, inode)
		return fuse.OK
	}()

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

		result := dir.hasWritePermission(c, c.fuseCtx.Owner.Uid, false)
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

		err := dir.hasWritePermission(c, c.fuseCtx.Owner.Uid, false)
		if err != fuse.OK {
			return err
		}

		c.dlog("Directory::Mknod Mode %x", input.Mode)
		if BitFlagsSet(uint(input.Mode), syscall.S_IFIFO) ||
			BitFlagsSet(uint(input.Mode), syscall.S_IFSOCK) ||
			BitFlagsSet(uint(input.Mode), syscall.S_IFBLK) ||
			BitFlagsSet(uint(input.Mode), syscall.S_IFCHR) {

			dir.create_(c, name, input.Mode, input.Umask, input.Rdev,
				newSpecial, quantumfs.ObjectTypeSpecial,
				quantumfs.ZeroKey, out)
		} else if BitFlagsSet(uint(input.Mode), syscall.S_IFREG) {
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

			dir.self.markAccessed(c, oldName, false)

			if oldName == newName {
				// Nothing more to be done other than marking the
				// file accessed above.
				return quantumfs.InodeIdInvalid,
					quantumfs.InodeIdInvalid, fuse.OK
			}

			oldInodeId_ := dir.children.inodeNum(oldName)
			oldRemoved_ := dir.children.renameChild(oldName, newName)
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
		dir.self.dirty(c)

		return fuse.OK
	}()

	return result
}

func sortParentChild(c *ctx, a *Directory, b *Directory) (parentDir *Directory,
	childDir *Directory) {

	// Determine if a parent-child relationship between the
	// directories exist
	var parent *Directory
	var child *Directory

	upwardsParent := a.parent(c)
	for ; upwardsParent != nil; upwardsParent = upwardsParent.parent(c) {
		if upwardsParent.inodeNum() == b.inodeNum() {

			// a is a (grand-)child of b
			parent = b
			child = a
			break
		}
	}

	if upwardsParent == nil {
		upwardsParent = b.parent(c)
		for ; upwardsParent != nil; upwardsParent = upwardsParent.parent(c) {

			if upwardsParent.inodeNum() == a.inodeNum() {

				// b is a (grand-)child of a
				parent = a
				child = b
				break
			}
		}
	}

	if upwardsParent == nil {
		// No relationship, choose arbitrarily
		parent = a
		child = b
	}

	return parent, child
}

func (dir *Directory) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	// moving any file into _null/null is not permitted
	if _, ok := dstInode.(*NullWorkspaceRoot); ok {
		return fuse.EPERM
	}

	defer c.FuncIn("Directory::MvChild", "Enter %s -> %s", oldName,
		newName).out()

	result := func() fuse.Status {
		dst := dstInode.(*Directory)

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
		firstLock.(*Directory).Lock()
		lastLock.(*Directory).Lock()

		defer child.lock.Unlock()

		result := func() fuse.Status {
			// we need to unlock the parent early
			defer parent.lock.Unlock()

			newEntry, oldInodeId, err := func() (DirectoryRecordIf,
				InodeId, fuse.Status) {

				defer dir.childRecordLock.Lock().Unlock()

				record := dir.children.recordByName(c, oldName)
				oldInodeId_ := dir.children.inodeNum(oldName)
				if record == nil {
					return nil, 0, fuse.ENOENT
				}

				// copy the record
				newEntry_ := cloneDirectoryRecord(record)
				return newEntry_, oldInodeId_, fuse.OK
			}()
			if err != fuse.OK {
				return err
			}

			// fix the name on the copy
			newEntry.SetFilename(newName)

			// Update the inode to point to the new name and mark as
			// accessed in both parents.
			child := c.qfs.inodeNoInstantiate(c, oldInodeId)
			if child != nil {
				child.setParent(dst.inodeNum())
				child.setName(newName)
			}
			dir.self.markAccessed(c, oldName, false)
			dst.self.markAccessed(c, newName, true)

			// Delete the target InodeId, before (possibly) overwriting
			// it.
			dst.deleteEntry_(c, newName)

			func() {
				defer dir.childRecordLock.Lock().Unlock()

				overwrittenRecord := dst.children.recordByName(c,
					newName)
				overwrittenId := dst.children.inodeNum(newName)
				if overwrittenRecord != nil {
					c.qfs.removeUninstantiated(c,
						[]InodeId{overwrittenId})
				}

				dst.insertEntry_(c, newEntry, oldInodeId,
					child != nil)
			}()

			// Set entry in new directory. If the renamed inode is
			// uninstantiated, we swizzle the parent here.
			if child == nil {
				c.qfs.addUninstantiated(c, []InodeId{oldInodeId},
					dst.inodeNum())
			}

			// Remove entry in old directory
			dir.deleteEntry_(c, oldName)

			parent.updateSize_(c)
			parent.self.dirty(c)

			return fuse.OK
		}()

		if result == fuse.OK {
			child.updateSize_(c)
			child.self.dirty(c)
		}
		return result
	}()

	return result
}

// Must hold childmap lock for writing
func (dir *Directory) deleteEntry_(c *ctx, name string) {
	if record := dir.children.recordByName(c, name); record == nil {
		// Nothing to do
		return
	}

	inodeId := dir.children.inodeNum(name)
	dir.children.deleteChild(inodeId)
}

// Needs to hold childRecordLock
func (dir *Directory) insertEntry_(c *ctx, entry DirectoryRecordIf, inodeNum InodeId,
	inodeLoaded bool) {

	dir.children.setChild(c, entry, inodeNum)

	// being inserted means you're dirty and need to be synced
	if inodeLoaded {
		dir.children.setDirty(c, inodeNum)
	} else {
		dir.self.dirty(c)
	}
}

func (dir *Directory) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	return dir.parent(c).getChildXAttrSize(c, dir.inodeNum(), attr)
}

func (dir *Directory) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	return dir.parent(c).getChildXAttrData(c, dir.inodeNum(), attr)
}

func (dir *Directory) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	return dir.parent(c).listChildXAttr(c, dir.inodeNum())
}

func (dir *Directory) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	return dir.parent(c).setChildXAttr(c, dir.inodeNum(), attr, data)
}

func (dir *Directory) RemoveXAttr(c *ctx, attr string) fuse.Status {
	return dir.parent(c).removeChildXAttr(c, dir.inodeNum(), attr)
}

func (dir *Directory) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	defer c.FuncIn("Directory::syncChild", "(%d %d) %s", dir.inodeNum(),
		inodeNum, newKey.String()).out()

	ok, key := func() (bool, quantumfs.ObjectKey) {
		defer dir.Lock().Unlock()
		dir.self.dirty(c)
		defer dir.childRecordLock.Lock().Unlock()

		entry := dir.children.record(inodeNum)
		if entry == nil {
			c.elog("Directory::syncChild inode %d not a valid child",
				inodeNum)
			return false, quantumfs.ObjectKey{}
		}

		entry.SetID(newKey)
		return true, dir.publish_(c)
	}()

	if ok && !dir.self.isWorkspaceRoot() {
		dir.parent(c).syncChild(c, dir.InodeCommon.id, key)
	}
}

// Get the extended attributes object. The status is EIO on error or ENOENT if there
// are no extended attributes for that child.
func (dir *Directory) getExtendedAttributes_(c *ctx,
	inodeNum InodeId) (*quantumfs.ExtendedAttributes, fuse.Status) {

	defer c.funcIn("Directory::getExtendedAttributes_").out()
	defer dir.childRecordLock.Lock().Unlock()

	record := dir.children.record(inodeNum)
	if record == nil {
		c.vlog("Child not found")
		return nil, fuse.EIO
	}

	if record.ExtendedAttributes().IsEqualTo(quantumfs.EmptyBlockKey) {
		c.vlog("Directory::getExtendedAttributes_ returning new object")
		return quantumfs.NewExtendedAttributes(), fuse.ENOENT
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
	// even when the attributeList is empty here, we must include
	// XAttrTypeKey in the attributeList

	if ok == fuse.EIO {
		return nil, fuse.EIO
	}

	var nameBuffer bytes.Buffer
	for i := 0; i < attributeList.NumAttributes(); i++ {
		name, _ := attributeList.Attribute(i)
		c.vlog("Appending %s", name)
		nameBuffer.WriteString(name)
		nameBuffer.WriteByte(0)
	}

	// append our self-defined extended attribute XAttrTypeKey
	c.vlog("Appending %s", quantumfs.XAttrTypeKey)
	nameBuffer.WriteString(quantumfs.XAttrTypeKey)
	nameBuffer.WriteByte(0)

	c.vlog("Returning %d bytes", nameBuffer.Len())

	return nameBuffer.Bytes(), fuse.OK
}

func (dir *Directory) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	defer c.FuncIn("Directory::setChildXAttr", "%d, %s len %d", inodeNum, attr,
		len(data)).out()
	// The self-defined extended attribute is not able to be set
	// it is the combination of two attributes
	if attr == quantumfs.XAttrTypeKey {
		c.elog("Illegal action to set extended attribute: typeKey")
		return fuse.EPERM
	}

	defer c.vlog("Directory::setChildXAttr Exit")

	defer dir.Lock().Unlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)
	if ok == fuse.EIO {
		return fuse.EIO
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
		dir.children.record(inodeNum).SetExtendedAttributes(key)
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	defer c.FuncIn("Directory::removeChildXAttr", "%d, %s", inodeNum, attr).out()
	// The self-defined extended attribute is not able to be removed
	// it is the combination of two attributes
	if attr == quantumfs.XAttrTypeKey {
		c.elog("Illegal action to remove extended attribute: typeKey")
		return fuse.EPERM
	}

	defer c.vlog("Directory::removeChildXAttr Exit")

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
		dir.children.record(inodeNum).SetExtendedAttributes(key)
	}()
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.vlog("Directory::instantiateChild Enter %d", inodeNum)
	defer c.vlog("Directory::instantiateChild Exit")

	defer dir.childRecordLock.Lock().Unlock()

	entry := dir.children.record(inodeNum)
	if entry == nil {
		panic(fmt.Sprintf("Cannot instantiate child with no record: %d",
			inodeNum))
	}

	c.vlog("Instantiate %s %d", entry.Filename(), inodeNum)

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

func encodeExtendedKey(key quantumfs.ObjectKey, type_ quantumfs.ObjectType,
	size uint64) []byte {

	append_ := make([]byte, 9)
	append_[0] = uint8(type_)
	binary.LittleEndian.PutUint64(append_[1:], size)

	data := append(key.Value(), append_...)
	return []byte(base64.StdEncoding.EncodeToString(data))
}

func decodeExtendedKey(packet string) (quantumfs.ObjectKey, quantumfs.ObjectType,
	uint64, error) {

	bDec, err := base64.StdEncoding.DecodeString(packet)
	if err != nil {
		return quantumfs.ZeroKey, 0, 0, err
	}

	key := quantumfs.NewObjectKeyFromBytes(bDec[:sourceDataLength-9])
	type_ := quantumfs.ObjectType(bDec[sourceDataLength-9])
	size := binary.LittleEndian.Uint64(bDec[sourceDataLength-8:])
	return key, type_, size, nil
}

// Do a similar work like  Lookup(), but it does not interact with fuse, and return
// the child node to the caller
func (dir *Directory) lookupInternal(c *ctx, name string,
	entryType quantumfs.ObjectType) (Inode, error) {

	c.vlog("Directory::LookupInternal Enter")
	defer c.vlog("Directory::LookupInternal Exit")

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
	DirectoryRecordIf, error) {

	c.vlog("Directory::LookupChildRecord_ Enter")
	defer c.vlog("Directory::LookupChildRecord_ Exit")

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
	key quantumfs.ObjectKey) DirectoryRecordIf {

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

	entry := dir.createNewEntry(c, name, mode, umask, rdev, size,
		uid, gid, type_, key)

	defer dir.childRecordLock.Lock().Unlock()
	inodeNum := dir.children.newChild(c, entry)

	c.qfs.addUninstantiated(c, []InodeId{inodeNum}, dir.inodeNum())
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

	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  src.inodeNum(),
			treeLock_: src.treeLock(),
		},
		src: src,
	}

	assert(ds.treeLock() != nil, "directorySnapshot treeLock nil at init")

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children []directoryContents
	src      directorySnapshotSource
}

func (ds *directorySnapshot) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	defer c.funcIn("Directory::ReadDirPlus").out()
	offset := input.Offset

	if offset == 0 {
		c.dlog("Refreshing child list")
		ds.children = ds.src.getChildSnapshot(c)
	}

	// Add .
	if offset == 0 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = uint64(ds.FileHandleCommon.inodeNum)
		fillEntryOutCacheData(c, details)
		fillRootAttr(c, &details.Attr, ds.FileHandleCommon.inodeNum)
	}
	offset++

	// Add ..
	if offset == 1 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = uint64(ds.FileHandleCommon.inodeNum)
		fillEntryOutCacheData(c, details)
		fillRootAttr(c, &details.Attr, ds.FileHandleCommon.inodeNum)
	}
	offset++

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

	c.elog("Invalid write on directorySnapshot")
	return 0, fuse.ENOSYS
}

func (ds *directorySnapshot) Sync(c *ctx) fuse.Status {
	return fuse.OK
}
