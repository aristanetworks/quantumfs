// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "errors"
import "syscall"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// If dirRecord is nil, then mode, rdev and dirRecord are invalid, but the key is
// coming from a DirRecord and not passed in from create_.
type InodeConstructor func(c *ctx, key quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord *quantumfs.DirectoryRecord) Inode

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon

	// These fields are protected by the InodeCommon.lock
	baseLayerId quantumfs.ObjectKey
	children    map[string]InodeId

	// Indexed by inode number
	childrenRecords map[InodeId]*quantumfs.DirectoryRecord

	dirtyChildren_ map[InodeId]Inode // set of children which are currently dirty
}

func initDirectory(c *ctx, dir *Directory, baseLayerId quantumfs.ObjectKey,
	inodeNum InodeId, parent Inode, treeLock *sync.RWMutex) {

	c.vlog("initDirectory Enter Fetching directory baselayer from %s",
		baseLayerId)
	defer c.vlog("initDirectory Exit")

	// Set directory data before processing the children incase the children
	// access the parent.
	dir.InodeCommon = InodeCommon{id: inodeNum, self: dir}
	dir.setParent(parent)
	dir.treeLock_ = treeLock
	dir.dirtyChildren_ = make(map[InodeId]Inode, 0)
	dir.baseLayerId = baseLayerId

	key := baseLayerId
	for {
		c.vlog("Fetching baselayer %v", key)
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic("No baseLayer object")
		}

		baseLayer := buffer.AsDirectoryEntry()

		if dir.children == nil {
			dir.children = make(map[string]InodeId,
				baseLayer.NumEntries())
			dir.childrenRecords = make(
				map[InodeId]*quantumfs.DirectoryRecord,
				baseLayer.NumEntries())
		}

		for i := 0; i < baseLayer.NumEntries(); i++ {
			dir.loadChild(c, baseLayer.Entry(i))
		}

		if baseLayer.Next() == quantumfs.EmptyDirKey ||
			baseLayer.NumEntries() == 0 {

			break
		} else {
			key = baseLayer.Next()
		}
	}

	assert(dir.treeLock() != nil, "Directory treeLock nil at init")
}

func (dir *Directory) loadChild(c *ctx, entry quantumfs.DirectoryRecord) {
	inodeId := c.qfs.newInodeId()
	dir.children[entry.Filename()] = inodeId
	dir.childrenRecords[inodeId] = &entry
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

	c.qfs.setInode(c, inodeId, constructor(c, entry.ID(), entry.Size(),
		inodeId, dir, 0, 0, nil))
}

func newDirectory(c *ctx, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord *quantumfs.DirectoryRecord) Inode {

	c.vlog("Directory::newDirectory Enter")
	defer c.vlog("Directory::newDirectory Exit")

	var dir Directory

	initDirectory(c, &dir, baseLayerId, inodeNum, parent, parent.treeLock())

	return &dir
}

// Needs inode lock for read
func (dir *Directory) updateSize_(c *ctx) {
	// If we do not have a parent, then the parent is a workspacelist and we have
	// nothing to update.
	if dir.parent != nil {
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = uint64(len(dir.childrenRecords))
		dir.parent.setChildAttr(c, dir.id, nil, &attr, nil)
	}
}

// Needs inode lock for write
func (dir *Directory) addChild_(c *ctx, name string, inodeNum InodeId,
	child *quantumfs.DirectoryRecord) {

	dir.children[name] = inodeNum
	dir.childrenRecords[inodeNum] = child
	dir.updateSize_(c)
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx, name string) {
	inodeNum := dir.children[name]
	delete(dir.childrenRecords, inodeNum)
	delete(dir.dirtyChildren_, inodeNum)
	delete(dir.children, name)
	dir.updateSize_(c)
}

func (dir *Directory) dirty(c *ctx) {
	dir.setDirty(true)
	dir.parent.dirtyChild(c, dir)
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, child Inode) {
	func() {
		defer dir.Lock().Unlock()
		dir.dirtyChildren_[child.inodeNum()] = child
	}()
	dir.self.dirty(c)
}

func fillAttrWithDirectoryRecord(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	owner fuse.Owner, entry *quantumfs.DirectoryRecord) {

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
		attr.Nlink = 1
	}

	attr.Atime = entry.ModificationTime().Seconds()
	attr.Mtime = entry.ModificationTime().Seconds()
	attr.Ctime = entry.CreationTime().Seconds()
	attr.Atimensec = entry.ModificationTime().Nanoseconds()
	attr.Mtimensec = entry.ModificationTime().Nanoseconds()
	attr.Ctimensec = entry.CreationTime().Nanoseconds()

	var permissions uint32
	permissions |= uint32(entry.Permissions())
	permissions |= uint32(entry.Permissions()) << 3
	permissions |= uint32(entry.Permissions()) << 6
	permissions |= fileType
	c.dlog("fillAttrWithDirectoryRecord fileType %x permissions %d", fileType,
		entry.Permissions())

	attr.Mode = permissions
	attr.Owner.Uid = quantumfs.SystemUid(entry.Owner(), owner.Uid)
	attr.Owner.Gid = quantumfs.SystemGid(entry.Group(), owner.Gid)
	attr.Blksize = qfsBlockSize
}

func modeToPermissions(mode uint32, umask uint32) uint8 {
	var permissions uint32
	mode = mode & ^umask
	permissions = mode & 0x7
	permissions |= (mode >> 3) & 0x7
	permissions |= (mode >> 6) & 0x7

	return uint8(permissions)
}

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		entry, exists := dir.childrenRecords[inodeNum]
		if !exists {
			return fuse.ENOENT
		}

		// Update the type if needed
		if newType != nil {
			entry.SetType(*newType)
			c.vlog("Type now %d", *newType)
		}

		valid := uint(attr.SetAttrInCommon.Valid)
		// We don't support file locks yet, but when we do we need
		// FATTR_LOCKOWNER

		if BitFlagsSet(valid, fuse.FATTR_MODE) {
			entry.SetPermissions(modeToPermissions(attr.Mode, 0))
			c.vlog("Permissions now %d Mode %d", entry.Permissions(),
				attr.Mode)
		}

		if BitFlagsSet(valid, fuse.FATTR_UID) {
			entry.SetOwner(quantumfs.ObjectUid(c.Ctx, attr.Owner.Uid,
				c.fuseCtx.Owner.Uid))
			c.vlog("Owner now %d UID %d context %d", entry.Owner(),
				attr.Owner.Uid, c.fuseCtx.Owner.Uid)
		}

		if BitFlagsSet(valid, fuse.FATTR_GID) {
			entry.SetGroup(quantumfs.ObjectGid(c.Ctx, attr.Owner.Gid,
				c.fuseCtx.Owner.Gid))
			c.vlog("Group now %d GID %d context %d", entry.Group(),
				attr.Owner.Gid, c.fuseCtx.Owner.Gid)
		}

		if BitFlagsSet(valid, fuse.FATTR_SIZE) {
			entry.SetSize(attr.Size)
			c.vlog("Size now %d", entry.Size())
		}

		if BitFlagsSet(valid, fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW) {
			// atime is ignored and not stored
		}

		if BitFlagsSet(valid, fuse.FATTR_MTIME) {
			entry.SetModificationTime(
				quantumfs.NewTimeSeconds(attr.Mtime, attr.Mtimensec))
			c.vlog("ModificationTime now %d", entry.ModificationTime())
		}

		if BitFlagsSet(valid, fuse.FATTR_MTIME_NOW) {
			entry.SetModificationTime(quantumfs.NewTime(time.Now()))
			c.vlog("ModificationTime now %d", entry.ModificationTime())
		}

		if BitFlagsSet(valid, fuse.FATTR_CTIME) {
			entry.SetCreationTime(quantumfs.NewTimeSeconds(attr.Ctime,
				attr.Ctimensec))
			c.vlog("CreationTime now %d", entry.CreationTime())
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

	c.elog("Unsupported Access on Directory")
	return fuse.ENOSYS
}

func (dir *Directory) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer dir.RLock().RUnlock()

	var childDirectories uint32
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	for _, entry := range dir.childrenRecords {
		if entry.Type() == quantumfs.ObjectTypeDirectoryEntry {
			childDirectories++
		}
	}
	fillAttr(&out.Attr, dir.InodeCommon.id, childDirectories)
	out.Attr.Mode = 0777 | fuse.S_IFDIR
	return fuse.OK
}

func (dir *Directory) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.vlog("Directory::Lookup Enter")
	defer c.vlog("Directory::Lookup Exit")
	defer dir.RLock().RUnlock()

	inodeNum, exists := dir.children[name]
	if !exists {
		return fuse.ENOENT
	}

	c.vlog("Directory::Lookup found inode %d", inodeNum)
	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		dir.childrenRecords[inodeNum])

	return fuse.OK
}

func (dir *Directory) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOSYS
}

func (dir *Directory) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer dir.RLock().RUnlock()

	children := make([]directoryContents, 0, len(dir.childrenRecords))
	for _, entry := range dir.childrenRecords {
		filename := entry.Filename()

		entryInfo := directoryContents{
			filename: filename,
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr,
			dir.children[filename], c.fuseCtx.Owner, entry)
		entryInfo.fuseType = entryInfo.attr.Mode

		children = append(children, entryInfo)
	}

	ds := newDirectorySnapshot(c, children, dir.InodeCommon.id, dir.treeLock())
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (dir *Directory) create_(c *ctx, name string, mode uint32, umask uint32,
	rdev uint32, constructor InodeConstructor, type_ quantumfs.ObjectType,
	key quantumfs.ObjectKey, out *fuse.EntryOut) Inode {

	c.vlog("Directory::create_ Enter")
	defer c.vlog("Directory::create_ Exit")

	now := time.Now()
	uid := c.fuseCtx.Owner.Uid
	gid := c.fuseCtx.Owner.Gid

	entry := quantumfs.NewDirectoryRecord()
	entry.SetFilename(name)
	entry.SetID(key)
	entry.SetType(type_)
	entry.SetPermissions(modeToPermissions(mode, umask))
	c.dlog("Directory::create_ mode %x umask %d permissions %d", mode, umask,
		entry.Permissions())
	entry.SetOwner(quantumfs.ObjectUid(c.Ctx, uid, uid))
	entry.SetGroup(quantumfs.ObjectGid(c.Ctx, gid, gid))
	entry.SetSize(0)
	entry.SetExtendedAttributes(quantumfs.EmptyBlockKey)
	entry.SetCreationTime(quantumfs.NewTime(now))
	entry.SetModificationTime(quantumfs.NewTime(now))

	inodeNum := c.qfs.newInodeId()
	newEntity := constructor(c, key, 0, inodeNum, dir.self, mode, rdev, entry)
	dir.addChild_(c, name, inodeNum, entry)
	c.qfs.setInode(c, inodeNum, newEntity)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner, entry)

	return newEntity
}

func (dir *Directory) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	var file Inode
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
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

	c.vlog("Directory::SetAttr Enter valid %x size %d", attr.Valid, attr.Size)
	defer c.vlog("Directory::SetAttr Exit")

	return dir.parent.setChildAttr(c, dir.InodeCommon.id, nil, attr, out)
}

func (dir *Directory) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
		}

		dir.create_(c, name, input.Mode, input.Umask, 0, newDirectory,
			quantumfs.ObjectTypeDirectoryEntry, quantumfs.EmptyDirKey,
			out)
		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) getChildRecord(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	defer dir.RLock().RUnlock()

	if val, ok := dir.childrenRecords[inodeNum]; ok {
		return *val, nil
	}

	return quantumfs.DirectoryRecord{},
		errors.New("Inode given is not a child of this directory")
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {
	c.vlog("Directory::Unlink Enter %s", name)
	defer c.vlog("Directory::Unlink Exit")
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.children[name]; !exists {
			return fuse.ENOENT
		}

		inode := dir.children[name]
		type_ := objectTypeToFileType(c, dir.childrenRecords[inode].Type())
		if type_ == fuse.S_IFDIR {
			return fuse.Status(syscall.EISDIR)
		}

		dir.delChild_(c, name)
		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) Rmdir(c *ctx, name string) fuse.Status {
	c.vlog("Directory::Rmdir Enter %s", name)
	defer c.vlog("Directory::Rmdir Exit")

	result := func() fuse.Status {
		defer dir.Lock().Unlock()
		if _, exists := dir.children[name]; !exists {
			return fuse.ENOENT
		}

		inode := dir.children[name]
		type_ := objectTypeToFileType(c, dir.childrenRecords[inode].Type())
		if type_ != fuse.S_IFDIR {
			return fuse.ENOTDIR
		}

		if dir.childrenRecords[inode].Size() != 0 {
			return fuse.Status(syscall.ENOTEMPTY)
		}

		dir.delChild_(c, name)
		return fuse.OK
	}()

	if result == fuse.OK {
		dir.self.dirty(c)
	}

	return result
}

func (dir *Directory) Symlink(c *ctx, pointedTo string, name string,
	out *fuse.EntryOut) fuse.Status {

	var key quantumfs.ObjectKey
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
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
		c.vlog("Created new symlink with key: %s", key)
	}

	return result
}

func (dir *Directory) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on Directory")
	return nil, fuse.EINVAL
}

func (dir *Directory) Sync(c *ctx) fuse.Status {
	return fuse.OK
}

func (dir *Directory) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Directory::Mknod Enter")
	defer c.vlog("Directory::Mknod Exit")

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
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

	c.vlog("Directory::RenameChild Enter %s -> %s", oldName, newName)
	defer c.vlog("Directory::RenameChild Exit")

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.children[oldName]; !exists {
			return fuse.ENOENT
		}

		oldInodeId := dir.children[oldName]
		newInodeId := dir.children[newName]

		dir.childrenRecords[oldInodeId].SetFilename(newName)

		dir.children[newName] = oldInodeId
		delete(dir.children, oldName)
		delete(dir.childrenRecords, newInodeId)
		delete(dir.dirtyChildren_, newInodeId)

		dir.updateSize_(c)
		dir.self.dirty(c)

		return fuse.OK
	}()

	return result
}

func (dir *Directory) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.vlog("Directory::MvChild Enter %s -> %s", oldName, newName)
	defer c.vlog("Directory::MvChild Exit")

	result := func() fuse.Status {
		dst := dstInode.(*Directory)

		// Lock both directories in inode number order to prevent a lock
		// ordering deadlock against a rename in the opposite direction.
		if dir.inodeNum() > dst.inodeNum() {
			defer dir.Lock().Unlock()
			defer dst.Lock().Unlock()
		} else {
			defer dst.Lock().Unlock()
			defer dir.Lock().Unlock()
		}

		if _, exists := dir.children[oldName]; !exists {
			return fuse.ENOENT
		}

		oldInodeId := dir.children[oldName]
		newInodeId := dst.children[newName]

		oldEntry := dir.childrenRecords[oldInodeId]

		child := c.qfs.inode(c, oldInodeId)
		child.setParent(dst)

		newEntry := quantumfs.NewDirectoryRecord()
		newEntry.SetFilename(newName)
		newEntry.SetID(oldEntry.ID())
		newEntry.SetType(oldEntry.Type())
		newEntry.SetPermissions(oldEntry.Permissions())
		newEntry.SetOwner(oldEntry.Owner())
		newEntry.SetGroup(oldEntry.Group())
		newEntry.SetSize(oldEntry.Size())
		newEntry.SetExtendedAttributes(oldEntry.ExtendedAttributes())
		newEntry.SetCreationTime(oldEntry.CreationTime())
		newEntry.SetModificationTime(oldEntry.ModificationTime())

		// Update entry in new directory
		dst.children[newName] = oldInodeId
		delete(dst.childrenRecords, newInodeId)
		delete(dst.dirtyChildren_, newInodeId)
		dst.childrenRecords[oldInodeId] = newEntry
		if _, exists := dir.dirtyChildren_[oldInodeId]; exists {
			dst.dirtyChildren_[oldInodeId] =
				dir.dirtyChildren_[oldInodeId]
		}

		// Remove entry in old directory
		delete(dir.children, oldName)
		delete(dir.childrenRecords, oldInodeId)
		delete(dir.dirtyChildren_, oldInodeId)

		dir.updateSize_(c)
		dir.self.dirty(c)
		dst.updateSize_(c)
		dst.self.dirty(c)

		return fuse.OK
	}()

	return result
}

func (dir *Directory) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("Directory::syncChild Enter")
	defer c.vlog("Directory::syncChild Exit")

	ok, key := func() (bool, quantumfs.ObjectKey) {
		defer dir.Lock().Unlock()
		dir.self.dirty(c)

		entry, exists := dir.childrenRecords[inodeNum]
		if !exists {
			c.elog("Directory::syncChild inode %d not a valid child",
				inodeNum)
			return false, quantumfs.ObjectKey{}
		}

		entry.SetID(newKey)
		return true, dir.publish(c)
	}()

	if ok && dir.parent != nil {
		dir.parent.syncChild(c, dir.InodeCommon.id, key)
	}
}

type directoryContents struct {
	// All immutable after creation
	filename string
	fuseType uint32 // One of fuse.S_IFDIR, S_IFREG, etc
	attr     fuse.Attr
}

func newDirectorySnapshot(c *ctx, children []directoryContents,
	inodeNum InodeId, treeLock *sync.RWMutex) *directorySnapshot {

	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  inodeNum,
			treeLock_: treeLock,
		},
		children: children,
	}

	assert(ds.treeLock() != nil, "directorySnapshot treeLock nil at init")

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children []directoryContents
}

func (ds *directorySnapshot) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.vlog("ReadDirPlus directorySnapshot in: %v out: %v", input, out)
	offset := input.Offset

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
