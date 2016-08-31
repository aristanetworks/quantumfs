// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bytes"
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
	dirChildren childRecords
}

func initDirectory(c *ctx, dir *Directory, baseLayerId quantumfs.ObjectKey,
	inodeNum InodeId, parent Inode, treeLock *sync.RWMutex) {

	c.vlog("initDirectory Enter Fetching directory baselayer from %s",
		baseLayerId.String())
	defer c.vlog("initDirectory Exit")

	// Set directory data before processing the children incase the children
	// access the parent.
	dir.InodeCommon = InodeCommon{id: inodeNum, self: dir}
	dir.setParent(parent)
	dir.treeLock_ = treeLock
	dir.baseLayerId = baseLayerId
	dir.dirChildren = newChildRecords(c, dir)

	key := baseLayerId
	for {
		c.vlog("Fetching baselayer %s", key.String())
		buffer := c.dataStore.Get(&c.Ctx, key)
		if buffer == nil {
			panic("No baseLayer object")
		}

		baseLayer := buffer.AsDirectoryEntry()

		for i := 0; i < baseLayer.NumEntries(); i++ {
			newEntry := baseLayer.Entry(i)
			dir.dirChildren.setRecord(&newEntry)
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
	if dir.parent() != nil {
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = uint64(dir.dirChildren.count())
		dir.parent().setChildAttr(c, dir.id, nil, &attr, nil)
	}
}

// Needs inode lock for write
func (dir *Directory) addChild_(c *ctx, inode InodeId,
	child *quantumfs.DirectoryRecord) {

	dir.dirChildren.setLoadedRecord(inode, child)
	dir.updateSize_(c)
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx, name string) {
	inodeNum, exists := dir.dirChildren.getInode(name)
	if !exists {
		panic("Unexpected missing child inode")
	}
	c.dlog("Unlinking inode %d", inodeNum)

	// If this is a file we need to reparent it to itself
	record, exists := dir.dirChildren.getRecord(inodeNum)
	if !exists {
		panic("Unexpected missing child inode")
	}
	if record.Type() == quantumfs.ObjectTypeSmallFile ||
		record.Type() == quantumfs.ObjectTypeMediumFile ||
		record.Type() == quantumfs.ObjectTypeLargeFile ||
		record.Type() == quantumfs.ObjectTypeVeryLargeFile {

		if inode := c.qfs.inode(c, inodeNum); inode != nil {
			if file, isFile := inode.(*File); isFile {
				file.setChildRecord(c, record)
				file.setParent(file)
			}
		}
	}

	dir.dirChildren.delete(name)
	dir.updateSize_(c)
}

func (dir *Directory) dirty(c *ctx) {
	dir.setDirty(true)
	dir.parent().dirtyChild(c, dir)
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, child Inode) {
	func() {
		defer dir.Lock().Unlock()
		dir.dirChildren.dirty(child.inodeNum())
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
		attr.Nlink = 2 // Workaround for BUG166665
	}

	attr.Atime = entry.ModificationTime().Seconds()
	attr.Mtime = entry.ModificationTime().Seconds()
	attr.Ctime = entry.CreationTime().Seconds()
	attr.Atimensec = entry.ModificationTime().Nanoseconds()
	attr.Mtimensec = entry.ModificationTime().Nanoseconds()
	attr.Ctimensec = entry.CreationTime().Nanoseconds()

	c.dlog("fillAttrWithDirectoryRecord fileType %x permissions %d", fileType,
		entry.Permissions())

	attr.Mode = fileType | permissionsToMode(entry.Permissions())
	attr.Owner.Uid = quantumfs.SystemUid(entry.Owner(), owner.Uid)
	attr.Owner.Gid = quantumfs.SystemGid(entry.Group(), owner.Gid)
	attr.Blksize = qfsBlockSize
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

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		entry, exists := dir.dirChildren.getRecord(inodeNum)
		if !exists {
			return fuse.ENOENT
		}

		modifyEntryWithAttr(c, newType, attr, entry)

		if out != nil {
			fillAttrOutCacheData(c, out)
			fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
				c.fuseCtx.Owner, entry)
		}

		dir.dirChildren.setRecord(entry)

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

	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	fillAttr(&out.Attr, dir.InodeCommon.id,
		uint32(dir.dirChildren.countChildDirs()))
	out.Attr.Mode = 0777 | fuse.S_IFDIR
	return fuse.OK
}

func (dir *Directory) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.vlog("Directory::Lookup Enter")
	defer c.vlog("Directory::Lookup Exit")
	defer dir.RLock().RUnlock()

	inodeNum, exists := dir.dirChildren.getInode(name)
	if !exists {
		return fuse.ENOENT
	}

	record, exists := dir.dirChildren.getRecord(inodeNum)
	if !exists {
		return fuse.ENOENT
	}

	c.vlog("Directory::Lookup found inode %d", inodeNum)
	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner, record)

	return fuse.OK
}

func (dir *Directory) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOSYS
}

func (dir *Directory) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	defer dir.RLock().RUnlock()

	children := make([]directoryContents, 0, dir.dirChildren.count())
	for _, entry := range dir.dirChildren.getRecords() {
		filename := entry.Filename()

		entryInfo := directoryContents{
			filename: filename,
		}
		inodeId, exists := dir.dirChildren.getInode(filename)
		if !exists {
			panic("Missing inode in child records")
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr, inodeId,
			c.fuseCtx.Owner, entry)
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
	dir.addChild_(c, inodeNum, entry)
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

		if _, exists := dir.dirChildren.getInode(name); exists {
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

	return dir.parent().setChildAttr(c, dir.InodeCommon.id, nil, attr, out)
}

func (dir *Directory) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.dirChildren.getInode(name); exists {
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

	if val, ok := dir.dirChildren.getRecord(inodeNum); ok {
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

		inode, exists := dir.dirChildren.getInode(name)
		if !exists {
			return fuse.ENOENT
		}

		record, exists := dir.dirChildren.getRecord(inode)
		if !exists {
			return fuse.ENOENT
		}

		type_ := objectTypeToFileType(c, record.Type())
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
		inode, exists := dir.dirChildren.getInode(name)
		if !exists {
			return fuse.ENOENT
		}

		record, exists := dir.dirChildren.getRecord(inode)
		if !exists {
			return fuse.ENOENT
		}

		type_ := objectTypeToFileType(c, record.Type())
		if type_ != fuse.S_IFDIR {
			return fuse.ENOTDIR
		}

		if record.Size() != 0 {
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

		if _, exists := dir.dirChildren.getInode(name); exists {
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
		c.vlog("Created new symlink with key: %s", key.String())
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

		if _, exists := dir.dirChildren.getInode(name); exists {
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

		if _, exists := dir.dirChildren.getInode(oldName); !exists {
			return fuse.ENOENT
		}

		dir.dirChildren.rename(oldName, newName)

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

		// The locking here is subtle.
		//
		// Firstly we must protect against the case where a concurrent rename
		// in the opposite direction (from dst into dir) is occurring as we
		// are renaming a file from dir into dst. If we lock naively we'll
		// end up with a lock ordering inversion and deadlock in this case.
		//
		// We prevent this by locking dir and dst in a consistent ordering
		// based upon their inode number.
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

		// Determine if a parent-child relationship between the
		// directories exist
		var parent *Directory
		var child *Directory
		if dst.parent() != nil &&
			dst.parent().inodeNum() == dir.inodeNum() {

			// dst is a child of dir
			parent = dir
			child = dst
		} else if dir.parent() != nil &&
			dir.parent().inodeNum() == dst.inodeNum() {

			// dir is a child of dst
			parent = dst
			child = dir
		} else {
			// No relationship, choose arbitrarily
			parent = dst
			child = dir
		}

		dirLocked := false
		if dir.inodeNum() > dst.inodeNum() {
			dir.Lock()
			dirLocked = true
		}
		defer child.lock.Unlock()

		result := func() fuse.Status {
			dst.Lock()
			defer parent.lock.Unlock()

			if !dirLocked {
				dir.Lock()
			}

			if _, exists := dir.dirChildren.getInode(oldName); !exists {
				return fuse.ENOENT
			}

			oldInodeId, exists := dir.dirChildren.getInode(oldName)
			if !exists {
				return fuse.ENOENT
			}

			// copy the record
			oldEntry, exists := dir.dirChildren.getRecord(oldInodeId)
			if !exists {
				return fuse.ENOENT
			}
			newEntry := cloneDirectoryRecord(oldEntry)
			newEntry.SetFilename(newName)

			// update the inode
			child := c.qfs.inode(c, oldInodeId)
			child.setParent(dst)

			//delete the target InodeId
			dst.dirChildren.delete(newName)

			// set entry in new directory
			dst.dirChildren.insertRecord(oldInodeId, newEntry)

			// Remove entry in old directory
			dir.dirChildren.delete(oldName)

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

func (dir *Directory) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	if dir.parent() == nil {
		c.wlog("No parent in GetXAttrSize call %d", dir.inodeNum())
		return 0, fuse.EIO
	}

	return dir.parent().getChildXAttrSize(c, dir.inodeNum(), attr)
}

func (dir *Directory) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	if dir.parent() == nil {
		c.wlog("No parent in GetXAttrData call %d", dir.inodeNum())
		return nil, fuse.EIO
	}

	return dir.parent().getChildXAttrData(c, dir.inodeNum(), attr)
}

func (dir *Directory) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	if dir.parent() == nil {
		c.wlog("No parent in ListXAttr call %d", dir.inodeNum())
		return nil, fuse.EIO
	}

	return dir.parent().listChildXAttr(c, dir.inodeNum())
}

func (dir *Directory) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	if dir.parent() == nil {
		c.wlog("No parent in SetXAttr call %d", dir.inodeNum())
		return fuse.EIO
	}

	return dir.parent().setChildXAttr(c, dir.inodeNum(), attr, data)
}

func (dir *Directory) RemoveXAttr(c *ctx, attr string) fuse.Status {
	if dir.parent() == nil {
		c.wlog("No parent in RemoveXAttr call %d", dir.inodeNum())
		return fuse.EIO
	}

	return dir.parent().removeChildXAttr(c, dir.inodeNum(), attr)
}

func (dir *Directory) Link(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Directory::Link Enter")
	defer c.vlog("Directory::Link Exit")

	origRecord, err := srcInode.parent().getChildRecord(c, srcInode.inodeNum())
	if err != nil {
		c.elog("QuantumFs::Link Failed to get srcInode record %v:", err)
		return fuse.EIO
	}
	newRecord := cloneDirectoryRecord(&origRecord)
	newRecord.SetFilename(newName)
	newRecord.SetID(srcInode.sync_DOWN(c))

	// We cannot lock earlier because the parent of srcInode may be us
	defer dir.Lock().Unlock()

	dir.dirChildren.setRecord(newRecord)
	inodeNum, exists := dir.dirChildren.getInode(newRecord.Filename())
	if !exists {
		panic("Failure to set record in children")
	}

	c.dlog("CoW linked %d to %s as inode %d", srcInode.inodeNum(), newName,
		inodeNum)

	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner,
		newRecord)

	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("Directory::syncChild Enter")
	defer c.vlog("Directory::syncChild Exit")

	ok, key := func() (bool, quantumfs.ObjectKey) {
		defer dir.Lock().Unlock()
		dir.self.dirty(c)

		entry, exists := dir.dirChildren.getRecord(inodeNum)
		if !exists {
			c.elog("Directory::syncChild inode %d not a valid child",
				inodeNum)
			return false, quantumfs.ObjectKey{}
		}

		entry.SetID(newKey)
		return true, dir.publish(c)
	}()

	if ok && dir.parent() != nil {
		dir.parent().syncChild(c, dir.InodeCommon.id, key)
	}
}

// Get the extended attributes object. The status is EIO on error or ENOENT if there
// are no extended attributes for that child.
func (dir *Directory) getExtendedAttributes_(c *ctx,
	inodeNum InodeId) (*quantumfs.ExtendedAttributes, fuse.Status) {

	c.vlog("Directory::getExtendedAttributes_ Enter")

	record, ok := dir.dirChildren.getRecord(inodeNum)
	if !ok {
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

	c.vlog("Directory::getChildXAttrBuffer Enter %d %s", inodeNum, attr)
	defer c.vlog("Directory::getChildXAttrBuffer Exit")

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

	c.vlog("XAttr name not found: %s", attr)
	return nil, fuse.ENODATA
}

func (dir *Directory) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return 0, status
	}

	return buffer.Size(), fuse.OK
}

func (dir *Directory) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	buffer, status := dir.getChildXAttrBuffer(c, inodeNum, attr)
	if status != fuse.OK {
		return []byte{}, status
	}

	return buffer.Get(), fuse.OK
}

func (dir *Directory) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.vlog("Directory::listChildXAttr Enter %d", inodeNum)
	defer c.vlog("Directory::listChildXAttr Exit")

	defer dir.RLock().RUnlock()

	attributeList, ok := dir.getExtendedAttributes_(c, inodeNum)
	if ok == fuse.ENOENT {
		return []byte{}, fuse.OK
	}

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

	c.vlog("Returning %d bytes", nameBuffer.Len())

	return nameBuffer.Bytes(), fuse.OK
}

func (dir *Directory) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.vlog("Directory::setChildXAttr Enter %d, %s", inodeNum, attr)
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
			quantumfs.MaxNumExtendedAttributes {

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

	record, exists := dir.dirChildren.getRecord(inodeNum)
	if !exists {
		c.elog("Unable to fetch child record: %d", inodeNum)
		return fuse.EIO
	}
	record.SetExtendedAttributes(key)
	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.vlog("Directory::removeChildXAttr Enter %d, %s", inodeNum, attr)
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

	record, exists := dir.dirChildren.getRecord(inodeNum)
	if !exists {
		c.elog("Unable to fetch child record: %d", inodeNum)
		return fuse.EIO
	}
	record.SetExtendedAttributes(key)
	dir.self.dirty(c)

	return fuse.OK
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

	c.vlog("ReadDirPlus directorySnapshot")
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
