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
type InodeConstructor func(c *ctx, name string, key quantumfs.ObjectKey,
	size uint64, inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord *quantumfs.DirectoryRecord) Inode

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon

	// These fields are protected by the InodeCommon.lock
	baseLayerId quantumfs.ObjectKey
	dirChildren childRecords
}

func initDirectory(c *ctx, name string, dir *Directory,
	baseLayerId quantumfs.ObjectKey, inodeNum InodeId,
	parent Inode, treeLock *sync.RWMutex) {

	c.vlog("initDirectory Enter Fetching directory baselayer from %s",
		baseLayerId.String())
	defer c.vlog("initDirectory Exit")

	// Set directory data before processing the children incase the children
	// access the parent.
	dir.InodeCommon = InodeCommon{
		id:        inodeNum,
		name_:     name,
		accessed_: 0,
		self:      dir,
	}
	dir.setParent(parent)
	dir.treeLock_ = treeLock
	dir.baseLayerId = baseLayerId
	dir.dirChildren = newChildRecords(dir)

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
			dir.dirChildren.setRecord(c, &newEntry)
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

func newDirectory(c *ctx, name string, baseLayerId quantumfs.ObjectKey, size uint64,
	inodeNum InodeId, parent Inode, mode uint32, rdev uint32,
	dirRecord *quantumfs.DirectoryRecord) Inode {

	c.vlog("Directory::newDirectory Enter")
	defer c.vlog("Directory::newDirectory Exit")

	var dir Directory

	initDirectory(c, name, &dir, baseLayerId, inodeNum,
		parent, parent.treeLock())
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
		dir.parent().setChildAttr(c, dir.id, nil, &attr, nil, true)
	}
}

// Needs inode lock for write
func (dir *Directory) addChild_(c *ctx, inode InodeId,
	child *quantumfs.DirectoryRecord) {

	dir.dirChildren.setLoadedRecord(c, inode, child)
	dir.updateSize_(c)
}

// Needs inode lock for write
func (dir *Directory) delChild_(c *ctx, name string) {
	inodeNum, exists := dir.dirChildren.getInode(c, name)
	if !exists {
		panic("Unexpected missing child inode")
	}
	c.dlog("Unlinking inode %d", inodeNum)

	// If this is a file we need to reparent it to itself
	record, exists := dir.dirChildren.getRecord(c, inodeNum)
	if !exists {
		panic("Unexpected missing child inode")
	}

	dir.self.markAccessed(c, name, false)
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
	if !dir.setDirty(true) {
		// Only go recursive if we aren't already dirty
		dir.parent().dirtyChild(c, dir)
	}
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, child Inode) {
	func() {
		defer dir.Lock().Unlock()
		dir.dirChildren.dirty(c, child.inodeNum())
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
	attr.Ctime = entry.ContentTime().Seconds()
	attr.Atimensec = entry.ModificationTime().Nanoseconds()
	attr.Mtimensec = entry.ModificationTime().Nanoseconds()
	attr.Ctimensec = entry.ContentTime().Nanoseconds()

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

func (dir *Directory) publish(c *ctx) quantumfs.ObjectKey {
	c.vlog("Directory::publish Enter")
	defer c.vlog("Directory::publish Exit")

	// Compile the internal records into a series of blocks which can be placed
	// in the datastore.

	newBaseLayerId := quantumfs.EmptyDirKey

	// childIdx indexes into childrenRecords, entryIdx indexes into the
	// metadata block
	baseLayer := quantumfs.NewDirectoryEntry()
	entryIdx := 0
	for _, child := range dir.dirChildren.getRecords() {
		if entryIdx == quantumfs.MaxDirectoryRecords {
			// This block is full, upload and create a new one
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			baseLayer = quantumfs.NewDirectoryEntry()
			entryIdx = 0
		}

		baseLayer.SetEntry(entryIdx, child)

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId)

	c.vlog("Directory key %s -> %s", dir.baseLayerId.String(),
		newBaseLayerId.String())
	dir.baseLayerId = newBaseLayerId

	dir.setDirty(false)

	return dir.baseLayerId
}

func (dir *Directory) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		entry, exists := dir.dirChildren.getRecord(c, inodeNum)
		if !exists {
			return fuse.ENOENT
		}

		modifyEntryWithAttr(c, newType, attr, entry, updateMtime)

		if out != nil {
			fillAttrOutCacheData(c, out)
			fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
				c.fuseCtx.Owner, entry)
		}

		dir.dirChildren.setRecord(c, entry)

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
	c.vlog("Directory::GetAttr Enter")
	defer c.vlog("Directory::GetAttr Exit")

	record, err := dir.parent().getChildRecord(c, dir.InodeCommon.id)
	if err != nil {
		c.elog("Unable to get record from parent for inode %d", dir.id)
		return fuse.EIO
	}

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, dir.InodeCommon.id,
		c.fuseCtx.Owner, &record)

	return fuse.OK
}

func (dir *Directory) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	c.vlog("Directory::Lookup Enter")
	defer c.vlog("Directory::Lookup Exit")
	defer dir.RLock().RUnlock()

	inodeNum, exists := dir.dirChildren.getInode(c, name)
	if !exists {
		return fuse.ENOENT
	}

	record, exists := dir.dirChildren.getRecord(c, inodeNum)
	if !exists {
		return fuse.ENOENT
	}

	c.vlog("Directory::Lookup found inode %d", inodeNum)
	dir.self.markAccessed(c, name, false)

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

	ds := newDirectorySnapshot(c, dir.self.(directorySnapshotSource))
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (dir *Directory) getChildSnapshot(c *ctx) []directoryContents {
	defer dir.RLock().RUnlock()

	dir.self.markSelfAccessed(c, false)

	children := make([]directoryContents, 0, dir.dirChildren.count())
	for _, entry := range dir.dirChildren.getRecords() {
		filename := entry.Filename()

		entryInfo := directoryContents{
			filename: filename,
		}
		inodeId, exists := dir.dirChildren.getInode(c, filename)
		if !exists {
			panic("Missing inode in child records")
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr, inodeId,
			c.fuseCtx.Owner, entry)
		entryInfo.fuseType = entryInfo.attr.Mode

		children = append(children, entryInfo)
	}

	return children
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
	entry.SetContentTime(quantumfs.NewTime(now))
	entry.SetModificationTime(quantumfs.NewTime(now))

	inodeNum := c.qfs.newInodeId()
	newEntity := constructor(c, name, key, 0, inodeNum, dir.self,
		mode, rdev, entry)
	dir.addChild_(c, inodeNum, entry)
	c.qfs.setInode(c, inodeNum, newEntity)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner, entry)

	newEntity.markSelfAccessed(c, true)

	return newEntity
}

func (dir *Directory) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	var file Inode
	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.dirChildren.getInode(c, name); exists {
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

	return dir.parent().setChildAttr(c, dir.InodeCommon.id, nil, attr, out,
		false)
}

func (dir *Directory) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.dirChildren.getInode(c, name); exists {
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

	if val, ok := dir.dirChildren.getRecord(c, inodeNum); ok {
		return *val, nil
	}

	return quantumfs.DirectoryRecord{},
		errors.New("Inode given is not a child of this directory")
}

func (dir *Directory) getPermissions(c *ctx, permission uint32, uid uint32,
        gid uint32, fileOwner uint32, dirOwner uint32, dirGroup uint32) fuse.Status {

        stickyBit := permission & syscall.S_ISVTX
        if stickyBit != 0 {
                if uid != fileOwner {
                        // Check if it's the directory owner
                        if uid != dirOwner {
                                // No root permission, return false
                                c.vlog("Directory::GetPermissions fail with the" +
                                " sticky bit")
                                return fuse.EACCES
                        }
                }
        }

        // Get whether current user in OWNER/GRP/OTHER
        var permWX uint32
        if uid == dirOwner {
                permWX = syscall.S_IWUSR | syscall.S_IXUSR
                // Check the current directory having x and w permissions
                if (permission & permWX) == permWX {
                        c.vlog("Directory::GetPermissions fail at owner: %o - %o",
                                permission, permWX)
                        return fuse.OK
                }
        } 
        
        if gid == dirGroup {
                permWX = syscall.S_IWGRP | syscall.S_IXGRP
                if (permission & permWX) == permWX {
                        c.vlog("Directory::GetPermissions fail at group: %o - %o",
                                permission, permWX)
                        return fuse.OK
                }
        }
        
        // all the other
        permWX = syscall.S_IWOTH | syscall.S_IXOTH
        if (permission & permWX) == permWX {
                c.vlog("Directory::GetPermissions fail at other: %o - %o",
                        permission, permWX)
                return fuse.OK
        }
        
        return fuse.EACCES
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {
	c.vlog("Directory::Unlink Enter %s", name)
	defer c.vlog("Directory::Unlink Exit")
	result := func() (status fuse.Status) {
		defer dir.Lock().Unlock()

		record, exists := dir.dirChildren.getNameRecord(c, name)
		if !exists {
			return fuse.ENOENT
		}

		type_ := objectTypeToFileType(c, record.Type())
		if type_ == fuse.S_IFDIR {
                        c.vlog("Directory::Unlink directory")
			return fuse.Status(syscall.EISDIR)
		}
                
                owner := c.fuseCtx.Owner

                // Verify the permission of the directory in order to delete a child
                // If the sticky bit of the directory is set, the action can only be 
                // performed by file's owner, directory's owner, or root user
                
                
                parent := dir.parent()

                // When parent is a nil, the directory is a root. Therefore, it can
                // always be albe to unlink any inodes because of its permission 777,
                // which is hardcoded in the file daemon/workspaceroot.go. In this
                // case, only no WorkspaceRoot needs a permission check.
                if parent != nil{
                        dirRecord, exist := parent.getChildRecord(c,
                                                        dir.InodeCommon.id)
                        dirOwner := quantumfs.SystemUid(dirRecord.Owner(), owner.Uid)
                        dirGroup := quantumfs.SystemGid(dirRecord.Group(), owner.Gid)
                        permission := dirRecord.Permissions()
                        fileOwner := quantumfs.SystemUid(record.Owner(), owner.Uid)
                        if exist != nil {
                                c.vlog("Directory::Unlink no directory")
                                return fuse.ENOENT
                        }

                        status = dir.getPermissions(c, permission, owner.Uid,
                                        owner.Gid, fileOwner, dirOwner, dirGroup)
                        if status != fuse.OK {
                                return status
                        }
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

		record, exists := dir.dirChildren.getNameRecord(c, name)
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

		if _, exists := dir.dirChildren.getInode(c, name); exists {
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

func (dir *Directory) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("Directory::Mknod Enter")
	defer c.vlog("Directory::Mknod Exit")

	result := func() fuse.Status {
		defer dir.Lock().Unlock()

		if _, exists := dir.dirChildren.getInode(c, name); exists {
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

		if _, exists := dir.dirChildren.getInode(c, oldName); !exists {
			return fuse.ENOENT
		}

		dir.dirChildren.rename(c, oldName, newName)

		dir.updateSize_(c)
		dir.self.dirty(c)

		return fuse.OK
	}()

	return result
}

func sortParentChild(a *Directory, b *Directory) (parentDir *Directory,
	childDir *Directory) {

	// Determine if a parent-child relationship between the
	// directories exist
	var parent *Directory
	var child *Directory

	upwardsParent := a.parent()
	for ; upwardsParent != nil; upwardsParent = upwardsParent.parent() {
		if upwardsParent.inodeNum() == b.inodeNum() {

			// a is a (grand-)child of b
			parent = b
			child = a
			break
		}
	}

	if upwardsParent == nil {
		upwardsParent = b.parent()
		for ; upwardsParent != nil; upwardsParent = upwardsParent.parent() {
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
		parent, child := sortParentChild(dst, dir)
		firstLock, lastLock := getLockOrder(dst, dir)
		firstLock.(*Directory).Lock()
		lastLock.(*Directory).Lock()

		defer child.lock.Unlock()

		result := func() fuse.Status {
			// we need to unlock the parent early
			defer parent.lock.Unlock()

			if _, exists := dir.dirChildren.getInode(c,
				oldName); !exists {

				return fuse.ENOENT
			}

			oldInodeId, exists := dir.dirChildren.getInode(c, oldName)
			if !exists {
				return fuse.ENOENT
			}

			// copy the record
			oldEntry, exists := dir.dirChildren.getRecord(c, oldInodeId)
			if !exists {
				return fuse.ENOENT
			}
			newEntry := cloneDirectoryRecord(oldEntry)
			newEntry.SetFilename(newName)

			// update the inode
			child := c.qfs.inode(c, oldInodeId)
			child.markSelfAccessed(c, false)
			child.setParent(dst.self)

			//delete the target InodeId
			dst.dirChildren.delete(newName)

			// set entry in new directory
			dst.dirChildren.insertRecord(c, oldInodeId, newEntry)

			// Remove entry in old directory
			dir.dirChildren.delete(oldName)

			child.setName(newName)
			child.markSelfAccessed(c, true)
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

	return dir.parent().getChildXAttrSize(c, dir.inodeNum(), attr)
}

func (dir *Directory) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	return dir.parent().getChildXAttrData(c, dir.inodeNum(), attr)
}

func (dir *Directory) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	return dir.parent().listChildXAttr(c, dir.inodeNum())
}

func (dir *Directory) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	return dir.parent().setChildXAttr(c, dir.inodeNum(), attr, data)
}

func (dir *Directory) RemoveXAttr(c *ctx, attr string) fuse.Status {
	return dir.parent().removeChildXAttr(c, dir.inodeNum(), attr)
}

func (dir *Directory) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("Directory::syncChild Enter")
	defer c.vlog("Directory::syncChild Exit")

	ok, key := func() (bool, quantumfs.ObjectKey) {
		defer dir.Lock().Unlock()
		dir.self.dirty(c)

		entry, exists := dir.dirChildren.getRecord(c, inodeNum)
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

	record, ok := dir.dirChildren.getRecord(c, inodeNum)
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

	record, exists := dir.dirChildren.getRecord(c, inodeNum)
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

	record, exists := dir.dirChildren.getRecord(c, inodeNum)
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

	c.vlog("ReadDirPlus directorySnapshot")
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
