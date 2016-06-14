// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "crypto/sha1"
import "encoding/json"
import "errors"
import "syscall"
import "time"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

type InodeConstructor func(c *ctx, key quantumfs.ObjectKey, inodeNum InodeId,
	parent Inode) Inode

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon

	// These fields are constant after instantiation
	parent Inode

	// These fields are protected by the InodeCommon.lock
	baseLayerId quantumfs.ObjectKey
	children    map[string]InodeId

	// Indexed by inode number
	childrenRecords map[InodeId]*quantumfs.DirectoryRecord

	dirtyChildren_ []Inode // list of children which are currently dirty
}

func initDirectory(c *ctx, dir *Directory, baseLayerId quantumfs.ObjectKey,
	inodeNum InodeId, parent Inode) {

	c.vlog("initDirectory Fetching directory baselayer from %s", baseLayerId)

	object := DataStore.Get(c, baseLayerId)
	if object == nil {
		panic("No baseLayer object")
	}

	var baseLayer quantumfs.DirectoryEntry
	if err := json.Unmarshal(object.Get(), &baseLayer); err != nil {
		c.elog("Invalid base layer object: %v %v", err, string(object.Get()))
		panic("Couldn't decode base layer object")
	}

	children := make(map[string]InodeId, baseLayer.NumEntries)
	childrenRecords := make(map[InodeId]*quantumfs.DirectoryRecord,
		baseLayer.NumEntries)
	for i, entry := range baseLayer.Entries {
		inodeId := c.qfs.newInodeId()
		children[BytesToString(entry.Filename[:])] = inodeId
		childrenRecords[inodeId] = &baseLayer.Entries[i]

		var constructor InodeConstructor
		switch entry.Type {
		default:
			c.elog("Unknown InodeConstructor type: %d", entry.Type)
		case quantumfs.ObjectTypeDirectoryEntry:
			constructor = newDirectory
		case quantumfs.ObjectTypeSmallFile:
			constructor = newSmallFile
		case quantumfs.ObjectTypeSymlink:
			constructor = newSymlink
		}

		c.qfs.setInode(c, inodeId, constructor(c, entry.ID, inodeId, dir))
	}

	dir.InodeCommon = InodeCommon{id: inodeNum, self: dir}
	dir.parent = parent
	dir.children = children
	dir.childrenRecords = childrenRecords
	dir.dirtyChildren_ = make([]Inode, 0)
}

func newDirectory(c *ctx, baseLayerId quantumfs.ObjectKey, inodeNum InodeId,
	parent Inode) Inode {

	var dir Directory

	initDirectory(c, &dir, baseLayerId, inodeNum, parent)

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
		dir.parent.setChildAttr(c, dir.id, &attr, nil)
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
	delete(dir.childrenRecords, dir.children[name])
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
		dir.lock.Lock()
		defer dir.lock.Unlock()
		dir.dirtyChildren_ = append(dir.dirtyChildren_, child)
	}()
	dir.self.dirty(c)
}

func (dir *Directory) sync(c *ctx) quantumfs.ObjectKey {
	if dir.isDirty() {
		return dir.baseLayerId
	}

	dir.lock.Lock()
	defer dir.lock.Unlock()

	dir.updateRecords_(c)

	// Compile the internal records into a block which can be placed in the
	// datastore.
	baseLayer := quantumfs.NewDirectoryEntry(len(dir.childrenRecords))
	baseLayer.NumEntries = uint32(len(dir.childrenRecords))

	for _, entry := range dir.childrenRecords {
		baseLayer.Entries = append(baseLayer.Entries, *entry)
	}

	// Upload the base layer object
	bytes, err := json.Marshal(baseLayer)
	if err != nil {
		panic("Failed to marshal baselayer")
	}

	hash := sha1.Sum(bytes)
	newBaseLayerId := quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata, hash)

	var buffer quantumfs.Buffer
	buffer.Set(bytes)
	if err := c.durableStore.Set(newBaseLayerId, &buffer); err != nil {
		panic("Failed to upload new baseLayer object")
	}

	dir.baseLayerId = newBaseLayerId

	dir.setDirty(false)

	return dir.baseLayerId
}

// Walk the list of children which are dirty and have them recompute their new key
// wsr can update its new key.
func (dir *Directory) updateRecords_(c *ctx) {
	for _, child := range dir.dirtyChildren_ {
		newKey := child.sync(c)
		dir.childrenRecords[child.inodeNum()].ID = newKey
	}
	dir.dirtyChildren_ = make([]Inode, 0)
}

func fillAttrWithDirectoryRecord(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	owner fuse.Owner, entry *quantumfs.DirectoryRecord) {

	attr.Ino = uint64(inodeNum)

	fileType := objectTypeToFileType(c, entry.Type)
	switch fileType {
	case fuse.S_IFDIR:
		attr.Size = qfsBlockSize
		attr.Blocks = 1
		attr.Nlink = uint32(entry.Size) + 2
	default:
		c.elog("Unhandled filetype in fillAttrWithDirectoryRecord",
			fileType)
		fallthrough
	case fuse.S_IFREG,
		fuse.S_IFLNK:

		attr.Size = entry.Size
		attr.Blocks = BlocksRoundUp(entry.Size, qfsBlockSize)
		attr.Nlink = 1
	}

	attr.Atime = entry.ModificationTime.Seconds()
	attr.Mtime = entry.ModificationTime.Seconds()
	attr.Ctime = entry.CreationTime.Seconds()
	attr.Atimensec = entry.ModificationTime.Nanoseconds()
	attr.Mtimensec = entry.ModificationTime.Nanoseconds()
	attr.Ctimensec = entry.CreationTime.Nanoseconds()

	var permissions uint32
	permissions |= uint32(entry.Permissions)
	permissions |= uint32(entry.Permissions) << 3
	permissions |= uint32(entry.Permissions) << 6
	permissions |= fileType

	attr.Mode = permissions
	attr.Owner.Uid = quantumfs.SystemUid(entry.Owner, owner.Uid)
	attr.Owner.Gid = quantumfs.SystemGid(entry.Group, owner.Gid)
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
	attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {

	result := func() fuse.Status {
		dir.lock.Lock()
		defer dir.lock.Unlock()

		entry, exists := dir.childrenRecords[inodeNum]
		if !exists {
			return fuse.ENOENT
		}

		valid := uint(attr.SetAttrInCommon.Valid)
		if BitFlagsSet(valid, fuse.FATTR_FH|
			fuse.FATTR_LOCKOWNER) {
			c.elog("Unsupported attribute(s) to set", valid)
			return fuse.ENOSYS
		}

		if BitFlagsSet(valid, fuse.FATTR_MODE) {
			entry.Permissions = modeToPermissions(attr.Mode, 0)
		}

		if BitFlagsSet(valid, fuse.FATTR_UID) {
			entry.Owner = quantumfs.ObjectUid(c.Ctx, attr.Owner.Uid,
				c.fuseCtx.Owner.Uid)
		}

		if BitFlagsSet(valid, fuse.FATTR_GID) {
			entry.Group = quantumfs.ObjectGid(c.Ctx, attr.Owner.Gid,
				c.fuseCtx.Owner.Gid)
		}

		if BitFlagsSet(valid, fuse.FATTR_SIZE) {
			entry.Size = attr.Size
		}

		if BitFlagsSet(valid, fuse.FATTR_ATIME|fuse.FATTR_ATIME_NOW) {
			// atime is ignored and not stored
		}

		if BitFlagsSet(valid, fuse.FATTR_MTIME) {
			entry.ModificationTime = quantumfs.NewTimeSeconds(attr.Mtime,
				attr.Mtimensec)
		}

		if BitFlagsSet(valid, fuse.FATTR_MTIME_NOW) {
			entry.ModificationTime = quantumfs.NewTime(time.Now())
		}

		if BitFlagsSet(valid, fuse.FATTR_CTIME) {
			entry.CreationTime = quantumfs.NewTimeSeconds(attr.Ctime,
				attr.Ctimensec)
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
	dir.lock.RLock()
	defer dir.lock.RUnlock()

	var childDirectories uint32
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	for _, entry := range dir.childrenRecords {
		if entry.Type == quantumfs.ObjectTypeDirectoryEntry {
			childDirectories++
		}
	}
	fillAttr(&out.Attr, dir.InodeCommon.id, childDirectories)
	out.Attr.Mode = 0777 | fuse.S_IFDIR
	return fuse.OK
}

func (dir *Directory) Lookup(c *ctx, name string, out *fuse.EntryOut) fuse.Status {
	dir.lock.RLock()
	defer dir.lock.RUnlock()

	inodeNum, exists := dir.children[name]
	if !exists {
		return fuse.ENOENT
	}

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

	dir.lock.RLock()
	defer dir.lock.RUnlock()

	children := make([]directoryContents, 0, len(dir.childrenRecords))
	for _, entry := range dir.childrenRecords {
		filename := BytesToString(entry.Filename[:])

		entryInfo := directoryContents{
			filename: filename,
			fuseType: objectTypeToFileType(c, entry.Type),
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr,
			dir.children[filename], c.fuseCtx.Owner, entry)

		children = append(children, entryInfo)
	}

	ds := newDirectorySnapshot(c, children, dir.InodeCommon.id)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (dir *Directory) create_(c *ctx, name string, mode uint32, umask uint32,
	constructor InodeConstructor, type_ quantumfs.ObjectType,
	key quantumfs.ObjectKey, out *fuse.EntryOut) Inode {

	now := time.Now()
	uid := c.fuseCtx.Owner.Uid
	gid := c.fuseCtx.Owner.Gid

	entry := quantumfs.DirectoryRecord{
		Filename:           StringToBytes256(name),
		ID:                 key,
		Type:               type_,
		Permissions:        modeToPermissions(mode, umask),
		Owner:              quantumfs.ObjectUid(c.Ctx, uid, uid),
		Group:              quantumfs.ObjectGid(c.Ctx, gid, gid),
		Size:               0,
		ExtendedAttributes: quantumfs.EmptyBlockKey,
		CreationTime:       quantumfs.NewTime(now),
		ModificationTime:   quantumfs.NewTime(now),
	}

	inodeNum := c.qfs.newInodeId()
	dir.addChild_(c, name, inodeNum, &entry)
	newEntity := constructor(c, key, inodeNum, dir.self)
	c.qfs.setInode(c, inodeNum, newEntity)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, c.fuseCtx.Owner, &entry)

	return newEntity
}

func (dir *Directory) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	var file Inode
	result := func() fuse.Status {
		dir.lock.Lock()
		defer dir.lock.Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
		}

		c.vlog("Creating file: '%s'", name)

		file = dir.create_(c, name, input.Mode, input.Umask, newSmallFile,
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
		fileHandleNum)
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	c.vlog("New file inode %d, fileHandle %d", file.inodeNum(), fileHandleNum)

	out.OpenOut.OpenFlags = 0
	out.OpenOut.Fh = uint64(fileHandleNum)

	return fuse.OK
}

func (dir *Directory) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid SetAttr on Directory")
	return fuse.ENOSYS
}

func (dir *Directory) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	result := func() fuse.Status {
		dir.lock.Lock()
		defer dir.lock.Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
		}

		dir.create_(c, name, input.Mode, input.Umask, newDirectory,
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

	dir.lock.RLock()
	defer dir.lock.RUnlock()

	if val, ok := dir.childrenRecords[inodeNum]; ok {
		return *val, nil
	}

	return quantumfs.DirectoryRecord{},
		errors.New("Inode given is not a child of this directory")
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {
	result := func() fuse.Status {
		dir.lock.Lock()
		defer dir.lock.Unlock()

		if _, exists := dir.children[name]; !exists {
			return fuse.ENOENT
		}

		inode := dir.children[name]
		type_ := objectTypeToFileType(c, dir.childrenRecords[inode].Type)
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
	result := func() fuse.Status {
		dir.lock.Lock()
		defer dir.lock.Unlock()
		if _, exists := dir.children[name]; !exists {
			return fuse.ENOENT
		}

		inode := dir.children[name]
		type_ := objectTypeToFileType(c, dir.childrenRecords[inode].Type)
		if type_ != fuse.S_IFDIR {
			return fuse.ENOTDIR
		}

		if dir.childrenRecords[inode].Size != 0 {
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
		dir.lock.Lock()
		defer dir.lock.Unlock()

		if _, exists := dir.children[name]; exists {
			return fuse.Status(syscall.EEXIST)
		}

		buf := quantumfs.NewBuffer([]byte(pointedTo))
		key = buf.Key(quantumfs.KeyTypeData)

		if err := DataStore.Set(c, key, buf); err != nil {
			c.elog("Failed to upload block: %v", err)
			return fuse.EIO
		}

		dir.create_(c, name, 0777, 0777, newSymlink, quantumfs.ObjectTypeSymlink,
			key, out)
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

type directoryContents struct {
	// All immutable after creation
	filename string
	fuseType uint32 // One of fuse.S_IFDIR, S_IFREG, etc
	attr     fuse.Attr
}

func newDirectorySnapshot(c *ctx, children []directoryContents,
	inodeNum InodeId) *directorySnapshot {

	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:       c.qfs.newFileHandleId(),
			inodeNum: inodeNum,
		},
		children: children,
	}

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children []directoryContents
}

func (ds *directorySnapshot) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.vlog("ReadDirPlus directorySnapshot", input, out)
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
