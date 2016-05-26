// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "crypto/sha1"
import "encoding/json"
import "syscall"
import "time"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// This file contains the normal directory Inode type for a workspace
type Directory struct {
	InodeCommon
	parent      Inode
	baseLayerId quantumfs.ObjectKey
	children    map[string]InodeId

	// Indexed by inode number
	childrenRecords map[InodeId]*quantumfs.DirectoryRecord

	dirtyChildren_ []Inode // list of children which are currently dirty
}

func initDirectory(c *ctx, dir *Directory, baseLayerId quantumfs.ObjectKey,
	inodeNum InodeId, parent Inode) {

	object := DataStore.Get(c, baseLayerId)
	if object == nil {
		panic("No baseLayer object")
	}

	var baseLayer quantumfs.DirectoryEntry
	if err := json.Unmarshal(object.Get(), &baseLayer); err != nil {
		panic("Couldn't decode base layer object")
	}

	children := make(map[string]InodeId, baseLayer.NumEntries)
	childrenRecords := make(map[InodeId]*quantumfs.DirectoryRecord,
		baseLayer.NumEntries)
	for i, entry := range baseLayer.Entries {
		inodeId := c.qfs.newInodeId()
		children[BytesToString(entry.Filename[:])] = inodeId
		childrenRecords[inodeId] = &baseLayer.Entries[i]
		c.qfs.setInode(c, inodeId, newDirectory(c, entry.ID, inodeId, dir))
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

func (dir *Directory) addChild(c *ctx, name string, inodeNum InodeId,
	child *quantumfs.DirectoryRecord) {

	dir.children[name] = inodeNum
	dir.childrenRecords[inodeNum] = child
	dir.self.dirty(c)
}

func (dir *Directory) dirty(c *ctx) {
	dir.dirty_ = true
	dir.parent.dirtyChild(c, dir)
}

// Record that a specific child is dirty and when syncing heirarchically, sync them
// as well.
func (dir *Directory) dirtyChild(c *ctx, child Inode) {
	dir.dirtyChildren_ = append(dir.dirtyChildren_, child)
	dir.self.dirty(c)
}

func (dir *Directory) sync(c *ctx) quantumfs.ObjectKey {
	if !dir.dirty_ {
		return dir.baseLayerId
	}

	dir.updateRecords(c)

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

	dir.dirty_ = false

	return dir.baseLayerId
}

// Walk the list of children which are dirty and have them recompute their new key
// wsr can update its new key.
func (dir *Directory) updateRecords(c *ctx) {
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
	case fuse.S_IFREG:
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
			attr.InHeader.Context.Owner.Uid)
	}

	if BitFlagsSet(valid, fuse.FATTR_GID) {
		entry.Group = quantumfs.ObjectGid(c.Ctx, attr.Owner.Gid,
			attr.InHeader.Context.Owner.Gid)
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

	fillAttrOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
		attr.SetAttrInCommon.InHeader.Context.Owner, entry)

	dir.self.dirty(c)

	return fuse.OK
}

func (dir *Directory) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	c.elog("Unsupported Access on Directory")
	return fuse.ENOSYS
}

func (dir *Directory) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	var childDirectories uint32
	for _, entry := range dir.childrenRecords {
		if entry.Type == quantumfs.ObjectTypeDirectoryEntry {
			childDirectories++
		}
	}
	fillAttr(&out.Attr, dir.InodeCommon.id, childDirectories)
	out.Attr.Mode = 0777 | fuse.S_IFDIR
	return fuse.OK
}

func (dir *Directory) Lookup(c *ctx, context fuse.Context, name string,
	out *fuse.EntryOut) fuse.Status {

	inodeNum, exists := dir.children[name]
	if !exists {
		return fuse.ENOENT
	}

	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum, context.Owner,
		dir.childrenRecords[inodeNum])

	return fuse.OK
}

func (dir *Directory) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOSYS
}

func (dir *Directory) OpenDir(c *ctx, context fuse.Context, flags uint32,
	mode uint32, out *fuse.OpenOut) fuse.Status {

	children := make([]directoryContents, 0, len(dir.childrenRecords))
	for _, entry := range dir.childrenRecords {
		filename := BytesToString(entry.Filename[:])

		entryInfo := directoryContents{
			filename: filename,
			fuseType: objectTypeToFileType(c, entry.Type),
		}
		fillAttrWithDirectoryRecord(c, &entryInfo.attr,
			dir.children[filename], context.Owner, entry)

		children = append(children, entryInfo)
	}

	ds := newDirectorySnapshot(c, children, dir.InodeCommon.id)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (dir *Directory) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	if _, exists := dir.children[name]; exists {
		return fuse.Status(syscall.EEXIST)
	}

	c.vlog("Creating file: '%s'", name)

	now := time.Now()
	uid := input.InHeader.Context.Owner.Uid
	gid := input.InHeader.Context.Owner.Gid

	entry := quantumfs.DirectoryRecord{
		Filename:           StringToBytes(name),
		ID:                 quantumfs.EmptyBlockKey,
		Type:               quantumfs.ObjectTypeSmallFile,
		Permissions:        modeToPermissions(input.Mode, input.Umask),
		Owner:              quantumfs.ObjectUid(c.Ctx, uid, uid),
		Group:              quantumfs.ObjectGid(c.Ctx, gid, gid),
		Size:               0,
		ExtendedAttributes: quantumfs.EmptyBlockKey,
		CreationTime:       quantumfs.NewTime(now),
		ModificationTime:   quantumfs.NewTime(now),
	}

	inodeNum := c.qfs.newInodeId()
	dir.addChild(c, name, inodeNum, &entry)
	file := newFile(inodeNum, quantumfs.ObjectTypeSmallFile,
		quantumfs.EmptyBlockKey, dir.self)
	c.qfs.setInode(c, inodeNum, file)

	fillEntryOutCacheData(c, &out.EntryOut)
	out.EntryOut.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.EntryOut.Attr, inodeNum,
		input.InHeader.Context.Owner, &entry)

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(file, inodeNum, fileHandleNum)
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

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

	if _, exists := dir.children[name]; exists {
		return fuse.Status(syscall.EEXIST)
	}

	now := time.Now()
	uid := input.InHeader.Context.Owner.Uid
	gid := input.InHeader.Context.Owner.Gid

	entry := quantumfs.DirectoryRecord{
		Filename:           StringToBytes(name),
		ID:                 quantumfs.EmptyDirKey,
		Type:               quantumfs.ObjectTypeDirectoryEntry,
		Permissions:        modeToPermissions(input.Mode, input.Umask),
		Owner:              quantumfs.ObjectUid(c.Ctx, uid, uid),
		Group:              quantumfs.ObjectGid(c.Ctx, gid, gid),
		Size:               0,
		ExtendedAttributes: quantumfs.EmptyBlockKey,
		CreationTime:       quantumfs.NewTime(now),
		ModificationTime:   quantumfs.NewTime(now),
	}

	inodeNum := c.qfs.newInodeId()
	dir.addChild(c, name, inodeNum, &entry)
	newDir := newDirectory(c, quantumfs.EmptyDirKey, inodeNum, dir.self)
	c.qfs.setInode(c, inodeNum, newDir)

	fillEntryOutCacheData(c, out)
	out.NodeId = uint64(inodeNum)
	fillAttrWithDirectoryRecord(c, &out.Attr, inodeNum,
		input.InHeader.Context.Owner, &entry)

	return fuse.OK
}

func (dir *Directory) Unlink(c *ctx, name string) fuse.Status {

	return fuse.ENOTDIR
}

type directoryContents struct {
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
