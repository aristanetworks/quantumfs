// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "crypto/sha1"
import "encoding/json"
import "fmt"
import "syscall"
import "time"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// WorkspaceRoot acts similarly to a directory except only a single object ID is used
// instead of one for each layer and that ID is directly requested from the
// WorkspaceDB instead of passed in from the parent.
type WorkspaceRoot struct {
	InodeCommon
	namespace string
	workspace string
	rootId    quantumfs.ObjectKey
	baseLayer quantumfs.DirectoryEntry
	children  map[string]uint64

	// Indexed by inode number
	childrenRecords map[uint64]*quantumfs.DirectoryRecord

	dirty bool // True if the contents of this subtree has changed since last sync
}

// Fetching the number of child directories for all the workspaces within a namespace
// is relatively expensive and not terribly useful. Instead fake it and assume a
// normal number here.
func fillWorkspaceAttrFake(c *ctx, attr *fuse.Attr, inodeNum uint64, workspace string) {
	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(c *ctx, parentName string, name string, inodeNum uint64) Inode {
	rootId := c.workspaceDB.Workspace(parentName, name)

	object := DataStore.Get(c, rootId)
	var workspaceRoot quantumfs.WorkspaceRoot
	if err := json.Unmarshal(object.Get(), &workspaceRoot); err != nil {
		panic("Couldn't decode WorkspaceRoot Object")
	}

	object = DataStore.Get(c, workspaceRoot.BaseLayer)
	if object == nil {
		panic("No baseLayer object")
	}

	var baseLayer quantumfs.DirectoryEntry
	if err := json.Unmarshal(object.Get(), &baseLayer); err != nil {
		panic("Couldn't decode base layer object")
	}

	children := make(map[string]uint64, baseLayer.NumEntries)
	childrenRecords := make(map[uint64]*quantumfs.DirectoryRecord, baseLayer.NumEntries)
	for i, entry := range baseLayer.Entries {
		inodeId := c.qfs.newInodeId()
		children[BytesToString(entry.Filename[:])] = inodeId
		childrenRecords[inodeId] = &baseLayer.Entries[i]
		c.qfs.setInode(c, inodeId, newDirectory(entry.ID, inodeId))
	}

	return &WorkspaceRoot{
		InodeCommon:     InodeCommon{id: inodeNum},
		namespace:       parentName,
		workspace:       name,
		rootId:          rootId,
		baseLayer:       baseLayer,
		children:        children,
		childrenRecords: childrenRecords,
	}
}

func (wsr *WorkspaceRoot) addChild(c *ctx, name string, inodeNum uint64, child quantumfs.DirectoryRecord) {
	wsr.children[name] = inodeNum
	wsr.baseLayer.NumEntries++
	wsr.baseLayer.Entries = append(wsr.baseLayer.Entries, child)
	wsr.childrenRecords[inodeNum] = &wsr.baseLayer.Entries[wsr.baseLayer.NumEntries-1]
	wsr.dirty = true

	wsr.advanceRootId(c)
}

// If the WorkspaceRoot is dirty recompute the rootId and update the workspacedb
func (wsr *WorkspaceRoot) advanceRootId(c *ctx) {
	if !wsr.dirty {
		return
	}

	// Upload the base layer object
	bytes, err := json.Marshal(wsr.baseLayer)
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

	// Upload the workspaceroot object
	var workspaceRoot quantumfs.WorkspaceRoot
	workspaceRoot.BaseLayer = newBaseLayerId

	bytes, err = json.Marshal(workspaceRoot)
	if err != nil {
		panic("Failed to marshal workspace root")
	}

	hash = sha1.Sum(bytes)
	newRootId := quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata, hash)
	buffer.Set(bytes)
	if err := c.durableStore.Set(newRootId, &buffer); err != nil {
		panic("Failed to upload new workspace root")
	}

	// Update workspace rootId
	if newRootId != wsr.rootId {
		rootId, err := c.workspaceDB.AdvanceWorkspace(wsr.namespace,
			wsr.workspace, wsr.rootId, newRootId)

		if err != nil {
			panic("Unexpected workspace rootID update failure")
		}

		wsr.rootId = rootId
	}

	wsr.dirty = false
}

func (wsr *WorkspaceRoot) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	var childDirectories uint32
	for _, entry := range wsr.baseLayer.Entries {
		if entry.Type == quantumfs.ObjectTypeDirectoryEntry {
			childDirectories++
		}
	}
	fillAttr(&out.Attr, wsr.InodeCommon.id, childDirectories)
	out.Attr.Mode = 0777 | fuse.S_IFDIR
	return fuse.OK
}

func (wsr *WorkspaceRoot) Open(c *ctx, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func fillAttrWithDirectoryRecord(attr *fuse.Attr, inodeNum uint64, owner fuse.Owner, entry *quantumfs.DirectoryRecord) {

	attr.Ino = inodeNum

	fileType := objectTypeToFileType(entry.Type)
	switch fileType {
	case fuse.S_IFDIR:
		attr.Size = qfsBlockSize
		attr.Blocks = 1
		attr.Nlink = uint32(entry.Size)
	default:
		fmt.Println("Unhandled filetype in fillAttrWithDirectoryRecord",
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

func (wsr *WorkspaceRoot) OpenDir(c *ctx, context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	children := make([]directoryContents, 0, wsr.baseLayer.NumEntries)
	for _, entry := range wsr.baseLayer.Entries {
		filename := BytesToString(entry.Filename[:])

		entryInfo := directoryContents{
			filename: filename,
			fuseType: objectTypeToFileType(entry.Type),
		}
		fillAttrWithDirectoryRecord(&entryInfo.attr, wsr.children[filename], context.Owner, &entry)

		children = append(children, entryInfo)
	}

	ds := newDirectorySnapshot(c, children, wsr.InodeCommon.id)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (wsr *WorkspaceRoot) Lookup(c *ctx, context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	inodeNum, exists := wsr.children[name]
	if !exists {
		return fuse.ENOENT
	}

	out.NodeId = inodeNum
	fillEntryOutCacheData(c, out)
	fillAttrWithDirectoryRecord(&out.Attr, out.NodeId, context.Owner, wsr.childrenRecords[inodeNum])

	return fuse.OK
}

func (wsr *WorkspaceRoot) Create(c *ctx, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	if _, exists := wsr.children[name]; exists {
		return fuse.Status(syscall.EEXIST)
	}

	var permissions uint32
	mode := input.Mode & ^input.Umask
	permissions = mode & 0x7
	permissions |= (mode >> 3) & 0x7
	permissions |= (mode >> 6) & 0x7

	now := time.Now()
	uid := input.InHeader.Context.Owner.Uid
	gid := input.InHeader.Context.Owner.Gid

	entry := quantumfs.DirectoryRecord{
		Filename:           StringToBytes(name),
		ID:                 quantumfs.EmptyBlockKey,
		Type:               quantumfs.ObjectTypeSmallFile,
		Permissions:        uint8(permissions),
		Owner:              quantumfs.ObjectUid(uid, uid),
		Group:              quantumfs.ObjectGid(gid, gid),
		Size:               0,
		ExtendedAttributes: quantumfs.EmptyBlockKey,
		CreationTime:       quantumfs.NewTime(now),
		ModificationTime:   quantumfs.NewTime(now),
	}

	inodeNum := c.qfs.newInodeId()
	wsr.addChild(c, name, inodeNum, entry)
	file := newFile(inodeNum, quantumfs.ObjectTypeSmallFile, quantumfs.EmptyBlockKey)
	c.qfs.setInode(c, inodeNum, file)

	fillEntryOutCacheData(c, &out.EntryOut)
	fillAttrWithDirectoryRecord(&out.EntryOut.Attr, inodeNum, input.InHeader.Context.Owner,
		&entry)

	fileHandleNum := c.qfs.newFileHandleId()
	fileDescriptor := newFileDescriptor(file, inodeNum, fileHandleNum)
	c.qfs.setFileHandle(c, fileHandleNum, fileDescriptor)

	out.OpenOut.OpenFlags = 0
	out.OpenOut.Fh = fileHandleNum

	return fuse.OK
}
