// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "encoding/json"
import "fmt"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// WorkspaceRoot acts similarly to a workspace directory except only a single object
// ID is used instead of one for each layer and that ID is directly requested from
// the WorkspaceDB instead of passed in from the parent.
type WorkspaceRoot struct {
	InodeCommon
	rootId    quantumfs.ObjectKey
	baseLayer quantumfs.DirectoryEntry
	children  map[string]uint64

	// Indexed by inode number
	childrenRecords map[uint64]*quantumfs.DirectoryRecord
}

// Fetching the number of child directories for all the workspaces within a namespace
// is relatively expensive and not terribly useful. Instead fake it and assume a
// normal number here.
func fillWorkspaceAttrFake(attr *fuse.Attr, inodeNum uint64, workspace string) {
	fillAttr(attr, inodeNum, 27)
	attr.Mode = 0777 | fuse.S_IFDIR
}

func newWorkspaceRoot(parentName string, name string, inodeNum uint64) Inode {
	rootId := config.workspaceDB.Workspace(parentName, name)

	object := DataStore.Get(rootId)
	var workspaceRoot quantumfs.WorkspaceRoot
	if err := json.Unmarshal(object.Get(), &workspaceRoot); err != nil {
		panic("Couldn't decode WorkspaceRoot Object")
	}

	object = DataStore.Get(workspaceRoot.BaseLayer)

	var baseLayer quantumfs.DirectoryEntry
	if err := json.Unmarshal(object.Get(), &baseLayer); err != nil {
		panic("Couldn't decode base layer object")
	}

	children := make(map[string]uint64, baseLayer.NumEntries)
	childrenRecords := make(map[uint64]*quantumfs.DirectoryRecord, baseLayer.NumEntries)
	for i, entry := range baseLayer.Entries {
		inodeId := globalQfs.newInodeId()
		children[BytesToString(entry.Filename[:])] = inodeId
		childrenRecords[inodeId] = &baseLayer.Entries[i]
		globalQfs.setInode(inodeId, newDirectory(entry.ID, inodeId))
	}

	return &WorkspaceRoot{
		InodeCommon:     InodeCommon{id: inodeNum},
		rootId:          rootId,
		baseLayer:       baseLayer,
		children:        children,
		childrenRecords: childrenRecords,
	}
}

func (wsr *WorkspaceRoot) GetAttr(out *fuse.AttrOut) fuse.Status {
	out.AttrValid = config.cacheTimeSeconds
	out.AttrValidNsec = config.cacheTimeNsecs
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

func (wsr *WorkspaceRoot) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func fillAttrWithDirectoryRecord(attr *fuse.Attr, inodeNum uint64, owner fuse.Owner, entry *quantumfs.DirectoryRecord) {

	attr.Ino = inodeNum

	fileType := objectTypeToFileType(entry.Type)
	switch fileType {
	case fuse.S_IFDIR:
		attr.Size = qfsBlockSize
		attr.Blocks = 1
	default:
		fmt.Println("Unhandled filetype in fillAttrWithDirectoryRecord",
			fileType)
		fallthrough
	case fuse.S_IFREG:
		attr.Size = entry.Size
		attr.Blocks = BlocksRoundUp(entry.Size, qfsBlockSize)
	}

	attr.Atime = entry.ModificationTime.Seconds()
	attr.Mtime = entry.ModificationTime.Seconds()
	attr.Ctime = entry.CreationTime.Seconds()
	attr.Atimensec = entry.ModificationTime.Nanoseconds()
	attr.Mtimensec = entry.ModificationTime.Nanoseconds()
	attr.Ctimensec = entry.CreationTime.Nanoseconds()

	var permissions uint32
	permissions |= uint32(entry.Permissions)
	permissions |= uint32(entry.Permissions << 3)
	permissions |= uint32(entry.Permissions << 6)
	permissions |= fileType

	attr.Mode = permissions
	attr.Nlink = uint32(entry.Size)
	attr.Owner.Uid = quantumfs.SystemUid(entry.Owner, owner.Uid)
	attr.Owner.Gid = quantumfs.SystemGid(entry.Group, owner.Gid)
	attr.Blksize = qfsBlockSize
}

func (wsr *WorkspaceRoot) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
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

	ds := newDirectorySnapshot(children, wsr.InodeCommon.id)
	globalQfs.setFileHandle(ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (wsr *WorkspaceRoot) Lookup(context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	inodeNum, exists := wsr.children[name]
	if !exists {
		return fuse.ENOENT
	}

	out.NodeId = inodeNum
	fillEntryOutCacheData(out)
	fillAttrWithDirectoryRecord(&out.Attr, out.NodeId, context.Owner, wsr.childrenRecords[inodeNum])

	return fuse.OK
}

func (wsr *WorkspaceRoot) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	fmt.Println("Unhandled Create call in WorkspaceRoot")
	return fuse.ENOSYS
}
