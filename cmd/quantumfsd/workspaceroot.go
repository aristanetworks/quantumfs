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
	for _, entry := range baseLayer.Entries {
		inodeId := globalQfs.newInodeId()
		children[BytesToString(entry.Filename[:])] = inodeId
		globalQfs.setInode(inodeId, newDirectory(entry.ID, inodeId))
	}

	return &WorkspaceRoot{
		InodeCommon: InodeCommon{id: inodeNum},
		rootId:      rootId,
		baseLayer:   baseLayer,
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

func (wsr *WorkspaceRoot) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	children := make([]directoryContents, 0, wsr.baseLayer.NumEntries)
	for _, entry := range wsr.baseLayer.Entries {
		filename := BytesToString(entry.Filename[:])

		var permissions uint32
		permissions |= uint32(entry.Permissions)
		permissions |= uint32(entry.Permissions << 3)
		permissions |= uint32(entry.Permissions << 6)
		permissions |= objectTypeToFileType(entry.Type)

		entryInfo := directoryContents{
			filename: filename,
			fuseType: objectTypeToFileType(entry.Type),
			attr: fuse.Attr{
				Ino:       wsr.children[filename],
				Size:      qfsBlockSize,
				Blocks:    1,
				Atime:     entry.ModificationTime.Seconds(),
				Mtime:     entry.ModificationTime.Seconds(),
				Ctime:     entry.CreationTime.Seconds(),
				Atimensec: entry.ModificationTime.Nanoseconds(),
				Mtimensec: entry.ModificationTime.Nanoseconds(),
				Ctimensec: entry.CreationTime.Nanoseconds(),
				Mode:      permissions,
				Nlink:     uint32(entry.Size),
				Owner: fuse.Owner{
					Uid: quantumfs.SystemUid(entry.Owner, context.Owner.Uid),
					Gid: quantumfs.SystemGid(entry.Group, context.Owner.Gid),
				},
				Blksize: qfsBlockSize,
			},
		}

		children = append(children, entryInfo)
	}

	ds := newDirectorySnapshot(children, wsr.InodeCommon.id)
	globalQfs.setFileHandle(ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (wsr *WorkspaceRoot) Lookup(name string, out *fuse.EntryOut) fuse.Status {
	fmt.Println("Unhandled Lookup in WorkspaceRoot")
	return fuse.ENOSYS
}
