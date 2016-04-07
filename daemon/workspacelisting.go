// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The code which handles listing available workspaces as the first two levels of the
// directory hierarchy.
package daemon

import "time"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func NewNamespaceList() Inode {
	nsl := NamespaceList{
		InodeCommon: InodeCommon{id: quantumfs.InodeIdRoot},
		namespaces:  make(map[string]uint64),
	}
	return &nsl
}

type NamespaceList struct {
	InodeCommon

	// Map from child name to Inode ID
	namespaces map[string]uint64
}

func (nsl *NamespaceList) GetAttr(out *fuse.AttrOut) fuse.Status {
	out.AttrValid = config.CacheTimeSeconds
	out.AttrValidNsec = config.CacheTimeNsecs

	fillRootAttr(&out.Attr, nsl.InodeCommon.id)
	return fuse.OK
}

func fillRootAttr(attr *fuse.Attr, inodeNum uint64) {
	fillAttr(attr, inodeNum,
		uint32(globalQfs.config.WorkspaceDB.NumNamespaces()))
}

type listingAttrFill func(attr *fuse.Attr, inodeNum uint64, name string)

func fillNamespaceAttr(attr *fuse.Attr, inodeNum uint64, namespace string) {
	fillAttr(attr, inodeNum,
		uint32(globalQfs.config.WorkspaceDB.NumWorkspaces(namespace)))
}

func fillAttr(attr *fuse.Attr, inodeNum uint64, numChildren uint32) {
	attr.Ino = inodeNum
	attr.Size = 4096
	attr.Blocks = 1

	now := time.Now()
	attr.Atime = uint64(now.Unix())
	attr.Atimensec = uint32(now.Nanosecond())
	attr.Mtime = uint64(now.Unix())
	attr.Mtimensec = uint32(now.Nanosecond())

	attr.Ctime = 1
	attr.Ctimensec = 1
	attr.Mode = 0555 | fuse.S_IFDIR
	attr.Nlink = 2 + numChildren
	attr.Owner.Uid = 0
	attr.Owner.Gid = 0
	attr.Blksize = 4096
}

func fillEntryOutCacheData(out *fuse.EntryOut) {
	out.Generation = 1
	out.EntryValid = config.CacheTimeSeconds
	out.EntryValidNsec = config.CacheTimeNsecs
	out.AttrValid = config.CacheTimeSeconds
	out.AttrValidNsec = config.CacheTimeNsecs
}

// Update the internal namespaces list with the most recent available listing
func updateChildren(parentName string, names []string, inodeMap *map[string]uint64,
	newInode func(parentName string, name string, inodeId uint64) Inode) {

	touched := make(map[string]bool)

	// First add any new entries
	for _, name := range names {
		if _, exists := (*inodeMap)[name]; !exists {
			inodeId := globalQfs.newInodeId()
			(*inodeMap)[name] = inodeId
			globalQfs.setInode(inodeId, newInode(parentName, name, inodeId))
		}
		touched[name] = true
	}

	// Then delete entries which no longer exist
	for name, _ := range *inodeMap {
		if _, exists := touched[name]; !exists {
			globalQfs.setInode((*inodeMap)[name], nil)
			delete(*inodeMap, name)
		}
	}
}

func snapshotChildren(children *map[string]uint64, fillAttr listingAttrFill) []directoryContents {
	out := make([]directoryContents, 0, len(*children))
	for name, inode := range *children {
		child := directoryContents{
			filename: name,
			fuseType: fuse.S_IFDIR,
		}
		fillAttr(&child.attr, inode, name)

		out = append(out, child)
	}

	return out
}

func (nsl *NamespaceList) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (nsl *NamespaceList) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	updateChildren("/", config.WorkspaceDB.NamespaceList(), &nsl.namespaces, newWorkspaceList)
	children := snapshotChildren(&nsl.namespaces, fillNamespaceAttr)

	api := directoryContents{
		filename: quantumfs.ApiPath,
		fuseType: fuse.S_IFREG,
	}
	fillApiAttr(&api.attr)
	children = append(children, api)

	ds := newDirectorySnapshot(children, nsl.InodeCommon.id)
	globalQfs.setFileHandle(ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (nsl *NamespaceList) Lookup(context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		fillEntryOutCacheData(out)
		fillApiAttr(&out.Attr)
		return fuse.OK
	}

	if !config.WorkspaceDB.NamespaceExists(name) {
		return fuse.ENOENT
	}

	updateChildren("/", config.WorkspaceDB.NamespaceList(), &nsl.namespaces,
		newWorkspaceList)

	out.NodeId = nsl.namespaces[name]
	fillEntryOutCacheData(out)
	fillNamespaceAttr(&out.Attr, out.NodeId, name)

	return fuse.OK
}

func (nsl *NamespaceList) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	return fuse.EACCES
}

func newWorkspaceList(parentName string, name string, inodeNum uint64) Inode {
	nsd := WorkspaceList{
		InodeCommon:   InodeCommon{id: inodeNum},
		namespaceName: name,
		workspaces:    make(map[string]uint64),
	}
	return &nsd
}

type WorkspaceList struct {
	InodeCommon
	namespaceName string

	// Map from child name to Inode ID
	workspaces map[string]uint64
}

func (nsd *WorkspaceList) GetAttr(out *fuse.AttrOut) fuse.Status {
	out.AttrValid = config.CacheTimeSeconds
	out.AttrValidNsec = config.CacheTimeNsecs

	fillRootAttr(&out.Attr, nsd.InodeCommon.id)
	return fuse.OK
}

func (wsl *WorkspaceList) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	updateChildren(wsl.namespaceName,
		config.WorkspaceDB.WorkspaceList(wsl.namespaceName), &wsl.workspaces,
		newWorkspaceRoot)
	children := snapshotChildren(&wsl.workspaces, fillWorkspaceAttrFake)

	ds := newDirectorySnapshot(children, wsl.InodeCommon.id)
	globalQfs.setFileHandle(ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (wsl *WorkspaceList) Lookup(context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	if !config.WorkspaceDB.WorkspaceExists(wsl.namespaceName, name) {
		return fuse.ENOENT
	}

	updateChildren(wsl.namespaceName,
		config.WorkspaceDB.WorkspaceList(wsl.namespaceName), &wsl.workspaces,
		newWorkspaceRoot)

	out.NodeId = wsl.workspaces[name]
	fillEntryOutCacheData(out)
	fillWorkspaceAttrFake(&out.Attr, out.NodeId, name)

	return fuse.OK
}

func (wsl *WorkspaceList) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	return fuse.EACCES
}
