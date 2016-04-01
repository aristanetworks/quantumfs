// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The code which handles listing available workspaces as the first two levels of the
// directory hierarchy.
package main

import "fmt"
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
	out.AttrValid = config.cacheTimeSeconds
	out.AttrValidNsec = config.cacheTimeNsecs

	fillRootAttr(&out.Attr, nsl.InodeCommon.id)
	return fuse.OK
}

func fillRootAttr(attr *fuse.Attr, inodeNum uint64) {
	fillAttr(attr, inodeNum,
		uint32(globalQfs.config.workspaceDB.NumNamespaces()))
}

type listingAttrFill func(attr *fuse.Attr, inodeNum uint64, name string)

func fillNamespaceAttr(attr *fuse.Attr, inodeNum uint64, namespace string) {
	fillAttr(attr, inodeNum,
		uint32(globalQfs.config.workspaceDB.NumWorkspaces(namespace)))
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
	out.EntryValid = config.cacheTimeSeconds
	out.EntryValidNsec = config.cacheTimeNsecs
	out.AttrValid = config.cacheTimeSeconds
	out.AttrValidNsec = config.cacheTimeNsecs
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

func snapshotChildren(children *map[string]uint64) []nameInodeIdTuple {
	out := make([]nameInodeIdTuple, 0, len(*children))
	for name, inode := range *children {
		out = append(out, nameInodeIdTuple{name: name, inodeId: inode})
	}

	return out
}

func (nsl *NamespaceList) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (nsl *NamespaceList) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	updateChildren("/", config.workspaceDB.NamespaceList(), &nsl.namespaces, newWorkspaceList)
	children := snapshotChildren(&nsl.namespaces)
	children = append(children, nameInodeIdTuple{name: quantumfs.ApiPath, inodeId: quantumfs.InodeIdApi})

	ds := newDirectorySnapshot(children, nsl.InodeCommon.id, fillNamespaceAttr)
	globalQfs.setFileHandle(ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (nsl *NamespaceList) Lookup(name string, out *fuse.EntryOut) fuse.Status {
	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		fillEntryOutCacheData(out)
		fillApiAttr(&out.Attr)
		return fuse.OK
	}

	if !config.workspaceDB.NamespaceExists(name) {
		return fuse.ENOENT
	}

	updateChildren("/", config.workspaceDB.NamespaceList(), &nsl.namespaces,
		newWorkspaceList)

	out.NodeId = nsl.namespaces[name]
	fillEntryOutCacheData(out)
	fillNamespaceAttr(&out.Attr, out.NodeId, name)

	return fuse.OK
}

type nameInodeIdTuple struct {
	name    string
	inodeId uint64
}

func newDirectorySnapshot(children []nameInodeIdTuple, inodeNum uint64,
	fillFn listingAttrFill) *directorySnapshot {
	ds := directorySnapshot{
		FileHandleCommon: FileHandleCommon{
			id:       globalQfs.newFileHandleId(),
			inodeNum: inodeNum,
		},
		children: children,
		fillFn:   fillFn,
	}

	return &ds
}

type directorySnapshot struct {
	FileHandleCommon
	children []nameInodeIdTuple
	fillFn   listingAttrFill
}

func (ds *directorySnapshot) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("ReadDirPlus", input, out)
	offset := input.Offset

	// Add .
	if offset == 0 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = ds.FileHandleCommon.inodeNum
		fillEntryOutCacheData(details)
		fillRootAttr(&details.Attr, ds.FileHandleCommon.inodeNum)
	}
	offset++

	// Add .., even though this will be overwritten
	if offset == 1 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = ds.FileHandleCommon.inodeNum
		fillEntryOutCacheData(details)
		fillRootAttr(&details.Attr, ds.FileHandleCommon.inodeNum)
	}
	offset++

	processed := 0
	for _, child := range ds.children {
		var mode uint32
		if child.name == quantumfs.ApiPath {
			mode = fuse.S_IFREG
		} else {
			mode = fuse.S_IFDIR
		}
		entry := fuse.DirEntry{Mode: mode, Name: child.name}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			break
		}

		details.NodeId = child.inodeId
		fillEntryOutCacheData(details)
		if child.name == quantumfs.ApiPath {
			fillApiAttr(&details.Attr)
		} else {
			ds.fillFn(&details.Attr, details.NodeId, child.name)
		}

		processed++
	}

	ds.children = ds.children[processed:]

	return fuse.OK
}

func (ds *directorySnapshot) Read(offset uint64, size uint32, buf []byte, nonblocking bool) (fuse.ReadResult, fuse.Status) {
	fmt.Println("Invalid read on directorySnapshot")
	return nil, fuse.ENOSYS
}

func (ds *directorySnapshot) Write(offset uint64, size uint32, flags uint32, buf []byte) (uint32, fuse.Status) {
	fmt.Println("Invalid write on directorySnapshot")
	return 0, fuse.ENOSYS
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
	out.AttrValid = config.cacheTimeSeconds
	out.AttrValidNsec = config.cacheTimeNsecs

	fillRootAttr(&out.Attr, nsd.InodeCommon.id)
	return fuse.OK
}

func (wsl *WorkspaceList) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	updateChildren(wsl.namespaceName,
		config.workspaceDB.WorkspaceList(wsl.namespaceName), &wsl.workspaces,
		newWorkspaceRoot)
	children := snapshotChildren(&wsl.workspaces)

	ds := newDirectorySnapshot(children, wsl.InodeCommon.id, fillWorkspaceAttrFake)
	globalQfs.setFileHandle(ds.FileHandleCommon.id, ds)
	out.Fh = ds.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

func (wsl *WorkspaceList) Lookup(name string, out *fuse.EntryOut) fuse.Status {
	if !config.workspaceDB.WorkspaceExists(wsl.namespaceName, name) {
		return fuse.ENOENT
	}

	updateChildren(wsl.namespaceName,
		config.workspaceDB.WorkspaceList(wsl.namespaceName), &wsl.workspaces,
		newWorkspaceRoot)

	out.NodeId = wsl.workspaces[name]
	fillEntryOutCacheData(out)
	fillWorkspaceAttrFake(&out.Attr, out.NodeId, name)

	return fuse.OK
}
