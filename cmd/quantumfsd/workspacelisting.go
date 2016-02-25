// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The code which handles listing available workspaces as the first two levels of the
// directory hierarchy.
package main

import "fmt"
import "time"

import "github.com/hanwen/go-fuse/fuse"

func NewNamespaceList() *NamespaceList {
	nsl := NamespaceList{
		InodeCommon: InodeCommon{id: fuse.FUSE_ROOT_ID},
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

	namespaceListFillAttr(&out.Attr, nsl.InodeCommon.id)
	return fuse.OK
}

func namespaceListFillAttr(attr *fuse.Attr, inodeNum uint64) {
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
	attr.Nlink = 2 + uint32(globalQfs.config.workspaceDB.NumNamespaces())
	attr.Owner.Uid = 0
	attr.Owner.Gid = 0
	attr.Blksize = 4096
}

// Update the internal namespaces list with the most recent available listing
func (nsl *NamespaceList) updateNamespaceList() {
	namespaces := globalQfs.config.workspaceDB.NamespaceList()
	touched := make(map[string]bool)

	// First add any new entries
	for _, name := range namespaces {
		if _, exists := nsl.namespaces[name]; !exists {
			nsl.namespaces[name] = globalQfs.newInodeId()
		}
		touched[name] = true
	}

	// Then delete entries which no longer exist
	for name, _ := range nsl.namespaces {
		if _, exists := touched[name]; !exists {
			delete(nsl.namespaces, name)
		}
	}
}

func (nsl *NamespaceList) OpenDir(flags uint32, mode uint32, out *fuse.OpenOut) (result fuse.Status) {
	nsl.updateNamespaceList()

	// Now take a snapshot of that mapping for the NamespaceSnapshot
	children := make([]nameInodeIdTuple, 0, len(nsl.namespaces))
	for name, inode := range nsl.namespaces {
		children = append(children, nameInodeIdTuple{name: name, inodeId: inode})
	}

	ns := newNamespaceSnapshot(children)
	globalQfs.setFileHandle(ns.FileHandleCommon.id, ns)
	out.Fh = ns.FileHandleCommon.id
	out.OpenFlags = 0

	return fuse.OK
}

type nameInodeIdTuple struct {
	name    string
	inodeId uint64
}

func newNamespaceSnapshot(children []nameInodeIdTuple) *namespaceSnapshot {
	ns := namespaceSnapshot{
		FileHandleCommon: FileHandleCommon{
			id:       globalQfs.newFileHandleId(),
			inodeNum: fuse.FUSE_ROOT_ID,
		},
		namespaces: children,
	}

	return &ns
}

type namespaceSnapshot struct {
	FileHandleCommon
	namespaces []nameInodeIdTuple
}

func (ns *namespaceSnapshot) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("ReadDirPlus", input, out)
	offset := input.Offset

	// Add .
	if offset == 0 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = ns.FileHandleCommon.inodeNum
		details.Generation = 1
		details.EntryValid = config.cacheTimeSeconds
		details.EntryValidNsec = config.cacheTimeNsecs
		details.AttrValid = config.cacheTimeSeconds
		details.AttrValidNsec = config.cacheTimeNsecs
		namespaceListFillAttr(&details.Attr, ns.FileHandleCommon.inodeNum)
	}
	offset++

	// Add .., even though this will be overwritten
	if offset == 1 {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".."}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			return fuse.OK
		}

		details.NodeId = ns.FileHandleCommon.inodeNum
		details.Generation = 1
		details.EntryValid = config.cacheTimeSeconds
		details.EntryValidNsec = config.cacheTimeNsecs
		details.AttrValid = config.cacheTimeSeconds
		details.AttrValidNsec = config.cacheTimeNsecs
		namespaceListFillAttr(&details.Attr, ns.FileHandleCommon.inodeNum)
	}
	offset++

	toRemove := 0
	for _, namespace := range ns.namespaces {
		entry := fuse.DirEntry{Mode: fuse.S_IFDIR, Name: namespace.name}
		details, _ := out.AddDirLookupEntry(entry)
		if details == nil {
			break
		}

		details.NodeId = namespace.inodeId
		details.Generation = 1
		details.EntryValid = config.cacheTimeSeconds
		details.EntryValidNsec = config.cacheTimeNsecs
		details.AttrValid = config.cacheTimeSeconds
		details.AttrValidNsec = config.cacheTimeNsecs

		details.Attr.Ino = offset - 1
		details.Attr.Size = 4096
		details.Attr.Blocks = 1

		now := time.Now()
		details.Attr.Atime = uint64(now.Unix())
		details.Attr.Atimensec = uint32(now.Nanosecond())
		details.Attr.Mtime = uint64(now.Unix())
		details.Attr.Mtimensec = uint32(now.Nanosecond())

		details.Attr.Ctime = 1
		details.Attr.Ctimensec = 1
		details.Attr.Mode = 0555 | fuse.S_IFDIR

		details.Attr.Nlink = 8
		details.Attr.Owner.Uid = 0
		details.Attr.Owner.Gid = 0
		details.Attr.Blksize = 4096

		toRemove++
	}

	ns.namespaces = ns.namespaces[toRemove:]

	return fuse.OK
}
