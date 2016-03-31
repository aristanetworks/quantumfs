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
	return wsr.OpenDir(flags, mode, out)
}

func (wsr *WorkspaceRoot) OpenDir(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	fmt.Println("Unhandled OpenDir in WorkspaceRoot")
	return fuse.ENOSYS
}

func (wsr *WorkspaceRoot) Lookup(name string, out *fuse.EntryOut) fuse.Status {
	fmt.Println("Unhandled Lookup in WorkspaceRoot")
	return fuse.ENOSYS
}
