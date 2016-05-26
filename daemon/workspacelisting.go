// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The code which handles listing available workspaces as the first two levels of the
// directory hierarchy.
package daemon

import "fmt"
import "time"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func NewNamespaceList() Inode {
	nsl := NamespaceList{
		InodeCommon: InodeCommon{id: quantumfs.InodeIdRoot},
		namespaces:  make(map[string]InodeId),
	}
	nsl.self = &nsl
	return &nsl
}

type NamespaceList struct {
	InodeCommon

	// Map from child name to Inode ID
	namespaces map[string]InodeId
}

func (nsl *NamespaceList) dirty(c *ctx) {
}

func (nsl *NamespaceList) dirtyChild(c *ctx, child Inode) {
}

func (nsl *NamespaceList) sync(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}

func (nsl *NamespaceList) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	c.elog("Unsupported Access on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs

	fillRootAttr(c, &out.Attr, nsl.InodeCommon.id)
	return fuse.OK
}

func fillRootAttr(c *ctx, attr *fuse.Attr, inodeNum InodeId) {
	fillAttr(attr, inodeNum,
		uint32(c.workspaceDB.NumNamespaces()))
}

type listingAttrFill func(c *ctx, attr *fuse.Attr, inodeNum InodeId, name string)

func fillNamespaceAttr(c *ctx, attr *fuse.Attr, inodeNum InodeId, namespace string) {
	fillAttr(attr, inodeNum,
		uint32(c.workspaceDB.NumWorkspaces(namespace)))
}

func fillAttr(attr *fuse.Attr, inodeNum InodeId, numChildren uint32) {
	attr.Ino = uint64(inodeNum)
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

func fillEntryOutCacheData(c *ctx, out *fuse.EntryOut) {
	out.Generation = 1
	out.EntryValid = c.config.CacheTimeSeconds
	out.EntryValidNsec = c.config.CacheTimeNsecs
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
}

func fillAttrOutCacheData(c *ctx, out *fuse.AttrOut) {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
}

// Update the internal namespaces list with the most recent available listing
func updateChildren(c *ctx, parentName string, names []string,
	inodeMap *map[string]InodeId, newInode func(c *ctx, parentName string,
		name string, inodeId InodeId) Inode) {

	touched := make(map[string]bool)

	// First add any new entries
	for _, name := range names {
		if _, exists := (*inodeMap)[name]; !exists {
			inodeId := c.qfs.newInodeId()
			(*inodeMap)[name] = inodeId
			c.qfs.setInode(c, inodeId, newInode(c, parentName, name,
				inodeId))
		}
		touched[name] = true
	}

	// Then delete entries which no longer exist
	for name, _ := range *inodeMap {
		if _, exists := touched[name]; !exists {
			c.qfs.setInode(c, (*inodeMap)[name], nil)
			delete(*inodeMap, name)
		}
	}
}

func snapshotChildren(c *ctx, children *map[string]InodeId,
	fillAttr listingAttrFill) []directoryContents {

	out := make([]directoryContents, 0, len(*children))
	for name, inode := range *children {
		child := directoryContents{
			filename: name,
			fuseType: fuse.S_IFDIR,
		}
		fillAttr(c, &child.attr, inode, name)

		out = append(out, child)
	}

	return out
}

func (nsl *NamespaceList) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOSYS
}

func (nsl *NamespaceList) OpenDir(c *ctx, context fuse.Context, flags uint32,
	mode uint32, out *fuse.OpenOut) fuse.Status {

	updateChildren(c, "/", c.workspaceDB.NamespaceList(), &nsl.namespaces,
		newWorkspaceList)
	children := snapshotChildren(c, &nsl.namespaces, fillNamespaceAttr)

	api := directoryContents{
		filename: quantumfs.ApiPath,
		fuseType: fuse.S_IFREG,
	}
	fillApiAttr(&api.attr)
	children = append(children, api)

	ds := newDirectorySnapshot(c, children, nsl.InodeCommon.id)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (nsl *NamespaceList) Lookup(c *ctx, context fuse.Context, name string,
	out *fuse.EntryOut) fuse.Status {

	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		fillEntryOutCacheData(c, out)
		fillApiAttr(&out.Attr)
		return fuse.OK
	}

	if !c.workspaceDB.NamespaceExists(name) {
		return fuse.ENOENT
	}

	updateChildren(c, "/", c.workspaceDB.NamespaceList(), &nsl.namespaces,
		newWorkspaceList)

	inodeNum := nsl.namespaces[name]
	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillNamespaceAttr(c, &out.Attr, inodeNum, name)

	return fuse.OK
}

func (nsl *NamespaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	return fuse.EACCES
}

func (nsl *NamespaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	fmt.Println("Invalid SetAttr on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (nsl *NamespaceList) Unlink(c *ctx, name string) fuse.Status {

	return fuse.ENOTDIR
}

func (nsl *NamespaceList) setChildAttr(c *ctx, inodeNum InodeId,
	attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {

	fmt.Println("Invalid setChildAttr on NamespaceList")
	return fuse.ENOSYS
}

func newWorkspaceList(c *ctx, parentName string, name string,
	inodeNum InodeId) Inode {

	wsl := WorkspaceList{
		InodeCommon:   InodeCommon{id: inodeNum},
		namespaceName: name,
		workspaces:    make(map[string]InodeId),
	}
	wsl.self = &wsl
	return &wsl
}

type WorkspaceList struct {
	InodeCommon
	namespaceName string

	// Map from child name to Inode ID
	workspaces map[string]InodeId
}

func (wsl *WorkspaceList) dirty(c *ctx) {
}

func (wsl *WorkspaceList) dirtyChild(c *ctx, child Inode) {
}

func (wsl *WorkspaceList) sync(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}

func (wsl *WorkspaceList) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	c.elog("Unsupported Access on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs

	fillRootAttr(c, &out.Attr, wsl.InodeCommon.id)
	return fuse.OK
}

func (wsl *WorkspaceList) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOSYS
}

func (wsl *WorkspaceList) OpenDir(c *ctx, context fuse.Context, flags uint32,
	mode uint32, out *fuse.OpenOut) fuse.Status {

	updateChildren(c, wsl.namespaceName,
		c.workspaceDB.WorkspaceList(wsl.namespaceName), &wsl.workspaces,
		newWorkspaceRoot)
	children := snapshotChildren(c, &wsl.workspaces, fillWorkspaceAttrFake)

	ds := newDirectorySnapshot(c, children, wsl.InodeCommon.id)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = 0

	return fuse.OK
}

func (wsl *WorkspaceList) Lookup(c *ctx, context fuse.Context, name string,
	out *fuse.EntryOut) fuse.Status {

	if !c.workspaceDB.WorkspaceExists(wsl.namespaceName, name) {
		return fuse.ENOENT
	}

	updateChildren(c, wsl.namespaceName,
		c.workspaceDB.WorkspaceList(wsl.namespaceName), &wsl.workspaces,
		newWorkspaceRoot)

	inodeNum := wsl.workspaces[name]
	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillWorkspaceAttrFake(c, &out.Attr, inodeNum, name)

	return fuse.OK
}

func (wsl *WorkspaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	return fuse.EACCES
}

func (wsl *WorkspaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	fmt.Println("Invalid SetAttr on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.EPERM
}

func (wsl *WorkspaceList) Unlink(c *ctx, name string) fuse.Status {

	return fuse.ENOTDIR
}

func (wsl *WorkspaceList) setChildAttr(c *ctx, inodeNum InodeId,
	attr *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {

	fmt.Println("Invalid setChildAttr on WorkspaceList")
	return fuse.ENOSYS
}
