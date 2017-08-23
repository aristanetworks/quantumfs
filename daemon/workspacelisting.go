// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// The code which handles listing available workspaces as the first three levels of
// the directory hierarchy.

import (
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// Arbitrary nlinks for directories if we fail to retrieve the real number from the
// WorkspaceDB. 2 are for . and ..
//
// Choose five to represent 3 typespaces.
const estimatedTypespaceNum = 5

// Choose three to represent 1 namespace
const minimumNamespaceNum = 3

// Choose three to represent 1 workspace
const minimumWorkspaceNum = 3

func NewTypespaceList() Inode {
	tsl := TypespaceList{
		InodeCommon:      InodeCommon{id: quantumfs.InodeIdRoot},
		typespacesByName: make(map[string]InodeId),
		typespacesById:   make(map[InodeId]string),
	}
	tsl.self = &tsl
	treeLock := TreeLock{lock: &tsl.realTreeLock, name: ""}
	tsl.InodeCommon.treeLock_ = &treeLock
	utils.Assert(tsl.treeLock() != nil, "TypespaceList treeLock nil at init")
	return &tsl
}

type TypespaceList struct {
	InodeCommon

	// Map from child name to Inode ID
	typespacesByName map[string]InodeId
	typespacesById   map[InodeId]string

	realTreeLock sync.RWMutex
}

func (tsl *TypespaceList) dirty(c *ctx) {
	// Override InodeCommon.dirty() because namespaces don't get dirty in the
	// traditional manner
	c.vlog("TypespaceList::dirty doing nothing")
}

func (tsl *TypespaceList) dirtyChild(c *ctx, child InodeId) {
	c.vlog("TypespaceList::dirtyChild doing nothing")
}

func (tsl *TypespaceList) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	return fuse.OK
}

func (tsl *TypespaceList) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("TypespaceList::GetAttr").Out()
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs

	fillRootAttr(c, &out.Attr, tsl.InodeCommon.id)
	return fuse.OK
}

func (tsl *TypespaceList) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	defer c.FuncIn("TypespaceList::markSelfAccessed", "CRUD %x", op).Out()
	tsl.markAccessed(c, "", op)
	return
}

func (tsl *TypespaceList) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {
	c.elog("Invalid markAccessed on TypespaceList")
	return
}

func fillRootAttrWrapper(c *ctx, attr *fuse.Attr, inodeNum InodeId, _ string,
	_ string) {

	fillRootAttr(c, attr, inodeNum)
}

func fillRootAttr(c *ctx, attr *fuse.Attr, inodeNum InodeId) {
	defer c.FuncIn("fillRootAttr", "inode %d", inodeNum).Out()
	num, err := c.workspaceDB.NumTypespaces(&c.Ctx)
	if err != nil {
		c.elog("Error fetching number of typespaces: %s", err.Error())
		num = estimatedTypespaceNum
	}

	c.vlog("/ has %d children", num)

	fillAttr(attr, inodeNum, uint32(num))
}

type listingAttrFill func(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	typespace string, namespace string)

func fillTypespaceAttr(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	typespace string, namespace string) {

	defer c.FuncIn("fillTypespaceAttr", "inode %d", inodeNum).Out()

	num, err := c.workspaceDB.NumNamespaces(&c.Ctx, typespace)
	if err != nil {
		c.elog("Error fetching number of namespaces in %s: %s", typespace,
			err.Error())
		num = minimumNamespaceNum
	}

	c.vlog("%s/%s has %d children", typespace, namespace, num)

	fillAttr(attr, inodeNum, uint32(num))
}

func fillNamespaceAttr(c *ctx, attr *fuse.Attr, inodeNum InodeId,
	typespace string, namespace string) {

	defer c.FuncIn("fillNamespaceAttr", "inode %d", inodeNum).Out()

	num, err := c.workspaceDB.NumWorkspaces(&c.Ctx, typespace, namespace)
	if err != nil {
		c.elog("Error fetching number of workspace in %s/%s: %s", typespace,
			namespace, err.Error())
		num = minimumWorkspaceNum
	}

	c.vlog("%s/%s has %d children", typespace, namespace, num)

	fillAttr(attr, inodeNum, uint32(num))
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
func updateChildren(c *ctx, names []string, inodeMap *map[string]InodeId,
	nameMap *map[InodeId]string, parent Inode) {

	defer c.FuncIn("updateChildren", "Parent Inode %d",
		parent.inodeNum()).Out()

	touched := make(map[string]bool)
	// First add any new entries
	for _, name := range names {
		if _, exists := (*inodeMap)[name]; !exists {
			c.vlog("Adding new child %s", name)
			inodeId := c.qfs.newInodeId()
			(*inodeMap)[name] = inodeId
			(*nameMap)[inodeId] = name

			c.qfs.addUninstantiated(c, []InodeId{inodeId},
				parent.inodeNum())
		}
		touched[name] = true
	}

	// Then delete entries which no longer exist
	for name, id := range *inodeMap {
		if _, exists := touched[name]; !exists {
			c.vlog("Removing deleted child %s", name)

			// Note: do not uninstantiate them now - remove them
			// from their parents and let the kernel forget them
			// naturally.
			delete(*inodeMap, name)
			delete(*nameMap, id)
		}
	}
}

func snapshotChildren(c *ctx, inode Inode, children *map[string]InodeId,
	typespace string, namespace string, fillChildren listingAttrFill,
	fillMe listingAttrFill, fillParent listingAttrFill) []directoryContents {

	defer c.FuncIn("snapshotChildren", "typespace %s namespace %s", typespace,
		namespace).Out()

	out := make([]directoryContents, 0, len(*children)+2)

	c.vlog("Adding .")
	child := directoryContents{
		filename: ".",
		fuseType: fuse.S_IFDIR,
	}
	fillMe(c, &child.attr, inode.inodeNum(), typespace, namespace)
	out = append(out, child)

	c.vlog("Adding ..")
	child = directoryContents{
		filename: "..",
		fuseType: fuse.S_IFDIR,
	}

	func() {
		defer inode.getParentLock().RLock().RUnlock()
		parentId := inode.parentId_()
		fillParent(c, &child.attr, parentId, typespace, namespace)
		out = append(out, child)
	}()

	c.vlog("Adding the rest of the %d children", len(*children))
	for name, inode := range *children {
		child := directoryContents{
			filename: name,
			fuseType: fuse.S_IFDIR,
		}

		if typespace == "" {
			fillChildren(c, &child.attr, inode, name, "")
		} else {
			fillChildren(c, &child.attr, inode, typespace, name)
		}

		out = append(out, child)
	}

	return out
}

func (tsl *TypespaceList) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("TypespaceList::Open doing nothing")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) OpenDir(c *ctx, flags uint32,
	mode uint32, out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("TypespaceList::OpenDir").Out()

	ds := newDirectorySnapshot(c, tsl)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (tsl *TypespaceList) directChildInodes() []InodeId {
	defer tsl.Lock().Unlock()

	rtn := make([]InodeId, 0, len(tsl.typespacesById))
	for k, _ := range tsl.typespacesById {
		rtn = append(rtn, k)
	}

	return rtn
}

func (tsl *TypespaceList) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("TypespaceList::getChildSnapshot").Out()

	typespaces, err := c.workspaceDB.TypespaceList(&c.Ctx)
	if err != nil {
		c.elog("Unexpected error from WorkspaceDB.TypespaceList: %s",
			err.Error())
		typespaces = []string{}
	}

	defer tsl.Lock().Unlock()

	if err == nil {
		// We only accept positive lists
		updateChildren(c, typespaces, &tsl.typespacesByName,
			&tsl.typespacesById, tsl)
	}

	// The kernel will override our parent's attributes so it doesn't matter what
	// we put into there.
	children := snapshotChildren(c, tsl, &tsl.typespacesByName, "", "",
		fillTypespaceAttr, fillRootAttrWrapper, fillRootAttrWrapper)

	api := directoryContents{
		filename: quantumfs.ApiPath,
		fuseType: fuse.S_IFREG,
	}
	fillApiAttr(c, &api.attr)
	children = append(children, api)

	return children
}

func (tsl *TypespaceList) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("TypespaceList::Lookup", "name %s", name).Out()

	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		fillEntryOutCacheData(c, out)
		fillApiAttr(c, &out.Attr)
		return fuse.OK
	}

	list, err := c.workspaceDB.TypespaceList(&c.Ctx)
	if err != nil {
		c.elog("Unexpected error from WorkspaceDB.TypespaceList: %s",
			err.Error())
		return fuse.EIO
	}

	exists := false
	for _, typespace := range list {
		if name == typespace {
			exists = true
			break
		}
	}
	if !exists {
		return fuse.ENOENT
	}

	c.vlog("Typespace exists")
	defer tsl.Lock().Unlock()

	updateChildren(c, list, &tsl.typespacesByName, &tsl.typespacesById, tsl)

	inodeNum := tsl.typespacesByName[name]
	c.qfs.increaseLookupCount(c, inodeNum)
	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillTypespaceAttr(c, &out.Attr, inodeNum, name, "")

	return fuse.OK
}

func (tsl *TypespaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("TypespaceList::Create doing nothing")
	return fuse.EACCES
}

func (tsl *TypespaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid SetAttr on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("TypespaceList::Mkdir doing nothing")
	return fuse.EPERM
}

func (tsl *TypespaceList) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	c.elog("Unsupported record fetch on TypespaceList")
	return &quantumfs.DirectRecord{},
		errors.New("Unsupported record fetch on TypespaceList")
}

func (tsl *TypespaceList) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on TypespaceList")
	return fuse.ENOTDIR
}

func (tsl *TypespaceList) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on TypespaceList")
	return fuse.EACCES
}

func (tsl *TypespaceList) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on TypespaceList")
	return fuse.EACCES
}

func (tsl *TypespaceList) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on TypespaceList")
	return nil, fuse.EINVAL
}

func (tsl *TypespaceList) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid GetXAttrSize on TypespaceList")
	return 0, fuse.ENODATA
}

func (tsl *TypespaceList) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid GetXAttrData on TypespaceList")
	return nil, fuse.ENODATA
}

func (tsl *TypespaceList) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.elog("Invalid ListXAttr on TypespaceList")
	return []byte{}, fuse.OK
}

func (tsl *TypespaceList) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.elog("Invalid SetXAttr on TypespaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (tsl *TypespaceList) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.elog("Invalid RemoveXAttr on TypespaceList")
	return fuse.ENODATA
}

func (tsl *TypespaceList) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {
	c.vlog("TypespaceList::syncChild doing nothing")
}

func (tsl *TypespaceList) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on TypespaceList")
	return 0, fuse.ENODATA
}

func (tsl *TypespaceList) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on TypespaceList")
	return nil, fuse.ENODATA
}

func (tsl *TypespaceList) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on TypespaceList")
	return []byte{}, fuse.OK
}

func (tsl *TypespaceList) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on TypespaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (tsl *TypespaceList) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on TypespaceList")
	return fuse.ENODATA
}

func (tsl *TypespaceList) instantiateChild(c *ctx,
	inodeNum InodeId) (Inode, []InodeId) {

	defer c.funcIn("TypespaceList::instantiateChild").Out()
	defer tsl.Lock().Unlock()

	// The api file will never be truly forgotten (see QuantumFs.Forget()) and so
	// doesn't need to ever be re-instantiated.

	name, exists := tsl.typespacesById[inodeNum]
	if exists {
		c.vlog("Instantiating %d -> %s", inodeNum, name)
	} else {
		c.vlog("inode %d doesn't exist", inodeNum)
	}

	return newNamespaceList(c, name, "", "", tsl, inodeNum)
}

func (tsl *TypespaceList) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("TypespaceList::flush").Out()
	return quantumfs.EmptyBlockKey
}

func newNamespaceList(c *ctx, typespace string, namespace string, workspace string,
	parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("newNamespaceList", "typespace %s", typespace).Out()

	nsl := NamespaceList{
		InodeCommon:      InodeCommon{id: inodeNum},
		typespaceName:    typespace,
		namespacesByName: make(map[string]InodeId),
		namespacesById:   make(map[InodeId]string),
	}
	nsl.self = &nsl
	nsl.setParent(parent.inodeNum())
	treeLock := TreeLock{lock: &nsl.realTreeLock, name: typespace}
	nsl.InodeCommon.treeLock_ = &treeLock
	utils.Assert(nsl.treeLock() != nil, "NamespaceList treeLock nil at init")
	return &nsl, nil
}

type NamespaceList struct {
	InodeCommon
	typespaceName string

	// Map from child name to Inode ID
	namespacesByName map[string]InodeId
	namespacesById   map[InodeId]string

	realTreeLock sync.RWMutex
}

func (nsl *NamespaceList) dirty(c *ctx) {
	// Override InodeCommon.dirty() because namespaces don't get dirty in the
	// traditional manner
	c.vlog("NamespaceList::dirty doing nothing")
}

func (nsl *NamespaceList) dirtyChild(c *ctx, child InodeId) {
	c.vlog("NamespaceList::dirtyChild doing nothing")
}

func (nsl *NamespaceList) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	return fuse.OK
}

func (nsl *NamespaceList) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("NamespaceList::GetAttr").Out()
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs

	fillTypespaceAttr(c, &out.Attr, nsl.InodeCommon.id, nsl.typespaceName, "")
	return fuse.OK
}

func (nsl *NamespaceList) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("NamespaceList::Open doing nothing")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) OpenDir(c *ctx, flags uint32,
	mode uint32, out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("NamespaceList::OpenDir").Out()

	ds := newDirectorySnapshot(c, nsl)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (nsl *NamespaceList) directChildInodes() []InodeId {
	defer nsl.Lock().Unlock()

	rtn := make([]InodeId, 0, len(nsl.namespacesById))
	for k, _ := range nsl.namespacesById {
		rtn = append(rtn, k)
	}

	return rtn
}

func (nsl *NamespaceList) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("NamespaceList::getChildSnapshot").Out()

	namespaces, err := c.workspaceDB.NamespaceList(&c.Ctx, nsl.typespaceName)
	if err != nil {
		c.elog("Unexpected error type from WorkspaceDB.NamespaceList: %s",
			err.Error())
		namespaces = []string{}
	}

	defer nsl.Lock().Unlock()

	if err == nil {
		// We only accept positive lists
		updateChildren(c, namespaces, &nsl.namespacesByName,
			&nsl.namespacesById, nsl)
	}

	children := snapshotChildren(c, nsl, &nsl.namespacesByName,
		nsl.typespaceName, "", fillNamespaceAttr, fillTypespaceAttr,
		fillRootAttrWrapper)

	return children
}

func (nsl *NamespaceList) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("NamespaceList::Lookup", "name %s", name).Out()

	list, err := c.workspaceDB.NamespaceList(&c.Ctx, nsl.typespaceName)
	if err != nil {
		c.elog("Unexpected error from WorkspaceDB.NamespaceList: %s",
			err.Error())
		return fuse.EIO
	}

	exists := false
	for _, namespace := range list {
		if name == namespace {
			exists = true
			break
		}
	}
	if !exists {
		return fuse.ENOENT
	}

	c.vlog("Namespace exists")
	defer nsl.Lock().Unlock()

	updateChildren(c, list, &nsl.namespacesByName, &nsl.namespacesById, nsl)

	inodeNum := nsl.namespacesByName[name]
	c.qfs.increaseLookupCount(c, inodeNum)
	out.NodeId = uint64(inodeNum)
	fillEntryOutCacheData(c, out)
	fillNamespaceAttr(c, &out.Attr, inodeNum, nsl.typespaceName, name)

	return fuse.OK
}

func (nsl *NamespaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("NamespaceList::Create doing nothing")
	return fuse.EACCES
}

func (nsl *NamespaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid SetAttr on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("NamespaceList::Mkdir doing nothing")
	return fuse.EPERM
}

func (nsl *NamespaceList) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	c.elog("Unsupported record fetch on NamespaceList")
	return &quantumfs.DirectRecord{},
		errors.New("Unsupported record fetch on NamespaceList")
}

func (nsl *NamespaceList) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on NamespaceList")
	return fuse.ENOTDIR
}

func (nsl *NamespaceList) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on NamespaceList")
	return fuse.EACCES
}

func (nsl *NamespaceList) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on NamespaceList")
	return fuse.EACCES
}

func (nsl *NamespaceList) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on NamespaceList")
	return nil, fuse.EINVAL
}

func (nsl *NamespaceList) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid GetXAttrSize on NamespaceList")
	return 0, fuse.ENODATA
}

func (nsl *NamespaceList) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid GetXAttrData on NamespaceList")
	return nil, fuse.ENODATA
}

func (nsl *NamespaceList) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.elog("Invalid ListXAttr on NamespaceList")
	return []byte{}, fuse.OK
}

func (nsl *NamespaceList) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.elog("Invalid SetXAttr on NamespaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (nsl *NamespaceList) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.elog("Invalid RemoveXAttr on NamespaceList")
	return fuse.ENODATA
}

func (nsl *NamespaceList) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("NamespaceList::syncChild doing nothing")
}

func (nsl *NamespaceList) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on NamespaceList")
	return 0, fuse.ENODATA
}

func (nsl *NamespaceList) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on NamespaceList")
	return nil, fuse.ENODATA
}

func (nsl *NamespaceList) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on NamespaceList")
	return []byte{}, fuse.OK
}

func (nsl *NamespaceList) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on NamespaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (nsl *NamespaceList) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on NamespaceList")
	return fuse.ENODATA
}

func (nsl *NamespaceList) instantiateChild(c *ctx,
	inodeNum InodeId) (Inode, []InodeId) {

	defer c.funcIn("NamespaceList::instantiateChild").Out()
	defer nsl.Lock().Unlock()

	name, exists := nsl.namespacesById[inodeNum]
	if exists {
		c.vlog("Instantiating %d -> %s/%s", inodeNum, nsl.typespaceName,
			name)
	} else {
		c.vlog("inode %d doesn't exist", inodeNum)
	}

	return newWorkspaceList(c, nsl.typespaceName, name, nsl, inodeNum)
}

func (nsl *NamespaceList) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	defer c.FuncIn("NamespaceList::markSelfAccessed", "CRUD %x", op).Out()
	nsl.markAccessed(c, "", op)
	return
}

func (nsl *NamespaceList) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {
	c.elog("Invalid markAccessed on NamespaceList")
	return
}

func (nsl *NamespaceList) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("NamespaceList::flush").Out()
	return quantumfs.EmptyBlockKey
}

func newWorkspaceList(c *ctx, typespace string, namespace string,
	parent Inode, inodeNum InodeId) (Inode, []InodeId) {

	defer c.FuncIn("newWorkspaceList", "typespace %s namespace %s",
		typespace, namespace).Out()

	wsl := WorkspaceList{
		InodeCommon:      InodeCommon{id: inodeNum},
		typespaceName:    typespace,
		namespaceName:    namespace,
		workspacesByName: make(map[string]workspaceInfo),
		workspacesById:   make(map[InodeId]string),
	}
	wsl.self = &wsl
	wsl.setParent(parent.inodeNum())
	treeLock := TreeLock{lock: &wsl.realTreeLock,
		name: typespace + "/" + namespace}
	wsl.InodeCommon.treeLock_ = &treeLock
	utils.Assert(wsl.treeLock() != nil, "WorkspaceList treeLock nil at init")
	return &wsl, nil
}

type workspaceInfo struct {
	id    InodeId
	nonce quantumfs.WorkspaceNonce
}

type WorkspaceList struct {
	InodeCommon
	typespaceName string
	namespaceName string

	// Map from child name to Inode ID
	workspacesByName map[string]workspaceInfo
	workspacesById   map[InodeId]string

	realTreeLock sync.RWMutex
}

func (wsl *WorkspaceList) dirty(c *ctx) {
	// Override InodeCommon.dirty() because workspaces don't get dirty in the
	// traditional manner.
	c.vlog("WorkspaceList::dirty doing nothing")
}

func (wsl *WorkspaceList) dirtyChild(c *ctx, child InodeId) {
	c.vlog("WorkspaceList::dirtyChild doing nothing")
}

func (wsl *WorkspaceList) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	return fuse.OK
}

func (wsl *WorkspaceList) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	defer c.funcIn("WorkspaceList::GetAttr").Out()
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs

	fillNamespaceAttr(c, &out.Attr, wsl.InodeCommon.id, wsl.typespaceName,
		wsl.namespaceName)
	return fuse.OK
}

func (wsl *WorkspaceList) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("WorkspaceList::Open doing nothing")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) OpenDir(c *ctx, flags uint32,
	mode uint32, out *fuse.OpenOut) fuse.Status {

	defer c.funcIn("WorkspaceList::OpenDir").Out()

	ds := newDirectorySnapshot(c, wsl)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (wsl *WorkspaceList) directChildInodes() []InodeId {
	defer wsl.Lock().Unlock()

	rtn := make([]InodeId, 0, len(wsl.workspacesById))
	for k, _ := range wsl.workspacesById {
		rtn = append(rtn, k)
	}

	return rtn
}

// Update the internal workspace list with the most recent available listing
func (wsl *WorkspaceList) updateChildren(c *ctx,
	names map[string]quantumfs.WorkspaceNonce) {

	defer c.FuncIn("WorkspaceList::updateChildren", "Parent Inode %d",
		wsl.inodeNum()).Out()

	// First delete any outdated entries
	for name, info := range wsl.workspacesByName {
		wsdbNonce, exists := names[name]
		if !exists || wsdbNonce != info.nonce {
			c.vlog("Removing deleted child %s (%d)", name,
				info.nonce)

			// Note: do not uninstantiate them now - remove them
			// from their parents and let the kernel forget them
			// naturally.
			delete(wsl.workspacesByName, name)
			delete(wsl.workspacesById, info.id)
		}
	}

	// Then re-add any new entries
	for name, nonce := range names {
		if _, exists := wsl.workspacesByName[name]; !exists {
			c.vlog("Adding new child %s (%d)", name, nonce)
			inodeId := c.qfs.newInodeId()
			wsl.workspacesByName[name] = workspaceInfo{
				id:    inodeId,
				nonce: nonce,
			}
			wsl.workspacesById[inodeId] = name

			c.qfs.addUninstantiated(c, []InodeId{inodeId},
				wsl.inodeNum())
		}
	}

}

func (wsl *WorkspaceList) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("WorkspaceList::getChildSnapshot").Out()

	workspaces, err := c.workspaceDB.WorkspaceList(&c.Ctx, wsl.typespaceName,
		wsl.namespaceName)
	if err != nil {
		c.elog("Unexpected error type from WorkspaceDB.WorkspaceList: %s",
			err.Error())
		workspaces = map[string]quantumfs.WorkspaceNonce{}
	}

	defer wsl.Lock().Unlock()

	if err == nil {
		// We only accept positive lists
		wsl.updateChildren(c, workspaces)
	}

	namesAndIds := make(map[string]InodeId, len(wsl.workspacesByName))
	for name, info := range wsl.workspacesByName {
		namesAndIds[name] = info.id
	}
	children := snapshotChildren(c, wsl, &namesAndIds, wsl.typespaceName,
		wsl.namespaceName, fillWorkspaceAttrFake, fillNamespaceAttr,
		fillTypespaceAttr)

	return children
}

func (wsl *WorkspaceList) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("WorkspaceList::Lookup", "name %s", name).Out()

	workspaces, err := c.workspaceDB.WorkspaceList(&c.Ctx, wsl.typespaceName,
		wsl.namespaceName)
	if err != nil {
		c.elog("Unexpected error from WorkspaceDB.WorkspaceList: %s",
			err.Error())
		return fuse.EIO
	}

	exists := false
	for workspace, _ := range workspaces {
		if name == workspace {
			exists = true
			break
		}
	}
	if !exists {
		return fuse.ENOENT
	}

	c.vlog("Workspace exists")
	defer wsl.Lock().Unlock()

	wsl.updateChildren(c, workspaces)

	inodeInfo := wsl.workspacesByName[name]
	c.qfs.increaseLookupCount(c, inodeInfo.id)
	out.NodeId = uint64(inodeInfo.id)
	fillEntryOutCacheData(c, out)
	fillWorkspaceAttrFake(c, &out.Attr, inodeInfo.id, "", "")

	return fuse.OK
}

func (wsl *WorkspaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("WorkspaceList::Create doing nothing")
	return fuse.EACCES
}

func (wsl *WorkspaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid SetAttr on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("WorkspaceList::Mkdir doing nothing")
	return fuse.EPERM
}

func (wsl *WorkspaceList) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.ImmutableDirectoryRecord, error) {

	c.elog("Unsupported record fetch on WorkspaceList")
	return &quantumfs.DirectRecord{},
		errors.New("Unsupported record fetch on WorkspaceList")
}

func (wsl *WorkspaceList) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on WorkspaceList")
	return fuse.ENOTDIR
}

func (wsl *WorkspaceList) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on WorkspaceList")
	return fuse.EACCES
}

func (wsl *WorkspaceList) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on WorkspaceList")
	return fuse.EACCES
}

func (wsl *WorkspaceList) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on WorkspaceList")
	return nil, fuse.EINVAL
}

func (wsl *WorkspaceList) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid GetXAttrSize on WorkspaceList")
	return 0, fuse.ENODATA
}

func (wsl *WorkspaceList) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid GetXAttrData on WorkspaceList")
	return nil, fuse.ENODATA
}

func (wsl *WorkspaceList) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.elog("Invalid ListXAttr on WorkspaceList")
	return []byte{}, fuse.OK
}

func (wsl *WorkspaceList) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.elog("Invalid SetXAttr on WorkspaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (wsl *WorkspaceList) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.elog("Invalid RemoveXAttr on WorkspaceList")
	return fuse.ENODATA
}

func (wsl *WorkspaceList) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.vlog("WorkspaceList::syncChild doing nothing")
}

func (wsl *WorkspaceList) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on WorkspaceList")
	return 0, fuse.ENODATA
}

func (wsl *WorkspaceList) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on WorkspaceList")
	return nil, fuse.ENODATA
}

func (wsl *WorkspaceList) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on WorkspaceList")
	return []byte{}, fuse.OK
}

func (wsl *WorkspaceList) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on WorkspaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (wsl *WorkspaceList) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on WorkspaceList")
	return fuse.ENODATA
}

func (wsl *WorkspaceList) instantiateChild(c *ctx,
	inodeNum InodeId) (Inode, []InodeId) {

	defer c.funcIn("WorkspaceList::instantiateChild").Out()
	defer wsl.Lock().Unlock()

	name, exists := wsl.workspacesById[inodeNum]
	if exists {
		c.vlog("Instantiating %d -> %s/%s/%s", inodeNum, wsl.typespaceName,
			wsl.namespaceName, name)
	} else {
		c.vlog("inode %d doesn't exist", inodeNum)
	}

	return newWorkspaceRoot(c, wsl.typespaceName, wsl.namespaceName,
		wsl.workspacesById[inodeNum], wsl, inodeNum)
}

func (wsl *WorkspaceList) markSelfAccessed(c *ctx, op quantumfs.PathFlags) {
	defer c.FuncIn("WorkspaceList::markSelfAccessed", "CRUD %x", op).Out()
	wsl.markAccessed(c, "", op)
	return
}

func (wsl *WorkspaceList) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {
	c.elog("Invalid markAccessed on WorkspaceList")
	return
}
func (wsl *WorkspaceList) flush(c *ctx) quantumfs.ObjectKey {
	defer c.funcIn("WorkspaceList::flush").Out()
	return quantumfs.EmptyBlockKey
}
