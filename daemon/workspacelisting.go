// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// The code which handles listing available workspaces as the first three levels of
// the directory hierarchy.

import (
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

func interpretListingError(c *ctx, err error, cmd string) fuse.Status {
	switch err := err.(type) {
	default:
		c.elog("Unexpected error from %s: %s", cmd, err.Error())
		return fuse.EIO

	case quantumfs.WorkspaceDbErr:
		if err.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND {
			return fuse.ENOENT
		}
		c.elog("Unexpected wsdb error from %s: %s", cmd, err.Error())
		return fuse.EIO
	}
}

func NewTypespaceList() Inode {
	tsl := TypespaceList{
		InodeCommon:      InodeCommon{id: quantumfs.InodeIdRoot},
		typespacesByName: make(map[string]InodeIdInfo),
		typespacesById:   make(map[InodeId]string),
		realTreeState: &TreeState{
			name: "",
		},
	}
	tsl.self = &tsl
	tsl.InodeCommon.treeState_ = tsl.realTreeState
	utils.Assert(tsl.treeState() != nil, "TypespaceList treeState nil at init")
	return &tsl
}

type TypespaceList struct {
	InodeCommon

	// Map from child name to Inode ID
	typespacesByName map[string]InodeIdInfo
	typespacesById   map[InodeId]string

	realTreeState *TreeState
}

func (tsl *TypespaceList) dirty_(c *ctx) {
	// Override InodeCommon.dirty_() because namespaces don't get dirty in the
	// traditional manner
	c.vlog("TypespaceList::dirty_ doing nothing")
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
	out.EntryValid = c.config.CacheTimeSeconds
	out.EntryValidNsec = c.config.CacheTimeNsecs
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
}

func clearEntryOutCacheData(c *ctx, out *fuse.EntryOut) {
	out.EntryValid = 0
	out.EntryValidNsec = 0
	out.AttrValid = 0
	out.AttrValidNsec = 0
}

func fillAttrOutCacheData(c *ctx, out *fuse.AttrOut) {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
}

type inodeRemoval struct {
	id     InodeId
	name   string
	parent InodeId
}

func handleRemovals(c *ctx, removals []inodeRemoval) {
	for _, rem := range removals {
		c.qfs.handleMetaInodeRemoval(c, rem.id, rem.name, rem.parent)
	}
}

// Update the internal namespaces list with the most recent available listing
// Must be called with the inode locks needed to protect inodeMap and nameMap
func updateChildren_(c *ctx, names []string, inodeMap *map[string]InodeIdInfo,
	nameMap *map[InodeId]string, parent Inode) []inodeRemoval {

	defer c.FuncIn("updateChildren_", "Parent Inode %d",
		parent.inodeNum()).Out()

	touched := make(map[string]bool)
	// First add any new entries
	for _, name := range names {
		if _, exists := (*inodeMap)[name]; !exists {
			inodeId := c.qfs.newInodeId()
			c.vlog("Adding new child %s inodeId %d generation %d", name,
				inodeId.id, inodeId.generation)
			(*inodeMap)[name] = inodeId
			(*nameMap)[inodeId.id] = name

			c.qfs.addUninstantiated(c, []inodePair{
				newInodePair(inodeId.id, parent.inodeNum())})
		}
		touched[name] = true
	}

	// Then delete entries which no longer exist
	rtn := make([]inodeRemoval, 0)
	for name, id := range *inodeMap {
		if _, exists := touched[name]; !exists {
			c.vlog("Removing deleted child %s", name)

			// Note: do not uninstantiate them now - remove them
			// from their parents and let the kernel forget them
			// naturally.
			delete(*inodeMap, name)
			delete(*nameMap, id.id)
			rtn = append(rtn, inodeRemoval{
				id:     id.id,
				name:   name,
				parent: parent.inodeNum(),
			})
		}
	}

	return rtn
}

func snapshotChildren(c *ctx, inode Inode, children *map[string]InodeIdInfo,
	typespace string, namespace string, fillChildren listingAttrFill,
	fillMe listingAttrFill, parentInfo directoryContents) []directoryContents {

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
	out = append(out, parentInfo)

	c.vlog("Adding the rest of the %d children", len(*children))
	for name, inode := range *children {
		child := directoryContents{
			filename: name,
			fuseType: fuse.S_IFDIR,
		}

		if typespace == "" {
			fillChildren(c, &child.attr, inode.id, name, "")
		} else {
			fillChildren(c, &child.attr, inode.id, typespace, name)
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

	ds := newDirectorySnapshot(c, tsl, tsl)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (tsl *TypespaceList) foreachDirectInode(c *ctx, visitFn inodeVisitFn) {
	defer tsl.Lock().Unlock()

	for k, _ := range tsl.typespacesById {
		iterateAgain := visitFn(k)
		if !iterateAgain {
			return
		}
	}
}

// Must be called with the inode's parent lock held for reads
func getParentInfo_(c *ctx, parent InodeId, fillParent listingAttrFill,
	typespace string, namespace string) directoryContents {

	rtn := directoryContents{
		filename: "..",
		fuseType: fuse.S_IFDIR,
	}

	fillParent(c, &rtn.attr, parent, typespace, namespace)
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

	removals, children := tsl.getChildSnapshotRemovals(c, typespaces)
	handleRemovals(c, removals)

	return children
}

func (tsl *TypespaceList) getChildSnapshotRemovals(c *ctx,
	typespaces []string) (removed []inodeRemoval, children []directoryContents) {

	var parentInfo directoryContents
	func() {
		defer tsl.getParentLock().RLock().RUnlock()
		parentInfo = getParentInfo_(c, tsl.parentId_(), fillRootAttrWrapper,
			"", "")
	}()

	defer tsl.Lock().Unlock()

	if len(typespaces) > 0 {
		// We only accept positive lists
		removed = updateChildren_(c, typespaces, &tsl.typespacesByName,
			&tsl.typespacesById, tsl)
	}

	// The kernel will override our parent's attributes so it doesn't matter what
	// we put into there.
	children = snapshotChildren(c, tsl, &tsl.typespacesByName, "", "",
		fillTypespaceAttr, fillRootAttrWrapper, parentInfo)

	api := directoryContents{
		filename: quantumfs.ApiPath,
		fuseType: fuse.S_IFREG,
	}
	fillApiAttr(c, &api.attr)
	children = append(children, api)

	return removed, children
}

func (tsl *TypespaceList) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("TypespaceList::Lookup", "name %s", name).Out()

	if name == quantumfs.ApiPath {
		out.NodeId = quantumfs.InodeIdApi
		out.Generation = 1
		fillEntryOutCacheData(c, out)
		fillApiAttr(c, &out.Attr)
		return fuse.OK
	}

	list, err := c.workspaceDB.TypespaceList(&c.Ctx)
	if err != nil {
		return interpretListingError(c, err, "WorkspaceDB.TypespaceList")
	}

	exists := false
	for _, typespace := range list {
		if name == typespace {
			exists = true
			break
		}
	}

	var removed []inodeRemoval
	rtn := func() fuse.Status {
		defer tsl.Lock().Unlock()
		removed = updateChildren_(c, list, &tsl.typespacesByName,
			&tsl.typespacesById, tsl)

		if !exists {
			return fuse.ENOENT
		}

		inodeNum := tsl.typespacesByName[name]
		c.qfs.incrementLookupCount(c, inodeNum.id)
		out.NodeId = uint64(inodeNum.id)
		out.Generation = inodeNum.generation
		fillEntryOutCacheData(c, out)
		fillTypespaceAttr(c, &out.Attr, inodeNum.id, name, "")

		return fuse.OK
	}()
	handleRemovals(c, removed)

	return rtn
}

func (tsl *TypespaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("TypespaceList::Create doing nothing")
	return fuse.EACCES
}

func (tsl *TypespaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.wlog("Invalid SetAttr on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("TypespaceList::Mkdir doing nothing")
	return fuse.EPERM
}

func (tsl *TypespaceList) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	panic("TypespaceList doesn't support record fetch")
}

func (tsl *TypespaceList) Unlink(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Unlink on TypespaceList")
	return fuse.ENOTDIR
}

func (tsl *TypespaceList) Rmdir(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Rmdir on TypespaceList")
	return fuse.EACCES
}

func (tsl *TypespaceList) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Symlink on TypespaceList")
	return fuse.EACCES
}

func (tsl *TypespaceList) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.wlog("Invalid Readlink on TypespaceList")
	return nil, fuse.EINVAL
}

func (tsl *TypespaceList) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Mknod on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on TypespaceList")
	return fuse.ENOSYS
}

func (tsl *TypespaceList) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.vlog("Invalid GetXAttrSize on TypespaceList")
	return 0, fuse.ENODATA
}

func (tsl *TypespaceList) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("Invalid GetXAttrData on TypespaceList")
	return nil, fuse.ENODATA
}

func (tsl *TypespaceList) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("Invalid ListXAttr on TypespaceList")
	return []byte{}, fuse.OK
}

func (tsl *TypespaceList) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("Invalid SetXAttr on TypespaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (tsl *TypespaceList) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("Invalid RemoveXAttr on TypespaceList")
	return fuse.ENODATA
}

func (tsl *TypespaceList) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, hardlinkDelta *HardlinkDelta) {

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

// Must be called with the instantiation lock
func (tsl *TypespaceList) instantiateChild_(c *ctx,
	inodeNum InodeId) Inode {

	defer c.funcIn("TypespaceList::instantiateChild_").Out()
	defer tsl.Lock().Unlock()

	inode, release := c.qfs.inodeNoInstantiate(c, inodeNum)
	// release immediately. We can't hold the mapMutex while we instantiate,
	// but it's okay since the instantiationLock should be held already.
	release()
	if inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeNum)
		return inode
	}

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
	parent Inode, inodeNum InodeId) Inode {

	defer c.FuncIn("newNamespaceList", "typespace %s", typespace).Out()

	nsl := NamespaceList{
		InodeCommon:      InodeCommon{id: inodeNum},
		typespaceName:    typespace,
		namespacesByName: make(map[string]InodeIdInfo),
		namespacesById:   make(map[InodeId]string),
		realTreeState: &TreeState{
			name: typespace,
		},
	}
	nsl.self = &nsl
	nsl.setParent(c, parent)
	nsl.InodeCommon.treeState_ = nsl.realTreeState
	utils.Assert(nsl.treeState() != nil, "NamespaceList treeState nil at init")
	return &nsl
}

type NamespaceList struct {
	InodeCommon
	typespaceName string

	// Map from child name to Inode ID
	namespacesByName map[string]InodeIdInfo
	namespacesById   map[InodeId]string

	realTreeState *TreeState
}

func (nsl *NamespaceList) dirty_(c *ctx) {
	// Override InodeCommon.dirty_() because namespaces don't get dirty in the
	// traditional manner
	c.vlog("NamespaceList::dirty_ doing nothing")
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

	ds := newDirectorySnapshot(c, nsl, nsl)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (nsl *NamespaceList) foreachDirectInode(c *ctx, visitFn inodeVisitFn) {
	defer nsl.Lock().Unlock()

	for k, _ := range nsl.namespacesById {
		iterateAgain := visitFn(k)
		if !iterateAgain {
			return
		}
	}
}

func (nsl *NamespaceList) getChildSnapshot(c *ctx) []directoryContents {
	defer c.funcIn("NamespaceList::getChildSnapshot").Out()

	namespaces, err := c.workspaceDB.NamespaceList(&c.Ctx, nsl.typespaceName)
	if err != nil {
		c.elog("Unexpected error type from WorkspaceDB.NamespaceList: %s",
			err.Error())
		namespaces = []string{}
	}

	removals, children := nsl.getChildSnapshotRemovals(c, namespaces)
	handleRemovals(c, removals)

	return children
}

func (nsl *NamespaceList) getChildSnapshotRemovals(c *ctx,
	namespaces []string) (removed []inodeRemoval, children []directoryContents) {

	var parentInfo directoryContents
	func() {
		defer nsl.getParentLock().RLock().RUnlock()
		parentInfo = getParentInfo_(c, nsl.parentId_(), fillRootAttrWrapper,
			nsl.typespaceName, "")
	}()

	defer nsl.Lock().Unlock()

	if len(namespaces) > 0 {
		// We only accept positive lists
		removed = updateChildren_(c, namespaces, &nsl.namespacesByName,
			&nsl.namespacesById, nsl)
	}

	children = snapshotChildren(c, nsl, &nsl.namespacesByName,
		nsl.typespaceName, "", fillNamespaceAttr, fillTypespaceAttr,
		parentInfo)

	return removed, children
}

func (nsl *NamespaceList) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("NamespaceList::Lookup", "name %s", name).Out()

	list, err := c.workspaceDB.NamespaceList(&c.Ctx, nsl.typespaceName)
	if err != nil {
		return interpretListingError(c, err, "WorkspaceDB.NamespaceList")
	}

	exists := false
	for _, namespace := range list {
		if name == namespace {
			exists = true
			break
		}
	}

	var removed []inodeRemoval
	rtn := func() fuse.Status {
		defer nsl.Lock().Unlock()
		removed = updateChildren_(c, list, &nsl.namespacesByName,
			&nsl.namespacesById, nsl)

		if !exists {
			return fuse.ENOENT
		}
		c.vlog("Namespace exists")

		inodeNum := nsl.namespacesByName[name]
		c.qfs.incrementLookupCount(c, inodeNum.id)
		out.NodeId = uint64(inodeNum.id)
		out.Generation = inodeNum.generation
		fillEntryOutCacheData(c, out)
		fillNamespaceAttr(c, &out.Attr, inodeNum.id, nsl.typespaceName, name)

		return fuse.OK
	}()
	handleRemovals(c, removed)

	return rtn
}

func (nsl *NamespaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("NamespaceList::Create doing nothing")
	return fuse.EACCES
}

func (nsl *NamespaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.wlog("Invalid SetAttr on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("NamespaceList::Mkdir doing nothing")
	return fuse.EPERM
}

func (nsl *NamespaceList) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	panic("NamespaceList doesn't support record fetch")
}

func (nsl *NamespaceList) Unlink(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Unlink on NamespaceList")
	return fuse.ENOTDIR
}

func (nsl *NamespaceList) Rmdir(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Rmdir on NamespaceList")
	return fuse.EACCES
}

func (nsl *NamespaceList) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Symlink on NamespaceList")
	return fuse.EACCES
}

func (nsl *NamespaceList) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.wlog("Invalid Readlink on NamespaceList")
	return nil, fuse.EINVAL
}

func (nsl *NamespaceList) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Mknod on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on NamespaceList")
	return fuse.ENOSYS
}

func (nsl *NamespaceList) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.vlog("Invalid GetXAttrSize on NamespaceList")
	return 0, fuse.ENODATA
}

func (nsl *NamespaceList) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("Invalid GetXAttrData on NamespaceList")
	return nil, fuse.ENODATA
}

func (nsl *NamespaceList) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("Invalid ListXAttr on NamespaceList")
	return []byte{}, fuse.OK
}

func (nsl *NamespaceList) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("Invalid SetXAttr on NamespaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (nsl *NamespaceList) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("Invalid RemoveXAttr on NamespaceList")
	return fuse.ENODATA
}

func (nsl *NamespaceList) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, hardlinkDelta *HardlinkDelta) {

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

// Must be called with the instantiation lock
func (nsl *NamespaceList) instantiateChild_(c *ctx,
	inodeNum InodeId) Inode {

	defer c.funcIn("NamespaceList::instantiateChild_").Out()
	defer nsl.Lock().Unlock()

	inode, release := c.qfs.inodeNoInstantiate(c, inodeNum)
	// release immediately. We can't hold the mapMutex while we instantiate,
	// but it's okay since the instantiationLock should be held already.
	release()
	if inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeNum)
		return inode
	}

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
	parent Inode, inodeNum InodeId) Inode {

	defer c.FuncIn("newWorkspaceList", "typespace %s namespace %s",
		typespace, namespace).Out()

	wsl := WorkspaceList{
		InodeCommon:      InodeCommon{id: inodeNum},
		typespaceName:    typespace,
		namespaceName:    namespace,
		workspacesByName: make(map[string]workspaceInfo),
		workspacesById:   make(map[InodeId]string),
		realTreeState: &TreeState{
			name: typespace + "/" + namespace,
		},
	}
	wsl.self = &wsl
	wsl.setParent(c, parent)
	wsl.InodeCommon.treeState_ = wsl.realTreeState
	utils.Assert(wsl.treeState() != nil, "WorkspaceList treeState nil at init")
	return &wsl
}

type workspaceInfo struct {
	id    InodeIdInfo
	nonce quantumfs.WorkspaceNonce
}

type WorkspaceList struct {
	InodeCommon
	typespaceName string
	namespaceName string

	// Map from child name to Inode ID
	workspacesByName map[string]workspaceInfo
	workspacesById   map[InodeId]string

	realTreeState *TreeState
}

func (wsl *WorkspaceList) dirty_(c *ctx) {
	// Override InodeCommon.dirty_() because workspaces don't get dirty in the
	// traditional manner.
	c.vlog("WorkspaceList::dirty_ doing nothing")
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

	ds := newDirectorySnapshot(c, wsl, wsl)
	c.qfs.setFileHandle(c, ds.FileHandleCommon.id, ds)
	out.Fh = uint64(ds.FileHandleCommon.id)
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE

	return fuse.OK
}

func (wsl *WorkspaceList) foreachDirectInode(c *ctx, visitFn inodeVisitFn) {
	defer wsl.Lock().Unlock()

	for k, _ := range wsl.workspacesById {
		iterateAgain := visitFn(k)
		if !iterateAgain {
			return
		}
	}
}

// Update the internal workspace list with the most recent available listing
// Must be called with the wsl inode lock held
func (wsl *WorkspaceList) updateChildren_(c *ctx,
	names map[string]quantumfs.WorkspaceNonce) []inodeRemoval {

	defer c.FuncIn("WorkspaceList::updateChildren_", "Parent Inode %d",
		wsl.inodeNum()).Out()

	// First delete any outdated entries
	rtn := make([]inodeRemoval, 0)
	for name, info := range wsl.workspacesByName {
		wsdbNonce, exists := names[name]
		if !exists || !wsdbNonce.SameIncarnation(&info.nonce) {
			c.vlog("Removing deleted child %s %s",
				name, info.nonce.String())

			// Note: do not uninstantiate them now - remove them
			// from their parents
			delete(wsl.workspacesByName, name)
			delete(wsl.workspacesById, info.id.id)
			rtn = append(rtn, inodeRemoval{
				id:     info.id.id,
				name:   name,
				parent: wsl.inodeNum(),
			})
		}
	}

	// Then re-add any new entries
	for name, nonce := range names {
		if _, exists := wsl.workspacesByName[name]; !exists {
			c.vlog("Adding new child %s (%s)", name, nonce.String())
			inodeId := c.qfs.newInodeId()
			wsl.workspacesByName[name] = workspaceInfo{
				id:    inodeId,
				nonce: nonce,
			}
			wsl.workspacesById[inodeId.id] = name

			c.qfs.addUninstantiated(c, []inodePair{
				newInodePair(inodeId.id, wsl.inodeNum())})
		}
	}

	return rtn
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

	removals, children := wsl.getChildSnapshotRemovals(c, workspaces)
	handleRemovals(c, removals)

	return children
}

func (wsl *WorkspaceList) getChildSnapshotRemovals(c *ctx,
	workspaces map[string]quantumfs.WorkspaceNonce) (removed []inodeRemoval,
	children []directoryContents) {

	var parentInfo directoryContents
	func() {
		defer wsl.getParentLock().RLock().RUnlock()
		parentInfo = getParentInfo_(c, wsl.parentId_(), fillTypespaceAttr,
			wsl.typespaceName, wsl.namespaceName)
	}()

	defer wsl.Lock().Unlock()

	if len(workspaces) > 0 {
		// We only accept positive lists
		removed = wsl.updateChildren_(c, workspaces)
	}

	namesAndIds := make(map[string]InodeIdInfo, len(wsl.workspacesByName))
	for name, info := range wsl.workspacesByName {
		namesAndIds[name] = info.id
	}
	children = snapshotChildren(c, wsl, &namesAndIds, wsl.typespaceName,
		wsl.namespaceName, fillWorkspaceAttrFake, fillNamespaceAttr,
		parentInfo)

	return removed, children
}

func (wsl *WorkspaceList) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("WorkspaceList::Lookup", "name %s", name).Out()

	workspaces, err := c.workspaceDB.WorkspaceList(&c.Ctx, wsl.typespaceName,
		wsl.namespaceName)
	if err != nil {
		return interpretListingError(c, err, "WorkspaceDB.WorkspaceList")
	}

	exists := false
	for workspace, _ := range workspaces {
		if name == workspace {
			exists = true
			break
		}
	}

	var removed []inodeRemoval
	rtn := func() fuse.Status {
		defer wsl.Lock().Unlock()
		removed = wsl.updateChildren_(c, workspaces)

		if !exists {
			return fuse.ENOENT
		}
		c.vlog("Workspace exists")

		inodeInfo := wsl.workspacesByName[name]
		c.qfs.incrementLookupCount(c, inodeInfo.id.id)
		out.NodeId = uint64(inodeInfo.id.id)
		out.Generation = inodeInfo.id.generation
		fillEntryOutCacheData(c, out)
		fillWorkspaceAttrFake(c, &out.Attr, inodeInfo.id.id, "", "")

		return fuse.OK
	}()
	handleRemovals(c, removed)

	return rtn
}

func (wsl *WorkspaceList) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c.vlog("WorkspaceList::Create doing nothing")
	return fuse.EACCES
}

func (wsl *WorkspaceList) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.wlog("Invalid SetAttr on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("WorkspaceList::Mkdir doing nothing")
	return fuse.EPERM
}

func (wsl *WorkspaceList) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	panic("WorkspaceList doesn't support record fetch")
}

func (wsl *WorkspaceList) Unlink(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Unlink on WorkspaceList")
	return fuse.ENOTDIR
}

func (wsl *WorkspaceList) Rmdir(c *ctx, name string) fuse.Status {
	c.wlog("Invalid Rmdir on WorkspaceList")
	return fuse.EACCES
}

func (wsl *WorkspaceList) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Symlink on WorkspaceList")
	return fuse.EACCES
}

func (wsl *WorkspaceList) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.wlog("Invalid Readlink on WorkspaceList")
	return nil, fuse.EINVAL
}

func (wsl *WorkspaceList) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.wlog("Invalid Mknod on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on WorkspaceList")
	return fuse.ENOSYS
}

func (wsl *WorkspaceList) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	c.vlog("Invalid GetXAttrSize on WorkspaceList")
	return 0, fuse.ENODATA
}

func (wsl *WorkspaceList) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("Invalid GetXAttrData on WorkspaceList")
	return nil, fuse.ENODATA
}

func (wsl *WorkspaceList) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("Invalid ListXAttr on WorkspaceList")
	return []byte{}, fuse.OK
}

func (wsl *WorkspaceList) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("Invalid SetXAttr on WorkspaceList")
	return fuse.Status(syscall.ENOSPC)
}

func (wsl *WorkspaceList) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("Invalid RemoveXAttr on WorkspaceList")
	return fuse.ENODATA
}

func (wsl *WorkspaceList) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey, hardlinkDelta *HardlinkDelta) {

	c.vlog("WorkspaceList::syncChild doing nothing")
}

func (wsl *WorkspaceList) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.vlog("WorkspaceList::setChildAttr doing nothing")
	out.AttrValid = 0
	out.AttrValidNsec = 0
	fillWorkspaceAttrFake(c, &out.Attr, inodeNum, "", "")
	return fuse.OK
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

// Must be called with the instantiation lock
func (wsl *WorkspaceList) instantiateChild_(c *ctx,
	inodeNum InodeId) Inode {

	defer c.funcIn("WorkspaceList::instantiateChild_").Out()
	defer wsl.Lock().Unlock()

	inode, release := c.qfs.inodeNoInstantiate(c, inodeNum)
	// release immediately. We can't hold the mapMutex while we instantiate,
	// but it's okay since the instantiationLock should be held already.
	release()
	if inode != nil {
		c.vlog("Someone has already instantiated inode %d", inodeNum)
		return inode
	}

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
