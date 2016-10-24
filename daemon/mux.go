// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// go-fuse creates a goroutine for every request. The code here simply takes these
// requests and forwards them to the correct Inode.
package daemon

import "fmt"
import "math"
import "runtime/debug"
import "syscall"
import "sync"
import "sync/atomic"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/hanwen/go-fuse/fuse"

func NewQuantumFs_(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	qfs := &QuantumFs{
		RawFileSystem:        fuse.NewDefaultRawFileSystem(),
		config:               config,
		inodes:               make(map[InodeId]Inode),
		fileHandles:          make(map[FileHandleId]FileHandle),
		inodeNum:             quantumfs.InodeIdReservedEnd,
		fileHandleNum:        quantumfs.InodeIdReservedEnd,
		activeWorkspaces:     make(map[string]*WorkspaceRoot),
		uninstantiatedInodes: make(map[InodeId]Inode),
		c: ctx{
			Ctx: quantumfs.Ctx{
				Qlog:      qlogIn,
				RequestId: qlog.MuxReqId,
			},
			config:      &config,
			workspaceDB: config.WorkspaceDB,
			dataStore:   newDataStore(config.DurableStore),
		},
		allowForget: true,
	}

	qfs.c.qfs = qfs

	namespaceList := NewNamespaceList()
	qfs.inodes[quantumfs.InodeIdRoot] = namespaceList
	qfs.inodes[quantumfs.InodeIdApi] = NewApiInode(namespaceList.treeLock())
	return qfs
}

func NewQuantumFsLogs(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	return NewQuantumFs_(config, qlogIn)
}

func NewQuantumFs(config QuantumFsConfig) *QuantumFs {
	return NewQuantumFs_(config, qlog.NewQlogExt(config.CachePath,
		config.MemLogBytes, qlog.PrintToStdout))
}

type QuantumFs struct {
	fuse.RawFileSystem
	server        *fuse.Server
	config        QuantumFsConfig
	inodeNum      uint64
	fileHandleNum uint64
	c             ctx
	allowForget   bool

	mapMutex         sync.RWMutex
	inodes           map[InodeId]Inode
	fileHandles      map[FileHandleId]FileHandle
	activeWorkspaces map[string]*WorkspaceRoot

	// Uninstantiated Inodes are inode numbers which have been reserved for a
	// particular inode, but the corresponding Inode has not yet been
	// instantiated. The Inode this map points to is the parent Inode which
	// should be called to instantiate the uninstantiated inode when necessary.
	uninstantiatedInodes map[InodeId]Inode
}

func (qfs *QuantumFs) Serve(mountOptions fuse.MountOptions) error {
	qfs.c.dlog("QuantumFs::Serve Initializing server")
	server, err := fuse.NewServer(qfs, qfs.config.MountPath, &mountOptions)
	if err != nil {
		return err
	}

	stopFlushTimer := make(chan bool)
	flushTimerStopped := make(chan bool)

	go qfs.flushTimer(stopFlushTimer, flushTimerStopped)

	qfs.server = server
	qfs.c.dlog("QuantumFs::Serve Serving")
	qfs.server.Serve()
	qfs.c.dlog("QuantumFs::Serve Finished serving")

	qfs.c.dlog("QuantumFs::Serve Waiting for flush thread to end")
	stopFlushTimer <- true
	<-flushTimerStopped

	func() {
		defer logRequestPanic(&qfs.c)
		qfs.syncAll(&qfs.c)
	}()

	return nil
}

func (qfs *QuantumFs) flushTimer(quit chan bool, finished chan bool) {
	c := qfs.c.reqId(qlog.FlushReqId, nil)
	for {
		var stop bool
		select {
		case <-time.After(30 * time.Second):
			func() {
				defer logRequestPanic(c)
				qfs.syncAll(c)
			}()

		case stop = <-quit:
		}

		if stop {
			finished <- true
			return
		}
	}
}

// Get an inode in a thread safe way
func (qfs *QuantumFs) inode(c *ctx, id InodeId) Inode {
	inode, needsInstantiation := func() (Inode, bool) {
		qfs.mapMutex.RLock()
		defer qfs.mapMutex.RUnlock()
		inode, instantiated := qfs.inodes[id]
		if instantiated {
			return inode, false
		}

		_, uninstantiated := qfs.uninstantiatedInodes[id]
		return nil, uninstantiated
	}()

	c.vlog("Inode %d: %t %t", id, needsInstantiation, inode != nil)
	if !needsInstantiation {
		return inode
	}

	c.vlog("Inode %d needs to be instantiated", id)

	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()
	// Recheck in case things changes while we didn't have the lock
	inode, instantiated := qfs.inodes[id]
	if instantiated {
		return inode
	}

	parent, uninstantiated := qfs.uninstantiatedInodes[id]
	if !uninstantiated {
		// We don't know anything about this Inode
		return nil
	}

	inode, newUninstantiated := parent.instantiateChild(c, id)
	delete(qfs.uninstantiatedInodes, id)
	qfs.inodes[id] = inode
	for _, id := range newUninstantiated {
		c.vlog("Adding uninstantiated %v", id)
		qfs.uninstantiatedInodes[id] = inode
	}

	return inode
}

// Set an inode in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setInode(c *ctx, id InodeId, inode Inode) {
	qfs.mapMutex.Lock()
	if inode != nil {
		qfs.inodes[id] = inode
	} else {
		delete(qfs.inodes, id)
	}
	qfs.mapMutex.Unlock()
}

// Set a list of inode numbers to be uninstantiated with the given parent
func (qfs *QuantumFs) addUninstantiated(c *ctx, uninstantiated []InodeId,
	parent Inode) {

	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

	for _, inodeNum := range uninstantiated {
		c.vlog("Adding uninstantiated %v", inodeNum)
		qfs.uninstantiatedInodes[inodeNum] = parent
	}
}

// Remove a list of inode numbers from the uninstantiatedInodes list
func (qfs *QuantumFs) removeUninstantiated(c *ctx, uninstantiated []InodeId) {
	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

	for _, inodeNum := range uninstantiated {
		delete(qfs.uninstantiatedInodes, inodeNum)
	}
}

// Get a file handle in a thread safe way
func (qfs *QuantumFs) fileHandle(c *ctx, id FileHandleId) FileHandle {
	qfs.mapMutex.RLock()
	fileHandle := qfs.fileHandles[id]
	qfs.mapMutex.RUnlock()
	return fileHandle
}

// Set a file handle in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setFileHandle(c *ctx, id FileHandleId, fileHandle FileHandle) {
	c.vlog("QuantumFs::setFileHandle Enter")
	defer c.vlog("QuantumFs::setFileHandle Exit")

	qfs.mapMutex.Lock()
	if fileHandle != nil {
		qfs.fileHandles[id] = fileHandle
	} else {
		delete(qfs.fileHandles, id)
	}
	qfs.mapMutex.Unlock()
}

// Retrieve a unique inode number
func (qfs *QuantumFs) newInodeId() InodeId {
	return InodeId(atomic.AddUint64(&qfs.inodeNum, 1))
}

// Retrieve a unique filehandle number
func (qfs *QuantumFs) newFileHandleId() FileHandleId {
	return FileHandleId(atomic.AddUint64(&qfs.fileHandleNum, 1))
}

// Track a workspace as active so we know if we have to sync it
func (qfs *QuantumFs) activateWorkspace(c *ctx, name string,
	workspaceroot *WorkspaceRoot) {

	c.vlog("QuantumFs::activateWorkspace %s", name)

	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()
	if _, exists := qfs.activeWorkspaces[name]; exists {
		panic("Workspace registered twice")
	}
	qfs.activeWorkspaces[name] = workspaceroot
}

// Untrack a workspace as active so we won't sync it. Usually this is called when
// the workspaceroot Inode is about to be deleted
func (qfs *QuantumFs) deactivateWorkspace(c *ctx, name string) {
	c.vlog("QuantumFs::deactivateWorkspace %s", name)

	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()
	delete(qfs.activeWorkspaces, name)
}

// Trigger all active workspaces to sync
func (qfs *QuantumFs) syncAll(c *ctx) {
	c.vlog("QuantumFs::syncAll Enter")
	defer c.vlog("QuantumFs::syncAll Exit")

	var workspaces []*WorkspaceRoot

	func() {
		qfs.mapMutex.RLock()
		defer qfs.mapMutex.RUnlock()

		workspaces = make([]*WorkspaceRoot, 0, len(qfs.activeWorkspaces))

		for _, workspace := range qfs.activeWorkspaces {
			workspaces = append(workspaces, workspace)
		}
	}()

	for _, workspace := range workspaces {
		func() {
			c.vlog("Locking and syncing workspace %s/%s",
				workspace.namespace, workspace.workspace)
			defer workspace.LockTree().Unlock()
			workspace.flush_DOWN(c)
		}()
	}
}

func logRequestPanic(c *ctx) {
	exception := recover()
	if exception == nil {
		return
	}

	stackTrace := debug.Stack()

	c.elog("ERROR: PANIC serving request %d: '%s' Stacktrace: %v", c.RequestId,
		fmt.Sprintf("%v", exception), BytesToString(stackTrace))
}

func (qfs *QuantumFs) Lookup(header *fuse.InHeader, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.dlog("QuantumFs::Lookup Inode %d Name %s", header.NodeId, name)
	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		c.elog("Lookup failed", name)
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Lookup(c, name, out)
}

func (qfs *QuantumFs) Forget(nodeID uint64, nlookup uint64) {
	defer logRequestPanic(&qfs.c)

	// Allow tests to disable this feature
	if !qfs.allowForget {
		return
	}

	qfs.c.dlog("Forgetting inode %d Looked up %d Times", nodeID, nlookup)

	inode := qfs.inode(&qfs.c, InodeId(nodeID))
	if inode == nil {
		// Nothing to do
		return
	}

	// We currently only support file / special forgetting
	switch inode.(type) {
	case *File, *Special:
		defer inode.LockTree().Unlock()
		inode.forget_DOWN(&qfs.c)
	}
}

func (qfs *QuantumFs) GetAttr(input *fuse.GetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::GetAttr Enter Inode %d", input.NodeId)
	defer c.vlog("QuantumFs::GetAttr Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.GetAttr(c, out)
}

func (qfs *QuantumFs) SetAttr(input *fuse.SetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::SetAttr Enter Inode %d", input.NodeId)
	defer c.vlog("QuantumFs::SetAttr Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.SetAttr(c, input, out)
}

func (qfs *QuantumFs) Mknod(input *fuse.MknodIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Mknod Enter Inode %d Name %s", input.NodeId, name)
	defer c.vlog("QuantumFs::Mknod Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Mknod(c, name, input, out)
}

func (qfs *QuantumFs) Mkdir(input *fuse.MkdirIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Mkdir Enter Inode %d Name %s", input.NodeId, name)
	defer c.vlog("QuantumFs::Mkdir Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Mkdir(c, name, input, out)
}

func (qfs *QuantumFs) Unlink(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Unlink Enter Inode %d Name %s", header.NodeId, name)
	defer c.vlog("QuantumFs::Unlink Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Unlink(c, name)
}

func (qfs *QuantumFs) Rmdir(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Rmdir Enter Inode %d Name %s", header.NodeId, name)
	defer c.vlog("QuantumFs::Rmdir Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Rmdir(c, name)
}

func (qfs *QuantumFs) Rename(input *fuse.RenameIn, oldName string,
	newName string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Rename Enter Inode %d newdir %d %s -> %s", input.NodeId,
		input.Newdir, oldName, newName)
	defer c.vlog("QuantumFs::Rename Exit")

	if input.NodeId == input.Newdir {
		inode := qfs.inode(c, InodeId(input.NodeId))
		if inode == nil {
			return fuse.ENOENT
		}

		defer inode.RLockTree().RUnlock()
		return inode.RenameChild(c, oldName, newName)
	} else {
		srcInode := qfs.inode(c, InodeId(input.NodeId))
		if srcInode == nil {
			return fuse.ENOENT
		}

		dstInode := qfs.inode(c, InodeId(input.Newdir))
		if dstInode == nil {
			return fuse.ENOENT
		}

		defer srcInode.RLockTree().RUnlock()
		defer dstInode.RLockTree().RUnlock()

		return srcInode.MvChild(c, dstInode, oldName, newName)
	}
}

func (qfs *QuantumFs) Link(input *fuse.LinkIn, filename string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Link Enter inode %d to name %s in dstDir %d",
		input.NodeId, filename, input.Oldnodeid)
	defer c.vlog("QuantumFs::Link Exit")

	srcInode := qfs.inode(c, InodeId(input.Oldnodeid))
	if srcInode == nil {
		return fuse.ENOENT
	}

	dstInode := qfs.inode(c, InodeId(input.NodeId))
	if dstInode == nil {
		return fuse.ENOENT
	}

	if srcInode.treeLock() == dstInode.treeLock() {
		// If src and dst live in the same workspace, we only need one lock
		defer dstInode.LockTree().Unlock()
	} else {
		// When we have to lock multiple inodes, we must make sure that we
		// lock the inodes in correct sequence to prevent deadlock
		firstLock, lastLock := getLockOrder(dstInode, srcInode)
		defer firstLock.LockTree().Unlock()
		defer lastLock.LockTree().Unlock()
	}

	return dstInode.link_DOWN(c, srcInode, filename, out)
}

func (qfs *QuantumFs) Symlink(header *fuse.InHeader, pointedTo string,
	linkName string, out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Symlink Enter Inode %d Name %s", header.NodeId, linkName)
	defer c.vlog("QuantumFs::Symlink Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Symlink(c, pointedTo, linkName, out)
}

func (qfs *QuantumFs) Readlink(header *fuse.InHeader) (out []byte,
	result fuse.Status) {

	out = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Readlink Enter Inode %d", header.NodeId)
	defer c.vlog("QuantumFs::Readlink Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return nil, fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Readlink(c)
}

func (qfs *QuantumFs) Access(input *fuse.AccessIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Access Enter Inode %d", input.NodeId)
	defer c.vlog("QuantumFs::Access Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Access(c, input.Mask, input.Uid, input.Gid)
}

func (qfs *QuantumFs) GetXAttrSize(header *fuse.InHeader, attr string) (size int,
	result fuse.Status) {

	size = 0
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::GetXAttrSize Enter Inode %d", header.NodeId)
	defer c.vlog("QuantumFs::GetXAttrSize Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return 0, fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.GetXAttrSize(c, attr)
}

func (qfs *QuantumFs) GetXAttrData(header *fuse.InHeader, attr string) (data []byte,
	result fuse.Status) {

	data = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::GetXAttrData Enter Inode %d", header.NodeId)
	defer c.vlog("QuantumFs::GetXAttrData Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return nil, fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.GetXAttrData(c, attr)
}

func (qfs *QuantumFs) ListXAttr(header *fuse.InHeader) (attributes []byte,
	result fuse.Status) {

	attributes = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ListXAttr Enter Inode %d", header.NodeId)
	defer c.vlog("QuantumFs::ListXAttr Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return nil, fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.ListXAttr(c)
}

func (qfs *QuantumFs) SetXAttr(input *fuse.SetXAttrIn, attr string,
	data []byte) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::SetXAttr Enter Inode %d", input.NodeId)
	defer c.vlog("QuantumFs::SetXAttr Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.SetXAttr(c, attr, data)
}

func (qfs *QuantumFs) RemoveXAttr(header *fuse.InHeader,
	attr string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::RemoveXAttr Enter Inode %d", header.NodeId)
	defer c.vlog("QuantumFs::RemoveXAttr Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.RemoveXAttr(c, attr)
}

func (qfs *QuantumFs) Create(input *fuse.CreateIn, name string,
	out *fuse.CreateOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Create Enter Inode %d Name %s", input.NodeId, name)
	defer c.vlog("QuantumFs::Create Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("Create failed", input)
		return fuse.EACCES // TODO Confirm this is correct
	}

	defer inode.RLockTree().RUnlock()
	return inode.Create(c, input, name, out)
}

func (qfs *QuantumFs) Open(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Open Enter Inode %d", input.NodeId)
	defer c.vlog("QuantumFs::Open Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("Open failed %v", input)
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Open(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) Read(input *fuse.ReadIn, buf []byte) (readRes fuse.ReadResult,
	result fuse.Status) {

	readRes = nil
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Read Enter Fh: %d", input.Fh)
	defer c.vlog("QuantumFs::Read Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Read failed", fileHandle)
		return nil, fuse.ENOENT
	}

	defer fileHandle.RLockTree().RUnlock()
	return fileHandle.Read(c, input.Offset, input.Size,
		buf, BitFlagsSet(uint(input.Flags), uint(syscall.O_NONBLOCK)))
}

func (qfs *QuantumFs) Release(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Release Enter Fh: %v", input.Fh)
	defer c.vlog("QuantumFs::Release Exit")

	qfs.setFileHandle(c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) Write(input *fuse.WriteIn, data []byte) (written uint32,
	result fuse.Status) {

	written = 0
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Write Enter Fh: %d", input.Fh)
	defer c.vlog("QuantumFs::Write Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Write failed")
		return 0, fuse.ENOENT
	}

	defer fileHandle.RLockTree().RUnlock()
	return fileHandle.Write(c, input.Offset, input.Size,
		input.Flags, data)
}

func (qfs *QuantumFs) Flush(input *fuse.FlushIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Flush Enter Fh: %v Context %d %d %d", input.Fh,
		input.Context.Uid, input.Context.Gid, input.Context.Pid)
	defer c.vlog("QuantumFs::Flush Exit")

	return fuse.OK
}

func (qfs *QuantumFs) Fsync(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Fsync Enter Fh %d", input.Fh)
	defer c.vlog("QuantumFs::Fsync Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Fsync failed")
		return fuse.EIO
	}

	defer fileHandle.LockTree().Unlock()
	return fileHandle.Sync_DOWN(c)
}

func (qfs *QuantumFs) Fallocate(input *fuse.FallocateIn) (result fuse.Status) {
	result = fuse.EIO
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Fallocate Enter")
	defer c.vlog("QuantumFs::Fallocate Exit")

	c.elog("Unhandled request Fallocate")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) OpenDir(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::OpenDir Enter Inode %d", input.NodeId)
	defer c.vlog("QuantumFs::OpenDir Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("OpenDir failed", input)
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.OpenDir(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) ReadDir(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ReadDir Enter Fh: %d offset %d", input.Fh, input.Offset)
	defer c.vlog("QuantumFs::ReadDir Exit")

	c.elog("Unhandled request ReadDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) ReadDirPlus(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ReadDirPlus Enter Fh: %d offset %d", input.Fh,
		input.Offset)
	defer c.vlog("QuantumFs::ReadDirPlus Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("ReadDirPlus failed", fileHandle)
		return fuse.ENOENT
	}

	defer fileHandle.RLockTree().RUnlock()
	return fileHandle.ReadDirPlus(c, input, out)
}

func (qfs *QuantumFs) ReleaseDir(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ReleaseDir Enter Fh: %d", input.Fh)
	defer c.vlog("QuantumFs::ReleaseDir Exit")

	qfs.setFileHandle(&qfs.c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) FsyncDir(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::FsyncDir Enter Fh %d", input.Fh)
	defer c.vlog("QuantumFs::FsyncDir Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("FsyncDir failed")
		return fuse.EIO
	}

	defer fileHandle.LockTree().Unlock()
	return fileHandle.Sync_DOWN(c)
}

func (qfs *QuantumFs) StatFs(input *fuse.InHeader,
	out *fuse.StatfsOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(input)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::StatFs Enter")
	defer c.vlog("QuantumFs::StatFs Exit")

	out.Blocks = 2684354560 // 10TB
	out.Bfree = out.Blocks / 2
	out.Bavail = out.Bfree
	out.Files = 0
	out.Ffree = math.MaxUint64
	out.Bsize = qfsBlockSize
	out.NameLen = uint32(quantumfs.MaxFilenameLength)
	out.Frsize = 0

	return fuse.OK
}

func (qfs *QuantumFs) Init(*fuse.Server) {
	qfs.c.elog("Unhandled request Init")
}

func (qfs *QuantumFs) getWorkspaceRoot(c *ctx, name string) (*WorkspaceRoot, bool) {

	c.vlog("QuantumFs::getWorkspaceRoot %s", name)

	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

	wsr, exists := qfs.activeWorkspaces[name]
	return wsr, exists
}
