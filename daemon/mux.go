// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// go-fuse creates a goroutine for every request. The code here simply takes these
// requests and forwards them to the correct Inode.
package daemon

import "math"
import "runtime/debug"
import "syscall"
import "sync"
import "sync/atomic"

import "arista.com/quantumfs"
import "arista.com/quantumfs/qlog"
import "github.com/hanwen/go-fuse/fuse"

func NewQuantumFs(config QuantumFsConfig) fuse.RawFileSystem {
	qfs := &QuantumFs{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		config:        config,
		inodes:        make(map[InodeId]Inode),
		fileHandles:   make(map[FileHandleId]FileHandle),
		inodeNum:      quantumfs.InodeIdReservedEnd,
		fileHandleNum: quantumfs.InodeIdReservedEnd,
		c: ctx{
			Ctx: quantumfs.Ctx{
				Qlog:      qlog.NewQlog(),
				RequestId: qlog.DummyReqId,
			},
			config:       &config,
			workspaceDB:  config.WorkspaceDB,
			durableStore: config.DurableStore,
		},
	}

	qfs.c.qfs = qfs

	qfs.inodes[quantumfs.InodeIdRoot] = NewNamespaceList()
	qfs.inodes[quantumfs.InodeIdApi] = NewApiInode()
	return qfs
}

type QuantumFs struct {
	fuse.RawFileSystem
	config        QuantumFsConfig
	inodeNum      uint64
	fileHandleNum uint64
	c             ctx

	mapMutex    sync.Mutex // TODO: Perhaps an RWMutex instead?
	inodes      map[InodeId]Inode
	fileHandles map[FileHandleId]FileHandle
}

// Get an inode in a thread safe way
func (qfs *QuantumFs) inode(c *ctx, id InodeId) Inode {
	qfs.mapMutex.Lock()
	inode := qfs.inodes[id]
	qfs.mapMutex.Unlock()
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

// Get a file handle in a thread safe way
func (qfs *QuantumFs) fileHandle(c *ctx, id FileHandleId) FileHandle {
	qfs.mapMutex.Lock()
	fileHandle := qfs.fileHandles[id]
	qfs.mapMutex.Unlock()
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

func logRequestPanic(c *ctx) {
	exception := recover()
	if exception == nil {
		return
	}

	stackTrace := debug.Stack()

	c.elog("PANIC serving request %u: '%v' Stacktrace: %v", exception,
		BytesToString(stackTrace))
}

func (qfs *QuantumFs) Lookup(header *fuse.InHeader, name string,
	out *fuse.EntryOut) fuse.Status {

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		c.elog("Lookup failed", name)
		return fuse.ENOENT
	}

	return inode.Lookup(c, name, out)
}

func (qfs *QuantumFs) Forget(nodeID uint64, nlookup uint64) {
	qfs.c.dlog("Forgetting inode %d Looked up %d Times", nodeID, nlookup)
	qfs.setInode(&qfs.c, InodeId(nodeID), nil)
}

func (qfs *QuantumFs) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::GetAttr Enter")
	defer c.vlog("QuantumFs::GetAttr Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.GetAttr(c, out)
}

func (qfs *QuantumFs) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::SetAttr Enter")
	defer c.vlog("QuantumFs::SetAttr Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.SetAttr(c, input, out)
}

func (qfs *QuantumFs) Mknod(input *fuse.MknodIn, name string,
	out *fuse.EntryOut) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Mknod Enter")
	defer c.vlog("QuantumFs::Mknod Exit")

	c.elog("Unhandled request Mknod")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Mkdir(input *fuse.MkdirIn, name string,
	out *fuse.EntryOut) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Mkdir Enter")
	defer c.vlog("QuantumFs::Mkdir Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.Mkdir(c, name, input, out)
}

func (qfs *QuantumFs) Unlink(header *fuse.InHeader, name string) fuse.Status {
	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Unlink Enter")
	defer c.vlog("QuantumFs::Unlink Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.Unlink(c, name)
}

func (qfs *QuantumFs) Rmdir(header *fuse.InHeader, name string) fuse.Status {
	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Rmdir Enter")
	defer c.vlog("QuantumFs::Rmdir Exit")

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.Rmdir(c, name)
}

func (qfs *QuantumFs) Rename(input *fuse.RenameIn, oldName string,
	newName string) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Rename Enter")
	defer c.vlog("QuantumFs::Rename Exit")

	c.elog("Unhandled request Rename")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Link(input *fuse.LinkIn, filename string,
	out *fuse.EntryOut) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Link Enter")
	defer c.vlog("QuantumFs::Link Exit")

	return fuse.EPERM
}

func (qfs *QuantumFs) Symlink(header *fuse.InHeader, pointedTo string,
	linkName string, out *fuse.EntryOut) fuse.Status {

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Symlink Enter")
	defer c.vlog("QuantumFs::Symlink Exit")

	c.elog("Unhandled request Symlink")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Readlink(header *fuse.InHeader) (out []byte,
	code fuse.Status) {

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Readlink Enter")
	defer c.vlog("QuantumFs::Readlink Exit")

	c.elog("Unhandled request Readlink")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) Access(input *fuse.AccessIn) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Access Enter")
	defer c.vlog("QuantumFs::Access Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.Access(c, input.Mask, input.Uid, input.Gid)
}

func (qfs *QuantumFs) GetXAttrSize(header *fuse.InHeader, attr string) (sz int,
	code fuse.Status) {

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::GetXAttrSize Enter")
	defer c.vlog("QuantumFs::GetXAttrSize Exit")

	c.elog("Unhandled request GetXAttrSize")
	return 0, fuse.ENOSYS
}

func (qfs *QuantumFs) GetXAttrData(header *fuse.InHeader, attr string) (data []byte,
	code fuse.Status) {

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::GetXAttrData Enter")
	defer c.vlog("QuantumFs::GetXAttrData Exit")

	c.elog("Unhandled request GetXAttrData")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) ListXAttr(header *fuse.InHeader) (attributes []byte,
	code fuse.Status) {

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ListXAttr Enter")
	defer c.vlog("QuantumFs::ListXAttr Exit")

	c.elog("Unhandled request ListXAttr")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) SetXAttr(input *fuse.SetXAttrIn, attr string,
	data []byte) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::SetXAttr Enter")
	defer c.vlog("QuantumFs::SetXAttr Exit")

	c.elog("Unhandled request SetXAttr")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) RemoveXAttr(header *fuse.InHeader, attr string) fuse.Status {
	c := qfs.c.req(header)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::RemoveXAttr Enter")
	defer c.vlog("QuantumFs::RemoveXAttr Exit")

	c.elog("Unhandled request RemoveXAttr")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Create(input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Create Enter")
	defer c.vlog("QuantumFs::Create Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("Create failed", input)
		return fuse.EACCES // TODO Confirm this is correct
	}

	return inode.Create(c, input, name, out)
}

func (qfs *QuantumFs) Open(input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Open Enter")
	defer c.vlog("QuantumFs::Open Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("Open failed", input)
		return fuse.ENOENT
	}

	return inode.Open(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult,
	fuse.Status) {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Read Enter")
	defer c.vlog("QuantumFs::Read Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Read failed", fileHandle)
		return nil, fuse.ENOENT
	}
	return fileHandle.Read(c, input.Offset, input.Size,
		buf, BitFlagsSet(uint(input.Flags), uint(syscall.O_NONBLOCK)))
}

func (qfs *QuantumFs) Release(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Release Enter Fh: ", input.Fh)
	defer c.vlog("QuantumFs::Release Exit")

	qfs.setFileHandle(c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) Write(input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Write Enter")
	defer c.vlog("QuantumFs::Write Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Write failed")
		return 0, fuse.ENOENT
	}
	return fileHandle.Write(c, input.Offset, input.Size,
		input.Flags, data)
}

func (qfs *QuantumFs) Flush(input *fuse.FlushIn) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Flush Enter Fh: ", input.Fh)
	defer c.vlog("QuantumFs::Flush Exit")

	c.elog("Unhandled request Flush")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Fsync(input *fuse.FsyncIn) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Fsync Enter")
	defer c.vlog("QuantumFs::Fsync Exit")

	c.elog("Unhandled request Fsync")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Fallocate(input *fuse.FallocateIn) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::Fallocate Enter")
	defer c.vlog("QuantumFs::Fallocate Exit")

	c.elog("Unhandled request Fallocate")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::OpenDir Enter")
	defer c.vlog("QuantumFs::OpenDir Exit")

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("OpenDir failed", input)
		return fuse.ENOENT
	}

	return inode.OpenDir(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) ReadDir(input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ReadDir Enter")
	defer c.vlog("QuantumFs::ReadDir Exit")

	c.elog("Unhandled request ReadDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) ReadDirPlus(input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ReadDirPlus Enter")
	defer c.vlog("QuantumFs::ReadDirPlus Exit")

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("ReadDirPlus failed", fileHandle)
		return fuse.ENOENT
	}
	return fileHandle.ReadDirPlus(c, input, out)
}

func (qfs *QuantumFs) ReleaseDir(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::ReleaseDir Enter")
	defer c.vlog("QuantumFs::ReleaseDir Exit")

	qfs.setFileHandle(&qfs.c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) FsyncDir(input *fuse.FsyncIn) fuse.Status {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	c.vlog("QuantumFs::FsyncDir Enter")
	defer c.vlog("QuantumFs::FsyncDir Exit")

	c.elog("Unhandled request FsyncDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) StatFs(input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
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
	out.NameLen = quantumfs.MaxFilenameLength
	out.Frsize = 0

	return fuse.OK
}

func (qfs *QuantumFs) Init(*fuse.Server) {
	qfs.c.elog("Unhandled request Init")
}
