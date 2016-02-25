// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// go-fuse creates a goroutine for every request. The code here simply takes these
// requests and forwards them to the correct Inode.
package main

import "fmt"
import "math"
import "sync"
import "sync/atomic"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

var globalQfs *QuantumFs

func getInstance(config QuantumFsConfig) fuse.RawFileSystem {
	if globalQfs == nil {
		qfs := &QuantumFs{
			RawFileSystem: fuse.NewDefaultRawFileSystem(),
			config:        config,
			inodes:        make(map[uint64]Inode),
			fileHandles:   make(map[uint64]FileHandle),
			inodeNum:      fuse.FUSE_ROOT_ID,
			fileHandleNum: fuse.FUSE_ROOT_ID,
		}

		qfs.inodes[fuse.FUSE_ROOT_ID] = NewNamespaceList()
		globalQfs = qfs
	}
	return globalQfs
}

type QuantumFs struct {
	fuse.RawFileSystem
	config        QuantumFsConfig
	inodeNum      uint64
	fileHandleNum uint64

	mapMutex    sync.Mutex // TODO: Perhaps an RWMutex instead?
	inodes      map[uint64]Inode
	fileHandles map[uint64]FileHandle
}

// Get an inode in a thread safe way
func (qfs *QuantumFs) inode(id uint64) Inode {
	qfs.mapMutex.Lock()
	inode := qfs.inodes[id]
	qfs.mapMutex.Unlock()
	return inode
}

// Set an inode in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setInode(id uint64, inode Inode) {
	qfs.mapMutex.Lock()
	if inode != nil {
		qfs.inodes[id] = inode
	} else {
		delete(qfs.inodes, id)
	}
	qfs.mapMutex.Unlock()
}

// Get a file handle in a thread safe way
func (qfs *QuantumFs) fileHandle(id uint64) FileHandle {
	qfs.mapMutex.Lock()
	fileHandle := qfs.fileHandles[id]
	qfs.mapMutex.Unlock()
	return fileHandle
}

// Set a file handle in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setFileHandle(id uint64, fileHandle FileHandle) {
	qfs.mapMutex.Lock()
	if fileHandle != nil {
		qfs.fileHandles[id] = fileHandle
	} else {
		delete(qfs.fileHandles, id)
	}
	qfs.mapMutex.Unlock()
}

// Retrieve a unique inode number
func (qfs *QuantumFs) newInodeId() uint64 {
	return atomic.AddUint64(&qfs.inodeNum, 1)
}

// Retrieve a unique filehandle number
func (qfs *QuantumFs) newFileHandleId() uint64 {
	return atomic.AddUint64(&qfs.fileHandleNum, 1)
}

func (qfs *QuantumFs) Lookup(header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	fmt.Println("Unhandled request Lookup")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Forget(nodeid, nlookup uint64) {
	fmt.Println("Unhandled request Forget")
}

func (qfs *QuantumFs) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (result fuse.Status) {
	inode := qfs.inode(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.GetAttr(out)
}

func (qfs *QuantumFs) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	fmt.Println("Unhandled request SetAttr")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	fmt.Println("Unhandled request Mknod")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	fmt.Println("Unhandled request Mkdir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Unlink(header *fuse.InHeader, name string) (code fuse.Status) {
	fmt.Println("Unhandled request Unlink")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Rmdir(header *fuse.InHeader, name string) (code fuse.Status) {
	fmt.Println("Unhandled request Rmdir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	fmt.Println("Unhandled request Rename")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Link(input *fuse.LinkIn, filename string, out *fuse.EntryOut) (code fuse.Status) {
	fmt.Println("Unhandled request Link")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Symlink(header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	fmt.Println("Unhandled request Symlink")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Readlink(header *fuse.InHeader) (out []byte, code fuse.Status) {
	fmt.Println("Unhandled request Readlink")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) Access(input *fuse.AccessIn) (code fuse.Status) {
	fmt.Println("Unhandled request Access")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) GetXAttrSize(header *fuse.InHeader, attr string) (sz int, code fuse.Status) {
	fmt.Println("Unhandled request GetXAttrSize")
	return 0, fuse.ENOSYS
}

func (qfs *QuantumFs) GetXAttrData(header *fuse.InHeader, attr string) (data []byte, code fuse.Status) {
	fmt.Println("Unhandled request GetXAttrData")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) ListXAttr(header *fuse.InHeader) (attributes []byte, code fuse.Status) {
	fmt.Println("Unhandled request ListXAttr")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) SetXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	fmt.Println("Unhandled request SetXAttr")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) RemoveXAttr(header *fuse.InHeader, attr string) (code fuse.Status) {
	fmt.Println("Unhandled request RemoveXAttr")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	fmt.Println("Unhandled request Create")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Open(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	fmt.Println("Unhandled request Open")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fmt.Println("Unhandled request Read")
	return nil, fuse.ENOSYS
}

func (qfs *QuantumFs) Release(input *fuse.ReleaseIn) {
	fmt.Println("Unhandled request Release")
}

func (qfs *QuantumFs) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	fmt.Println("Unhandled request Write")
	return 0, fuse.ENOSYS
}

func (qfs *QuantumFs) Flush(input *fuse.FlushIn) fuse.Status {
	fmt.Println("Unhandled request Flush")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Fsync(input *fuse.FsyncIn) (code fuse.Status) {
	fmt.Println("Unhandled request Fsync")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) Fallocate(input *fuse.FallocateIn) (code fuse.Status) {
	fmt.Println("Unhandled request Fallocate")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	inode := qfs.inode(input.NodeId)
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.OpenDir(input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) ReadDir(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("Unhandled request ReadDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fileHandle := qfs.fileHandle(input.Fh)
	if fileHandle == nil {
		return fuse.ENOENT
	}
	return fileHandle.ReadDirPlus(input, out)
}

func (qfs *QuantumFs) ReleaseDir(input *fuse.ReleaseIn) {
	qfs.setFileHandle(input.Fh, nil)
}

func (qfs *QuantumFs) FsyncDir(input *fuse.FsyncIn) (code fuse.Status) {
	fmt.Println("Unhandled request FsyncDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) StatFs(input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	out.Blocks = 2684354560 // 10TB
	out.Bfree = out.Blocks / 2
	out.Bavail = out.Bfree
	out.Files = 0
	out.Ffree = math.MaxUint64
	out.Bsize = 4096
	out.NameLen = quantumfs.MaxFilenameLength
	out.Frsize = 0

	return fuse.OK
}

func (qfs *QuantumFs) Init(*fuse.Server) {
	fmt.Println("Unhandled request Init")
}
