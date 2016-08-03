// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains all the interaction with the quantumfs API file.

import "encoding/json"
import "errors"
import "fmt"
import "strings"
import "sync"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func NewApiInode(treeLock *sync.RWMutex) Inode {
	api := ApiInode{
		InodeCommon: InodeCommon{
			id:        quantumfs.InodeIdApi,
			treeLock_: treeLock,
		},
	}
	api.self = &api
	assert(api.treeLock() != nil, "ApiInode treeLock is nil at init")
	return &api
}

type ApiInode struct {
	InodeCommon
}

func fillApiAttr(attr *fuse.Attr) {
	attr.Ino = quantumfs.InodeIdApi
	attr.Size = 1024
	attr.Blocks = 1

	now := time.Now()
	attr.Atime = uint64(now.Unix())
	attr.Atimensec = uint32(now.Nanosecond())
	attr.Mtime = uint64(now.Unix())
	attr.Mtimensec = uint32(now.Nanosecond())

	attr.Ctime = 1
	attr.Ctimensec = 1
	attr.Mode = 0666 | fuse.S_IFREG
	attr.Nlink = 1
	attr.Owner.Uid = 0
	attr.Owner.Gid = 0
	attr.Blksize = 4096
}

func (api *ApiInode) dirty(c *ctx) {
}

func (api *ApiInode) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	switch mask {
	case fuse.F_OK,
		fuse.W_OK,
		fuse.R_OK:
		return fuse.OK
	case fuse.X_OK:
		return fuse.EACCES
	default:
		return fuse.EINVAL
	}
}

func (api *ApiInode) GetAttr(c *ctx, out *fuse.AttrOut) fuse.Status {
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	fillApiAttr(&out.Attr)
	return fuse.OK
}

func (api *ApiInode) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	return fuse.ENOTDIR
}

func (api *ApiInode) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	return fuse.ENOTDIR
}

func (wsr *ApiInode) getChildRecord(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	c.elog("Api doesn't support record fetch")
	return quantumfs.DirectoryRecord{}, errors.New("Unsupported record fetch")
}

func (api *ApiInode) Unlink(c *ctx, name string) fuse.Status {
	c.elog("Invalid Unlink on ApiInode")
	return fuse.ENOTDIR
}

func (api *ApiInode) Rmdir(c *ctx, name string) fuse.Status {
	c.elog("Invalid Rmdir on ApiInode")
	return fuse.ENOTDIR
}

func (api *ApiInode) Open(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	out.OpenFlags = 0
	handle := newApiHandle(c, api.treeLock())
	c.qfs.setFileHandle(c, handle.FileHandleCommon.id, handle)
	out.Fh = uint64(handle.FileHandleCommon.id)
	return fuse.OK
}

func (api *ApiInode) Lookup(c *ctx, name string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Lookup on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) Create(c *ctx, input *fuse.CreateIn, name string,
	out *fuse.CreateOut) fuse.Status {
	c.vlog("creating file %s", name)
	return fuse.ENOTDIR
}

func (api *ApiInode) SetAttr(c *ctx, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid SetAttr on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) Symlink(c *ctx, pointedTo string, linkName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Symlink on ApiInode")
	return fuse.ENOTDIR
}

func (api *ApiInode) Readlink(c *ctx) ([]byte, fuse.Status) {
	c.elog("Invalid Readlink on ApiInode")
	return nil, fuse.EINVAL
}

func (api *ApiInode) Sync(c *ctx) fuse.Status {
	return fuse.OK
}

func (api *ApiInode) Mknod(c *ctx, name string, input *fuse.MknodIn,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Mknod on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) RenameChild(c *ctx, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid RenameChild on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) MvChild(c *ctx, dstInode Inode, oldName string,
	newName string) fuse.Status {

	c.elog("Invalid MvChild on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) Link(c *ctx, srcInode Inode, newName string,
	out *fuse.EntryOut) fuse.Status {

	c.elog("Invalid Link on ApiInode")
	return fuse.ENOTDIR
}

func (api *ApiInode) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.elog("Invalid syncChild on ApiInode")
}
func (api *ApiInode) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut) fuse.Status {

	c.elog("Invalid setChildAttr on ApiInode")
	return fuse.ENOSYS
}

func newApiHandle(c *ctx, treeLock *sync.RWMutex) *ApiHandle {
	c.vlog("newApiHandle Enter")
	defer c.vlog("newApiHandle Exit")

	api := ApiHandle{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  quantumfs.InodeIdApi,
			treeLock_: treeLock,
		},
		responses: make(chan fuse.ReadResult, 10),
	}
	assert(api.treeLock() != nil, "ApiHandle treeLock nil at init")
	return &api
}

// ApiHandle represents the user's interactions with quantumfs and is not necessarily
// synchronized with other api handles.
type ApiHandle struct {
	FileHandleCommon
	responses chan fuse.ReadResult
}

func (api *ApiHandle) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.elog("Invalid ReadDirPlus against ApiHandle")
	return fuse.ENOSYS
}

func (api *ApiHandle) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	c.vlog("Received read request on Api")
	var blocking chan struct{}
	if !nonblocking {
		blocking = make(chan struct{})
	}

	select {
	case response := <-api.responses:
		c.vlog("API Response %s", response)
		return response, fuse.OK
	case <-blocking:
		// This is a nonblocking socket, so return that nothing is ready
		c.vlog("Nonblocking socket, return nothing")
		return nil, fuse.OK
	}
}

func makeErrorResponse(code uint32, message string) []byte {
	response := quantumfs.ErrorResponse{
		CommandCommon: quantumfs.CommandCommon{
			CommandId: quantumfs.CmdError,
		},
		ErrorCode: code,
		Message:   message,
	}
	bytes, err := json.Marshal(response)
	if err != nil {
		panic("Failed to marshall API error response")
	}
	return bytes
}

func (api *ApiHandle) queueErrorResponse(code uint32, message string) {
	bytes := makeErrorResponse(code, message)
	api.responses <- fuse.ReadResultData(bytes)
}

func (api *ApiHandle) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {
	c.vlog("writing to file")

	var cmd quantumfs.CommandCommon
	err := json.Unmarshal(buf, &cmd)

	if err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	switch cmd.CommandId {
	default:
		message := fmt.Sprintf("Unknown command number %d", cmd.CommandId)
		api.queueErrorResponse(quantumfs.ErrorBadCommandId, message)

	case quantumfs.CmdError:
		message := fmt.Sprintf("Invalid message %d to send to quantumfsd",
			cmd.CommandId)
		api.queueErrorResponse(quantumfs.ErrorBadCommandId, message)

	case quantumfs.CmdBranchRequest:
		c.vlog("Received branch request")
		api.branchWorkspace(c, buf)
	case quantumfs.CmdSyncAll:
		c.vlog("Received all workspace sync request")
		api.syncAll(c)

	}

	c.vlog("done writing to file")
	return size, fuse.OK
}

func (api *ApiHandle) branchWorkspace(c *ctx, buf []byte) {
	var cmd quantumfs.BranchRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
		return
	}

	src := strings.Split(cmd.Src, "/")
	dst := strings.Split(cmd.Dst, "/")

	c.qfs.syncAll(c)

	if err := c.workspaceDB.BranchWorkspace(&c.Ctx, src[0], src[1], dst[0],
		dst[1]); err != nil {

		api.queueErrorResponse(quantumfs.ErrorCommandFailed, err.Error())
		return
	}

	api.queueErrorResponse(quantumfs.ErrorOK, "Branch Succeeded")
}

func (api *ApiHandle) syncAll(c *ctx) {
	c.qfs.syncAll(c)
	api.queueErrorResponse(quantumfs.ErrorOK, "SyncAll Succeeded")
}

func (api *ApiHandle) Sync(c *ctx) fuse.Status {
	return fuse.OK
}
