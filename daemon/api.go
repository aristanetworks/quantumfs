// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains all the interaction with the quantumfs API file.
import "encoding/json"
import "errors"
import "fmt"
import "strings"
import "sync"
import "sync/atomic"
import "syscall"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

func NewApiInode(treeLock *sync.RWMutex, parent InodeId) Inode {
	api := ApiInode{
		InodeCommon: InodeCommon{
			id:        quantumfs.InodeIdApi,
			name_:     quantumfs.ApiPath,
			treeLock_: treeLock,
		},
	}
	api.self = &api
	api.setParent(parent)
	utils.Assert(api.treeLock() != nil, "ApiInode treeLock is nil at init")
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
	// Override the InodeCommon dirty because the Api can never be changed on the
	// filesystem itself.
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
	inodeNum InodeId) (DirectoryRecordIf, error) {

	c.elog("Api doesn't support record fetch")
	return &quantumfs.DirectoryRecord{}, errors.New("Unsupported record fetch")
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

func (api *ApiInode) GetXAttrSize(c *ctx,
	attr string) (size int, result fuse.Status) {

	return 0, fuse.ENODATA
}

func (api *ApiInode) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	return nil, fuse.ENODATA
}

func (api *ApiInode) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	return []byte{}, fuse.OK
}

func (api *ApiInode) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	return fuse.Status(syscall.ENOSPC)
}

func (api *ApiInode) RemoveXAttr(c *ctx, attr string) fuse.Status {
	return fuse.ENODATA
}

func (api *ApiInode) syncChild(c *ctx, inodeNum InodeId,
	newKey quantumfs.ObjectKey) {

	c.elog("Invalid syncChild on ApiInode")
}

func (api *ApiInode) setChildAttr(c *ctx, inodeNum InodeId,
	newType *quantumfs.ObjectType, attr *fuse.SetAttrIn,
	out *fuse.AttrOut, updateMtime bool) fuse.Status {

	c.elog("Invalid setChildAttr on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) getChildXAttrSize(c *ctx, inodeNum InodeId,
	attr string) (size int, result fuse.Status) {

	c.elog("Invalid getChildXAttrSize on ApiInode")
	return 0, fuse.ENODATA
}

func (api *ApiInode) getChildXAttrData(c *ctx, inodeNum InodeId,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid getChildXAttrData on ApiInode")
	return nil, fuse.ENODATA
}

func (api *ApiInode) listChildXAttr(c *ctx,
	inodeNum InodeId) (attributes []byte, result fuse.Status) {

	c.elog("Invalid listChildXAttr on ApiInode")
	return []byte{}, fuse.OK
}

func (api *ApiInode) setChildXAttr(c *ctx, inodeNum InodeId, attr string,
	data []byte) fuse.Status {

	c.elog("Invalid setChildXAttr on ApiInode")
	return fuse.Status(syscall.ENOSPC)
}

func (api *ApiInode) removeChildXAttr(c *ctx, inodeNum InodeId,
	attr string) fuse.Status {

	c.elog("Invalid removeChildXAttr on ApiInode")
	return fuse.ENODATA
}

func (api *ApiInode) instantiateChild(c *ctx, inodeNum InodeId) (Inode, []InodeId) {
	c.elog("Invalid instantiateChild on ApiInode")
	return nil, nil
}

func (api *ApiInode) flush(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}

func newApiHandle(c *ctx, treeLock *sync.RWMutex) *ApiHandle {
	defer c.funcIn("newApiHandle Enter").out()

	api := ApiHandle{
		FileHandleCommon: FileHandleCommon{
			id:        c.qfs.newFileHandleId(),
			inodeNum:  quantumfs.InodeIdApi,
			treeLock_: treeLock,
		},
		responses: make(chan fuse.ReadResult, 10),
	}
	utils.Assert(api.treeLock() != nil, "ApiHandle treeLock nil at init")
	return &api
}

// ApiHandle represents the user's interactions with quantumfs and is not necessarily
// synchronized with other api handles.
type ApiHandle struct {
	FileHandleCommon
	outstandingRequests int32
	responses           chan fuse.ReadResult
}

func (api *ApiHandle) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.elog("Invalid ReadDirPlus against ApiHandle")
	return fuse.ENOSYS
}

func (api *ApiHandle) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	c.vlog("Received read request on Api")

	if atomic.LoadInt32(&api.outstandingRequests) == 0 {
		if nonblocking {
			return nil, fuse.Status(syscall.EAGAIN)
		}

		c.vlog("No outstanding requests, returning early")
		return nil, fuse.OK
	}

	response := <-api.responses

	atomic.AddInt32(&api.outstandingRequests, -1)
	debug := make([]byte, response.Size())
	bytes, _ := response.Bytes(debug)
	c.vlog("API Response %s", string(bytes))
	return response, fuse.OK
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

func (api *ApiHandle) queueErrorResponse(code uint32, format string,
	a ...interface{}) {

	message := fmt.Sprintf(format, a)
	bytes := makeErrorResponse(code, message)
	api.responses <- fuse.ReadResultData(bytes)
}

func makeAccessListResponse(list map[string]bool) []byte {
	response := quantumfs.AccessListResponse{
		ErrorResponse: quantumfs.ErrorResponse{
			CommandCommon: quantumfs.CommandCommon{
				CommandId: quantumfs.CmdError,
			},
			ErrorCode: quantumfs.ErrorOK,
			Message:   "",
		},
		AccessList: list,
	}

	bytes, err := json.Marshal(response)
	if err != nil {
		panic("Failed to marshall API AccessListResponse")
	}
	return bytes
}

func (api *ApiHandle) queueAccesslistResponse(list map[string]bool) {
	bytes := makeAccessListResponse(list)
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
		api.queueErrorResponse(quantumfs.ErrorBadCommandId,
			"Unknown command number %d", cmd.CommandId)

	case quantumfs.CmdError:
		api.queueErrorResponse(quantumfs.ErrorBadCommandId,
			"Invalid message %d to send to quantumfsd",
			cmd.CommandId)

	case quantumfs.CmdBranchRequest:
		c.vlog("Received branch request")
		api.branchWorkspace(c, buf)
	case quantumfs.CmdGetAccessed:
		c.vlog("Received GetAccessed request")
		api.getAccessed(c, buf)
	case quantumfs.CmdClearAccessed:
		c.vlog("Received ClearAccessed request")
		api.clearAccessed(c, buf)
	case quantumfs.CmdSyncAll:
		c.vlog("Received all workspace sync request")
		api.syncAll(c)
	// create an object with a given ObjectKey and path
	case quantumfs.CmdInsertInode:
		c.vlog("Received InsertInode request")
		api.insertInode(c, buf)
	case quantumfs.CmdDeleteWorkspace:
		c.vlog("Received DeleteWorkspace request")
		api.deleteWorkspace(c, buf)
	}

	c.vlog("done writing to file")
	atomic.AddInt32(&api.outstandingRequests, 1)
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

	if err := c.workspaceDB.BranchWorkspace(&c.Ctx, src[0], src[1], src[2],
		dst[0], dst[1], dst[2]); err != nil {

		api.queueErrorResponse(quantumfs.ErrorCommandFailed, err.Error())
		return
	}

	api.queueErrorResponse(quantumfs.ErrorOK, "Branch Succeeded")
}

func (api *ApiHandle) getAccessed(c *ctx, buf []byte) {
	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
		return
	}

	wsr := cmd.WorkspaceRoot
	parts := strings.Split(wsr, "/")
	workspace, ok := c.qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	if !ok {
		api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"WorkspaceRoot %s does not exist or is not active", wsr)
		return
	}

	accessList := workspace.getList()
	api.queueAccesslistResponse(accessList)
}

func (api *ApiHandle) clearAccessed(c *ctx, buf []byte) {
	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
		return
	}

	wsr := cmd.WorkspaceRoot
	parts := strings.Split(wsr, "/")
	workspace, ok := c.qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	if !ok {
		api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"WorkspaceRoot %s does not exist or is not active", wsr)
		return
	}

	workspace.clearList()
	api.queueErrorResponse(quantumfs.ErrorOK, "Clear AccessList Succeeded")
}

func (api *ApiHandle) syncAll(c *ctx) {
	c.qfs.syncAll(c)
	api.queueErrorResponse(quantumfs.ErrorOK, "SyncAll Succeeded")
}

func (api *ApiHandle) insertInode(c *ctx, buf []byte) {
	c.vlog("Api::insertInode Enter")
	defer c.vlog("Api::insertInode Exit")

	var cmd quantumfs.InsertInodeRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
		return
	}

	dst := strings.Split(cmd.DstPath, "/")
	key, type_, size, err := quantumfs.DecodeExtendedKey(cmd.Key)
	permissions := cmd.Permissions
	uid := quantumfs.ObjectUid(c.Ctx, uint32(cmd.Uid), uint32(cmd.Uid))
	gid := quantumfs.ObjectGid(c.Ctx, uint32(cmd.Gid), uint32(cmd.Gid))

	wsr := dst[0] + "/" + dst[1] + "/" + dst[2]
	workspace, ok := c.qfs.getWorkspaceRoot(c, dst[0], dst[1], dst[2])
	if !ok {
		api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"WorkspaceRoot %s does not exist or is not active", wsr)
		return
	}

	if len(dst) == 3 { // only have typespace/namespace/workspace
		// duplicate the entire workspace root is illegal
		api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"WorkspaceRoot can not be duplicated")
		return
	}

	if key.Type() != quantumfs.KeyTypeEmbedded {
		if buffer := c.dataStore.Get(&c.Ctx, key); buffer == nil {
			api.queueErrorResponse(quantumfs.ErrorKeyNotFound,
				"Key does not exist in the datastore")
			return
		}
	}

	// get immediate parent of the target node
	p, err := func() (Inode, error) {
		// The ApiInode uses tree lock of NamespaceList and not any
		// particular workspace. Thus at this point in the code, we don't
		// have the tree lock on the WorkspaceRoot. Hence, it is safe and
		// necessary to get the tree lock of the WorkspaceRoot exclusively
		// here.
		defer (&workspace.Directory).LockTree().Unlock()
		return (&workspace.Directory).followPath_DOWN(c, dst)
	}()
	if err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Path %s does not exist", cmd.DstPath)
		return
	}

	parent := p.(*Directory)
	target := dst[len(dst)-1]

	defer parent.Lock().Unlock()
	if record := parent.children.recordByName(c, target); record != nil {
		c.vlog("Removing target in preparation for replacement")
		parent.delChild_(c, target)
	}

	c.vlog("Api::insertInode put key %v into node %d - %s",
		key.Value(), parent.inodeNum(), parent.InodeCommon.name_)

	parent.duplicateInode_(c, target, permissions, 0, 0, size,
		quantumfs.UID(uid), quantumfs.GID(gid),
		type_, key)
	parent.self.dirty(c)

	api.queueErrorResponse(quantumfs.ErrorOK, "Insert Inode Succeeded")
}

func (api *ApiHandle) deleteWorkspace(c *ctx, buf []byte) {
	var cmd quantumfs.DeleteWorkspaceRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
		return
	}

	parts := strings.Split(cmd.WorkspacePath, "/")

	if err := c.workspaceDB.DeleteWorkspace(&c.Ctx, parts[0], parts[1],
		parts[2]); err != nil {

		api.queueErrorResponse(quantumfs.ErrorCommandFailed, err.Error())
		return
	}

	api.queueErrorResponse(quantumfs.ErrorOK, "Workspace deletion succeeded")
}
