// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains all the interaction with the quantumfs API file.

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func NewApiInode(treeLock *TreeLock, parent InodeId) Inode {
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

func fillApiAttr(c *ctx, attr *fuse.Attr) {
	attr.Ino = quantumfs.InodeIdApi
	attr.Size = uint64(atomic.LoadInt64(&c.qfs.apiFileSize))
	attr.Blocks = utils.BlocksRoundUp(attr.Size, statBlockSize)

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

func isKeyValid(key string) bool {
	if length := len(key); length != quantumfs.ExtendedKeyLength {
		return false
	}
	return true
}

func isWorkspaceNameValid(name string) bool {
	parts := strings.Split(name, "/")
	if len(parts) != 3 {
		return false
	}

	for _, part := range parts {
		if len(part) == 0 {
			return false
		}
	}

	return true
}

func (api *ApiInode) dirty(c *ctx) {
	c.vlog("ApiInode::dirty doing nothing")
	// Override the InodeCommon dirty because the Api can never be changed on the
	// filesystem itself.
}

func (api *ApiInode) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.FuncIn("ApiInode::Access", "mask %d uid %d gid %d", mask, uid,
		gid).Out()

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
	defer c.funcIn("ApiInode::GetAttr").Out()
	out.AttrValid = c.config.CacheTimeSeconds
	out.AttrValidNsec = c.config.CacheTimeNsecs
	fillApiAttr(c, &out.Attr)
	return fuse.OK
}

func (api *ApiInode) OpenDir(c *ctx, flags uint32, mode uint32,
	out *fuse.OpenOut) fuse.Status {

	c.vlog("ApiInode::OpenDir doing nothing")
	return fuse.ENOTDIR
}

func (api *ApiInode) Mkdir(c *ctx, name string, input *fuse.MkdirIn,
	out *fuse.EntryOut) fuse.Status {

	c.vlog("ApiInode::Mkdir doing nothing")
	return fuse.ENOTDIR
}

func (api *ApiInode) getChildAttr(c *ctx, inodeNum InodeId, out *fuse.Attr,
	owner fuse.Owner) {

	panic("Api doesn't support record fetch")
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

	defer c.FuncIn("ApiInode::Open", "flags %d mode %d", flags, mode).Out()

	// Verify that O_DIRECT is actually set
	if !utils.BitAnyFlagSet(uint(syscall.O_DIRECT), uint(flags)) {
		c.vlog("FAIL setting O_DIRECT in File descriptor")
		return fuse.EINVAL
	}

	out.OpenFlags = 0
	handle := newApiHandle(c, api.treeLock())
	c.qfs.setFileHandle(c, handle.FileHandleCommon.id, handle)

	c.dlog(OpenedInodeDebug, api.id, handle.id)

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

	c.vlog("ApiInode::GetXAttrSize doing nothing")
	return 0, fuse.ENODATA
}

func (api *ApiInode) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.vlog("ApiInode::GetXAttrData doing nothing")
	return nil, fuse.ENODATA
}

func (api *ApiInode) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.vlog("ApiInode::ListXAttr doing nothing")
	return []byte{}, fuse.OK
}

func (api *ApiInode) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.vlog("ApiInode::SetXAttr doing nothing")
	return fuse.Status(syscall.ENOSPC)
}

func (api *ApiInode) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.vlog("ApiInode::RemoveXAttr doing nothing")
	return fuse.ENODATA
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
	c.vlog("ApiInode::flush doing nothing")
	return quantumfs.EmptyBlockKey
}

func newApiHandle(c *ctx, treeLock *TreeLock) *ApiHandle {
	defer c.funcIn("newApiHandle").Out()

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
	responses       chan fuse.ReadResult
	currentResponse []byte
}

func (api *ApiHandle) ReadDirPlus(c *ctx, input *fuse.ReadIn,
	out *fuse.DirEntryList) fuse.Status {

	c.elog("Invalid ReadDirPlus against ApiHandle")
	return fuse.ENOSYS
}

func (api *ApiHandle) Read(c *ctx, offset uint64, size uint32, buf []byte,
	nonblocking bool) (fuse.ReadResult, fuse.Status) {

	defer c.FuncIn("ApiHandle::Read", "offset %d size %d nonblocking %t", offset,
		size, nonblocking).Out()

	// Read() returns nil only if there is no response. There are two cases:
	// 1. The offset is zero and channel api.responses is empty;
	// 2. Buffer api.currentResponse finishes reading.
	if (offset == 0 && len(api.responses) == 0) ||
		(offset > 0 && offset >= uint64(len(api.currentResponse))) {
		c.vlog("No outstanding requests, returning early")
		return nil, fuse.OK
	}

	if offset == 0 {
		// Subtract the file size of last response
		c.qfs.decreaseApiFileSize(c, len(api.currentResponse))
		response := <-api.responses

		buffer := make([]byte, response.Size())
		bytes, _ := response.Bytes(buffer)
		api.currentResponse = bytes
	}

	bytes := api.currentResponse
	maxReturnIndx := offset + uint64(size)
	responseSize := uint64(len(bytes))
	c.vlog("API Response size %d with offset %d", responseSize, offset)

	if responseSize <= maxReturnIndx {
		c.vlog("API returning len %d '%q'", len(bytes[offset:]),
			bytes[offset:])
		return fuse.ReadResultData(bytes[offset:]), fuse.OK
	}
	c.vlog("API returning '%q'", bytes[offset:maxReturnIndx])
	return fuse.ReadResultData(bytes[offset:maxReturnIndx]), fuse.OK
}

func (api *ApiHandle) drainResponseData(c *ctx) {
	c.qfs.decreaseApiFileSize(c, len(api.currentResponse))

	// In case the queue is not empty
	for len(api.responses) > 0 {
		response := <-api.responses
		c.qfs.decreaseApiFileSize(c, response.Size())
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
		panic("Failed to marshal API error response")
	}
	return bytes
}

func (api *ApiHandle) queueErrorResponse(code uint32, format string,
	a ...interface{}) int {

	message := fmt.Sprintf(format, a...)
	bytes := makeErrorResponse(code, message)
	api.responses <- fuse.ReadResultData(bytes)

	return len(bytes)
}

func makeAccessListResponse(list quantumfs.PathsAccessed) []byte {
	response := quantumfs.AccessListResponse{
		ErrorResponse: quantumfs.ErrorResponse{
			CommandCommon: quantumfs.CommandCommon{
				CommandId: quantumfs.CmdError,
			},
			ErrorCode: quantumfs.ErrorOK,
			Message:   "",
		},
		PathList: list,
	}

	bytes, err := json.Marshal(response)
	if err != nil {
		panic("Failed to marshal API AccessListResponse")
	}
	return bytes
}

func (api *ApiHandle) queueAccesslistResponse(
	pathList quantumfs.PathsAccessed) int {

	bytes := makeAccessListResponse(pathList)
	api.responses <- fuse.ReadResultData(bytes)
	return len(bytes)
}

func (api *ApiHandle) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	defer c.FuncIn("ApiHandle::Write", "offset %d size %d flags %d", offset,
		size, flags).Out()

	var cmd quantumfs.CommandCommon
	err := json.Unmarshal(buf, &cmd)

	if err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		api.queueErrorResponse(quantumfs.ErrorBadJson, "%s", err.Error())
	}

	var responseSize int
	switch cmd.CommandId {
	default:
		c.vlog("Received unknown request")
		responseSize = api.queueErrorResponse(quantumfs.ErrorBadCommandId,
			"Unknown command number %d", cmd.CommandId)

	case quantumfs.CmdError:
		c.vlog("Received error from above")
		responseSize = api.queueErrorResponse(quantumfs.ErrorBadCommandId,
			"Invalid message %d to send to quantumfsd",
			cmd.CommandId)

	case quantumfs.CmdBranchRequest:
		c.vlog("Received branch request")
		responseSize = api.branchWorkspace(c, buf)
	case quantumfs.CmdGetAccessed:
		c.vlog("Received GetAccessed request")
		responseSize = api.getAccessed(c, buf)
	case quantumfs.CmdClearAccessed:
		c.vlog("Received ClearAccessed request")
		responseSize = api.clearAccessed(c, buf)
	case quantumfs.CmdSyncAll:
		c.vlog("Received all workspace sync request")
		responseSize = api.syncAll(c)
	case quantumfs.CmdSyncWorkspace:
		c.vlog("Received workspace sync request")
		responseSize = api.syncWorkspace(c, buf)
	// create an object with a given ObjectKey and path
	case quantumfs.CmdInsertInode:
		c.vlog("Received InsertInode request")
		responseSize = api.insertInode(c, buf)
	case quantumfs.CmdDeleteWorkspace:
		c.vlog("Received DeleteWorkspace request")
		responseSize = api.deleteWorkspace(c, buf)
	case quantumfs.CmdSetBlock:
		c.vlog("Received SetBlock request")
		responseSize = api.setBlock(c, buf)
	case quantumfs.CmdGetBlock:
		c.vlog("Received GetBlock request")
		responseSize = api.getBlock(c, buf)
	case quantumfs.CmdEnableRootWrite:
		c.vlog("Received EnableRootWrite request")
		responseSize = api.enableRootWrite(c, buf)
	case quantumfs.CmdSetWorkspaceImmutable:
		c.vlog("Received SetWorkspaceImmutable request")
		responseSize = api.setWorkspaceImmutable(c, buf)
	case quantumfs.CmdMergeWorkspaces:
		c.vlog("Received merge request")
		responseSize = api.mergeWorkspace(c, buf)
	case quantumfs.CmdWorkspaceFinished:
		c.vlog("Received WorkspaceFinished request")
		responseSize = api.workspaceFinished(c, buf)
	case quantumfs.CmdRefreshWorkspace:
		c.vlog("Received refresh request")
		responseSize = api.refreshWorkspace(c, buf)
	case quantumfs.CmdAdvanceWSDB:
		c.vlog("Received advanceWSDB request")
		responseSize = api.advanceWSDB(c, buf)
	}

	c.vlog("done writing to file")
	c.qfs.increaseApiFileSize(c, responseSize)
	return size, fuse.OK
}

func (api *ApiHandle) branchWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::branchWorkspace").Out()

	var cmd quantumfs.BranchRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.Src) {
		c.vlog("workspace name '%s' is malformed", cmd.Src)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.Src)
	}

	if !isWorkspaceNameValid(cmd.Dst) {
		c.vlog("workspace name '%s' is malformed", cmd.Dst)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.Dst)
	}

	src := strings.Split(cmd.Src, "/")
	dst := strings.Split(cmd.Dst, "/")

	c.vlog("Branching %s/%s/%s to %s/%s/%s", src[0], src[1], src[2], dst[0],
		dst[1], dst[2])

	if err := c.qfs.syncWorkspace(c, cmd.Src); err != nil {
		c.vlog("syncWorkspace failed: %s", err.Error())
		return api.queueErrorResponse(
			quantumfs.ErrorCommandFailed, "%s", err.Error())
	}

	if err := c.workspaceDB.BranchWorkspace(&c.Ctx, src[0], src[1], src[2],
		dst[0], dst[1], dst[2]); err != nil {

		c.vlog("branch failed: %s", err.Error())
		return api.queueErrorResponse(
			quantumfs.ErrorCommandFailed, "%s", err.Error())
	}

	return api.queueErrorResponse(quantumfs.ErrorOK, "Branch Succeeded")
}

func (api *ApiHandle) mergeWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::mergeWorkspace").Out()

	var cmd quantumfs.MergeRequest
	var err error
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.BaseWorkspace) {
		c.vlog("workspace name '%s' is malformed", cmd.BaseWorkspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.BaseWorkspace)
	}
	if !isWorkspaceNameValid(cmd.RemoteWorkspace) {
		c.vlog("workspace name '%s' is malformed", cmd.RemoteWorkspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.RemoteWorkspace)
	}
	if !isWorkspaceNameValid(cmd.LocalWorkspace) {
		c.vlog("workspace name '%s' is malformed", cmd.LocalWorkspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.LocalWorkspace)
	}

	// Fetch base
	c.qfs.syncWorkspace(c, cmd.BaseWorkspace)
	baseRootId := quantumfs.EmptyWorkspaceKey
	base := strings.Split(cmd.BaseWorkspace, "/")
	baseRootId, _, err = c.workspaceDB.Workspace(&c.Ctx, base[0], base[1],
		base[2])
	if err != nil {
		c.vlog("Workspace not fetched (%s): %s", base, err.Error())
		return api.queueErrorResponse(0+
			quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active",
			cmd.BaseWorkspace)
	}

	// Fetch Remote
	c.qfs.syncWorkspace(c, cmd.RemoteWorkspace)
	remote := strings.Split(cmd.RemoteWorkspace, "/")
	remoteRootId, _, err := c.workspaceDB.Workspace(&c.Ctx, remote[0], remote[1],
		remote[2])
	if err != nil {
		c.vlog("Workspace not fetched (%s): %s", remote, err.Error())
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active",
			cmd.RemoteWorkspace)
	}

	// Fetch/prepare Local
	local := strings.Split(cmd.LocalWorkspace, "/")
	localWsr, cleanup, ok := c.qfs.getWorkspaceRoot(c, local[0], local[1],
		local[2])
	defer cleanup()
	if !ok {
		c.vlog("Unable to instantiate local workspace")
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s could not be instantiated (not found)",
			cmd.LocalWorkspace)
	}

	// Prevent local changes while we perform the merge
	defer localWsr.LockTree().Unlock()
	if err := c.qfs.flusher.syncWorkspace_(c, cmd.LocalWorkspace); err != nil {
		c.vlog("Failed flushing local workspace: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadCommandId,
			"Failed flushing local workspace: %s", err.Error())
	}

	localRootId := localWsr.publishedRootId
	localNonce := localWsr.nonce

	c.vlog("Merging %s/%s/%s into %s/%s/%s", remote[0], remote[1], remote[2],
		local[0], local[1], local[2])

	skipPaths := mergeSkipPaths{
		paths: make(map[string]struct{}, len(cmd.SkipPaths)),
	}

	for _, path := range cmd.SkipPaths {
		skipPaths.paths[path] = struct{}{}
	}

	newRootId, err := mergeWorkspaceRoot(c, baseRootId, remoteRootId,
		localRootId, mergePreference(cmd.ConflictPreference), &skipPaths,
		cmd.LocalWorkspace)
	if err != nil {
		c.vlog("Merge failed: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Merge failed: %s", err.Error())
	}

	_, err = c.workspaceDB.AdvanceWorkspace(&c.Ctx, local[0], local[1],
		local[2], localNonce, localRootId, newRootId)
	if err != nil {
		c.vlog("Workspace can't advance after merge began, try again: %s",
			err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Workspace rootId advanced after merge began, try again.")
	}

	localWsr.refresh_(c)

	return api.queueErrorResponse(quantumfs.ErrorOK, "Merge Succeeded")
}

func (api *ApiHandle) refreshWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::refreshWorkspace").Out()

	var cmd quantumfs.RefreshRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}
	c.vlog("Refreshing workspace %s", cmd.Workspace)

	if !isWorkspaceNameValid(cmd.Workspace) {
		c.vlog("workspace name '%s' is malformed", cmd.Workspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.Workspace)
	}

	c.qfs.refreshWorkspace(c, cmd.Workspace)

	return api.queueErrorResponse(quantumfs.ErrorOK, "Refresh Succeeded")
}

func (api *ApiHandle) advanceWSDB(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::advanceWSDB").Out()

	var cmd quantumfs.AdvanceWSDBRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}
	c.vlog("Advancing wsdb of %s to that of %s", cmd.Workspace,
		cmd.ReferenceWorkspace)

	if !isWorkspaceNameValid(cmd.Workspace) {
		c.vlog("workspace name '%s' is malformed", cmd.Workspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.Workspace)
	}

	workspace := strings.Split(cmd.Workspace, "/")
	wsr, cleanup, ok := c.qfs.getWorkspaceRoot(c, workspace[0],
		workspace[1], workspace[2])
	defer cleanup()
	if !ok {
		c.vlog("Workspace not found: %s", cmd.Workspace)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"Workspace %s does not exist or is not active",
			cmd.Workspace)
	}

	ref := strings.Split(cmd.ReferenceWorkspace, "/")
	refRootId, nonce, err := c.workspaceDB.Workspace(&c.Ctx, ref[0], ref[1],
		ref[2])

	if err != nil {
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"Workspace %s does not exist or is not active",
			cmd.ReferenceWorkspace)
	}

	rootId, err := c.workspaceDB.AdvanceWorkspace(&c.Ctx, workspace[0],
		workspace[1], workspace[2], nonce, wsr.publishedRootId, refRootId)

	if err != nil {
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Workspace %s is already at %s",
			cmd.Workspace, rootId.String())
	}

	return api.queueErrorResponse(quantumfs.ErrorOK,
		"AdvanceWSDB Succeeded")
}

func (api *ApiHandle) getAccessed(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::getAccessed").Out()

	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.WorkspaceRoot) {
		c.vlog("workspace name '%s' is malformed", cmd.WorkspaceRoot)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.WorkspaceRoot)
	}

	wsr := cmd.WorkspaceRoot
	dst := strings.Split(wsr, "/")
	workspace, cleanup, ok := c.qfs.getWorkspaceRoot(c, dst[0], dst[1], dst[2])
	defer cleanup()
	if !ok {
		c.vlog("Workspace not found: %s", wsr)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	accessList := workspace.getList(c)
	return api.queueAccesslistResponse(accessList)
}

func (api *ApiHandle) clearAccessed(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::clearAccessed").Out()

	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.WorkspaceRoot) {
		c.vlog("workspace name '%s' is malformed", cmd.WorkspaceRoot)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.WorkspaceRoot)
	}

	wsr := cmd.WorkspaceRoot
	dst := strings.Split(wsr, "/")
	workspace, cleanup, ok := c.qfs.getWorkspaceRoot(c, dst[0], dst[1], dst[2])
	defer cleanup()
	if !ok {
		c.vlog("Workspace not found: %s", wsr)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	workspace.clearList()
	return api.queueErrorResponse(quantumfs.ErrorOK,
		"Clear AccessList Succeeded")
}

func (api *ApiHandle) syncAll(c *ctx) int {
	defer c.funcIn("ApiHandle::syncAll").Out()

	if err := c.qfs.syncAll(c); err != nil {
		c.vlog("Error syncAll %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed, "%s",
			err.Error())

	}
	return api.queueErrorResponse(quantumfs.ErrorOK, "SyncAll Succeeded")
}

func (api *ApiHandle) syncWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::syncWorkspace").Out()

	var cmd quantumfs.SyncWorkspaceRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}
	c.vlog("Syncing workspace %s", cmd.Workspace)

	if !isWorkspaceNameValid(cmd.Workspace) {
		c.vlog("workspace name '%s' is malformed", cmd.Workspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.Workspace)
	}

	if err := c.qfs.syncWorkspace(c, cmd.Workspace); err != nil {
		c.vlog("Error sync %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed, "%s",
			err.Error())

	}
	return api.queueErrorResponse(quantumfs.ErrorOK, "SyncWorkspace Succeeded")
}

func (api *ApiHandle) insertInode(c *ctx, buf []byte) int {
	defer c.funcIn("Api::insertInode").Out()

	var cmd quantumfs.InsertInodeRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isKeyValid(cmd.Key) {
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"key \"%s\" should be %d bytes",
			cmd.Key, quantumfs.ExtendedKeyLength)
	}

	dst := strings.Split(cmd.DstPath, "/")
	key, type_, size, err := quantumfs.DecodeExtendedKey(cmd.Key)

	if err != nil {
		c.vlog("Could not decode key \"%s\". Errror %s",
			cmd.Key, err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Could not decode key \"%s\". Errror %s",
			cmd.Key, err.Error())
	}
	permissions := cmd.Permissions
	uid := quantumfs.ObjectUid(uint32(cmd.Uid), uint32(cmd.Uid))
	gid := quantumfs.ObjectGid(uint32(cmd.Gid), uint32(cmd.Gid))

	if type_ == quantumfs.ObjectTypeDirectory {
		c.vlog("Attempted to insert a directory")
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"InsertInode with directories is not supported")
	}

	wsr := dst[0] + "/" + dst[1] + "/" + dst[2]

	if !isWorkspaceNameValid(wsr) {
		c.vlog("workspace name '%s' is malformed", wsr)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", wsr)
	}

	workspace, cleanup, ok := c.qfs.getWorkspaceRoot(c, dst[0], dst[1], dst[2])
	defer cleanup()
	if !ok {
		c.vlog("Workspace not found: %s", wsr)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	if len(dst) == 3 { // only have typespace/namespace/workspace
		// duplicate the entire workspace root is illegal
		c.vlog("Attempted to insert workspace root")
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"WorkspaceRoot can not be duplicated")
	}

	if key.Type() != quantumfs.KeyTypeEmbedded {
		if buffer := c.dataStore.Get(&c.Ctx, key); buffer == nil {
			c.vlog("Key not found: %s", key.String())
			return api.queueErrorResponse(quantumfs.ErrorKeyNotFound,
				"Key does not exist in the datastore")
		}
	}

	// get immediate parent of the target node
	p, cleanup, err := func() (Inode, func(), error) {
		// The ApiInode uses tree lock of NamespaceList and not any
		// particular workspace. Thus at this point in the code, we don't
		// have the tree lock on the WorkspaceRoot. Hence, it is safe and
		// necessary to get the tree lock of the WorkspaceRoot exclusively
		// here.
		defer workspace.LockTree().Unlock()
		return workspace.followPath_DOWN(c, dst)
	}()
	defer cleanup()
	if err != nil {
		c.vlog("Path does not exist: %s", cmd.DstPath)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Path %s does not exist", cmd.DstPath)
	}

	p, treeUnlock := c.qfs.RLockTreeGetInode(c, p.inodeNum())
	defer treeUnlock.RUnlock()

	// The parent may have been deleted between the search and locking its tree.
	if p == nil {
		c.vlog("Path does not exist: %s", cmd.DstPath)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Path %s does not exist", cmd.DstPath)
	}

	parent := asDirectory(p)
	target := dst[len(dst)-1]

	status := parent.Unlink(c, target)
	if status != fuse.OK && status != fuse.ENOENT {
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Inode %s should not exist, error unlinking %d", target,
			status)
	}

	c.vlog("Api::insertInode put key %v into node %d - %s",
		key.Value(), parent.inodeNum(), parent.InodeCommon.name_)

	err = freshenKeys(c, key, type_)
	if err != nil {
		return api.queueErrorResponse(quantumfs.ErrorKeyNotFound,
			"Unable to freshen all blocks for key: %s", err)
	}

	func() {
		defer parent.Lock().Unlock()
		parent.duplicateInode_(c, target, permissions, 0, 0, size,
			quantumfs.UID(uid), quantumfs.GID(gid), type_, key)
	}()

	parent.updateSize(c, fuse.OK)
	return api.queueErrorResponse(quantumfs.ErrorOK, "Insert Inode Succeeded")
}

func (api *ApiHandle) deleteWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::deleteWorkspace").Out()

	var cmd quantumfs.DeleteWorkspaceRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.WorkspacePath) {
		c.vlog("workspace name '%s' is malformed", cmd.WorkspacePath)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.WorkspacePath)
	}

	workspacePath := cmd.WorkspacePath
	parts := strings.Split(workspacePath, "/")
	if err := c.workspaceDB.DeleteWorkspace(&c.Ctx, parts[0], parts[1],
		parts[2]); err != nil {

		c.vlog("DeleteWorkspace failed: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"%s", err.Error())
	}

	// Remove the record of the removed workspace from workspaceMutability map
	c.vlog("updating local mutability cache")
	defer c.qfs.mutabilityLock.Lock().Unlock()
	delete(c.qfs.workspaceMutability, workspacePath)

	return api.queueErrorResponse(quantumfs.ErrorOK,
		"Workspace deletion succeeded")
}

func (api *ApiHandle) processImmutablilityError(c *ctx, err error,
	workspacePath string, msg string) int {

	switch err := err.(type) {
	default:
		c.wlog("Unknown error type from WorkspaceDB."+
			"WorkspaceIsImmutable: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"%s of WorkspaceRoot %s", msg, workspacePath)
	case quantumfs.WorkspaceDbErr:
		switch err.Code {
		default:
			c.wlog("Unhandled error from WorkspaceDB."+
				"WorkspaceIsImmutable: %s", err.Error())
			return api.queueErrorResponse(
				quantumfs.ErrorCommandFailed,
				"%s of WorkspaceRoot %s", msg, workspacePath)
		case quantumfs.WSDB_WORKSPACE_NOT_FOUND:
			c.vlog("Workspace does not exist: %s", workspacePath)
			return api.queueErrorResponse(
				quantumfs.ErrorWorkspaceNotFound,
				"WorkspaceRoot %s does not exist", workspacePath)
		}
	}
}

func (api *ApiHandle) enableRootWrite(c *ctx, buf []byte) int {
	defer c.funcIn("Api::enableRootWrite").Out()

	var cmd quantumfs.EnableRootWriteRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.Workspace) {
		c.vlog("workspace name '%s' is malformed", cmd.Workspace)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.Workspace)
	}

	workspacePath := cmd.Workspace
	dst := strings.Split(workspacePath, "/")
	immutable, err := c.workspaceDB.WorkspaceIsImmutable(&c.Ctx,
		dst[0], dst[1], dst[2])
	if err != nil {
		return api.processImmutablilityError(c, err, workspacePath,
			"Failed to get immutability")
	}

	c.vlog("Setting immutable")

	defer c.qfs.mutabilityLock.Lock().Unlock()
	if immutable {
		delete(c.qfs.workspaceMutability, workspacePath)
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"WorkspaceRoot has already been set immutable")
	}

	mutability, exists := c.qfs.workspaceMutability[workspacePath]
	if exists && mutability == workspaceImmutableUntilRestart {
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Another user is writing to this workspace. Writes are "+
				"disabled, some changes already made may be lost.")
	}

	c.qfs.workspaceMutability[workspacePath] = workspaceMutable
	return api.queueErrorResponse(quantumfs.ErrorOK,
		"Enable Workspace Write Permission Succeeded")
}

func (api *ApiHandle) setBlock(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::setBlock").Out()

	var cmd quantumfs.SetBlockRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if len(cmd.Key) != quantumfs.HashSize {
		c.vlog("Key incorrect size %d", len(cmd.Key))
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Key must be %d bytes", quantumfs.HashSize)
	}

	var hash [quantumfs.HashSize]byte
	copy(hash[:len(hash)], cmd.Key)
	key := quantumfs.NewObjectKey(quantumfs.KeyTypeApi, hash)

	buffer := newBuffer(c, cmd.Data, key.Type())

	err := c.dataStore.durableStore.Set(&c.Ctx, key, buffer)
	if err != nil {
		c.vlog("Setting block in datastore failed: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed, "%s",
			err.Error())
	}

	return api.queueErrorResponse(quantumfs.ErrorOK, "Block set succeeded")
}

func (api *ApiHandle) getBlock(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::getBlock").Out()

	var cmd quantumfs.GetBlockRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s ", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if len(cmd.Key) != quantumfs.HashSize {
		c.vlog("Key incorrect size %d", len(cmd.Key))
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Key must be %d bytes", quantumfs.HashSize)
	}

	var hash [quantumfs.HashSize]byte
	copy(hash[:len(hash)], cmd.Key)
	key := quantumfs.NewObjectKey(quantumfs.KeyTypeApi, hash)

	buffer := c.dataStore.Get(&c.Ctx, key)
	if buffer == nil {
		c.vlog("Datastore returned no data")
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Nil buffer returned from datastore")
	}

	response := quantumfs.GetBlockResponse{
		ErrorResponse: quantumfs.ErrorResponse{
			CommandCommon: quantumfs.CommandCommon{
				CommandId: quantumfs.CmdError,
			},
			ErrorCode: quantumfs.ErrorOK,
			Message:   "",
		},
		Data: buffer.Get(),
	}

	bytes, err := json.Marshal(response)
	if err != nil {
		panic("Failed to marshal API GetBlockResponse")
	}

	c.vlog("Data length %d, response length %d", buffer.Size(), len(bytes))
	api.responses <- fuse.ReadResultData(bytes)
	return len(bytes)
}

func (api *ApiHandle) setWorkspaceImmutable(c *ctx, buf []byte) int {
	defer c.funcIn("Api::setWorkspaceImmutable").Out()

	var cmd quantumfs.SetWorkspaceImmutableRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}

	if !isWorkspaceNameValid(cmd.WorkspacePath) {
		c.vlog("workspace name '%s' is malformed", cmd.WorkspacePath)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.WorkspacePath)
	}

	workspacePath := cmd.WorkspacePath
	dst := strings.Split(workspacePath, "/")
	err := c.workspaceDB.SetWorkspaceImmutable(&c.Ctx, dst[0], dst[1], dst[2])
	if err != nil {
		return api.processImmutablilityError(c, err, workspacePath,
			"Failed to set immutability")
	}

	defer c.qfs.mutabilityLock.Lock().Unlock()

	mutability, exists := c.qfs.workspaceMutability[workspacePath]
	if exists && mutability == workspaceImmutableUntilRestart {
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Another user is writing to this workspace. Writes are "+
				"disabled, some changes already made may be lost.")
	}

	delete(c.qfs.workspaceMutability, workspacePath)

	return api.queueErrorResponse(quantumfs.ErrorOK,
		"Making workspace immutable succeeded")
}

const WorkspaceFinishedFormat = "Workspace %s finished"

func (api *ApiHandle) workspaceFinished(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::workspaceFinished").Out()

	var cmd quantumfs.WorkspaceFinishedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, "%s",
			err.Error())
	}
	if !isWorkspaceNameValid(cmd.WorkspacePath) {
		c.vlog("workspace name '%s' is malformed", cmd.WorkspacePath)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"workspace name '%s' is malformed", cmd.WorkspacePath)
	}

	c.vlog(WorkspaceFinishedFormat, cmd.WorkspacePath)

	return api.queueErrorResponse(quantumfs.ErrorOK,
		"WorkspaceFinished Succeeded")
}
