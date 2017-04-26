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

func (api *ApiInode) dirty(c *ctx) {
	c.vlog("ApiInode::dirty doing nothing")
	// Override the InodeCommon dirty because the Api can never be changed on the
	// filesystem itself.
}

func (api *ApiInode) Access(c *ctx, mask uint32, uid uint32,
	gid uint32) fuse.Status {

	defer c.FuncIn("ApiInode::Access", "mask %d uid %d gid %d", mask, uid,
		gid).out()

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
	defer c.funcIn("ApiInode::GetAttr").out()
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

func (wsr *ApiInode) getChildRecordCopy(c *ctx,
	inodeNum InodeId) (quantumfs.DirectoryRecord, error) {

	c.elog("Api doesn't support record fetch")
	return &quantumfs.DirectRecord{}, errors.New("Unsupported record fetch")
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

	defer c.FuncIn("ApiInode::Open", "flags %d mode %d", flags, mode).out()

	// Verify that O_DIRECT is actually set
	if !utils.BitAnyFlagSet(uint(syscall.O_DIRECT), uint(flags)) {
		c.dlog("FAIL setting O_DIRECT in File descriptor")
		return fuse.EINVAL
	}

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
	c.vlog("ApiInode::flush doing nothing")
	return quantumfs.EmptyBlockKey
}

func newApiHandle(c *ctx, treeLock *sync.RWMutex) *ApiHandle {
	defer c.funcIn("newApiHandle").out()

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
		size, nonblocking).out()

	// Read() returns nil only if there is no response. There are two cases:
	// 1. The offset is zero and channel api.responses is empty;
	// 2. Buffer api.currentResponse finishes reading.
	if (offset == 0 && len(api.responses) == 0) ||
		(offset > 0 && offset >= uint64(len(api.currentResponse))) {

		if nonblocking {
			c.vlog("No outstanding requests on nonblocking")
			return nil, fuse.Status(syscall.EAGAIN)
		}

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
	c.vlog("API Response size %d with offset  %d", responseSize, offset)

	if responseSize <= maxReturnIndx {
		return fuse.ReadResultData(bytes[offset:]), fuse.OK
	}
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

	message := fmt.Sprintf(format, a)
	bytes := makeErrorResponse(code, message)
	api.responses <- fuse.ReadResultData(bytes)

	return len(bytes)
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
		panic("Failed to marshal API AccessListResponse")
	}
	return bytes
}

func (api *ApiHandle) queueAccesslistResponse(list map[string]bool) int {
	bytes := makeAccessListResponse(list)
	api.responses <- fuse.ReadResultData(bytes)
	return len(bytes)
}

func (api *ApiHandle) Write(c *ctx, offset uint64, size uint32, flags uint32,
	buf []byte) (uint32, fuse.Status) {

	defer c.FuncIn("ApiHandle::Write", "offset %d size %d flags %d", offset,
		size, flags).out()

	var cmd quantumfs.CommandCommon
	err := json.Unmarshal(buf, &cmd)

	if err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
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
	// create an object with a given ObjectKey and path
	case quantumfs.CmdInsertInode:
		c.vlog("Recieved InsertInode request")
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
	case quantumfs.CmdMergeRequest:
		c.vlog("Received merge request")
		responseSize = api.mergeWorkspace(c, buf)
	}

	c.vlog("done writing to file")
	c.qfs.increaseApiFileSize(c, responseSize)
	return size, fuse.OK
}

func (api *ApiHandle) branchWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::branchWorkspace").out()

	var cmd quantumfs.BranchRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	src := strings.Split(cmd.Src, "/")
	dst := strings.Split(cmd.Dst, "/")

	c.vlog("Branching %s/%s/%s to %s/%s/%s", src[0], src[1], src[2], dst[0],
		dst[1], dst[2])

	c.qfs.syncAll(c)

	if err := c.workspaceDB.BranchWorkspace(&c.Ctx, src[0], src[1], src[2],
		dst[0], dst[1], dst[2]); err != nil {

		c.vlog("branch failed: %s", err.Error())
		return api.queueErrorResponse(
			quantumfs.ErrorCommandFailed, err.Error())
	}

	return api.queueErrorResponse(quantumfs.ErrorOK, "Branch Succeeded")
}

func (api *ApiHandle) mergeWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::mergeWorkspace").out()

	var cmd quantumfs.MergeRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	local := strings.Split(cmd.Local, "/")
	localRootId, err := c.workspaceDB.Workspace(&c.Ctx, local[0], local[1],
		local[2])
	if err != nil {
		c.vlog("Workspace not found: %s", local)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active", local)
	}

	remote := strings.Split(cmd.Remote, "/")
	remoteRootId, err := c.workspaceDB.Workspace(&c.Ctx, remote[0], remote[1],
		remote[2])
	if err != nil {
		c.vlog("Workspace not found: %s", remote)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active", remote)
	}

	c.vlog("Merging %s/%s/%s into %s/%s/%s", remote[0], remote[1], remote[2],
		local[0], local[1], local[2])

	nullWsrInode, _ := newNullWorkspaceRoot(c, quantumfs.InodeIdInvalid,
		quantumfs.InodeIdInvalid)
	nullWsr := nullWsrInode.(*NullWorkspaceRoot).WorkspaceRoot
	nullRootId := publishWorkspaceRoot(c, nullWsr.baseLayerId, nullWsr.hardlinks)

	newRootId := mergeWorkspaceRoot(c, nullRootId, remoteRootId, localRootId)
	_, err = c.workspaceDB.AdvanceWorkspace(&c.Ctx, local[0], local[1],
		local[2], localRootId, newRootId)
	if err != nil {
		c.vlog("Workspace rootId advanced after merge began, try again.")
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Workspace rootId advanced after merge began, try again.")
	}


	return api.queueErrorResponse(quantumfs.ErrorOK, "Merge Succeeded")
}

func (api *ApiHandle) getAccessed(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::getAccessed").out()

	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	wsr := cmd.WorkspaceRoot
	parts := strings.Split(wsr, "/")
	workspace, ok := c.qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	if !ok {
		c.vlog("Workspace not found: %s", wsr)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	accessList := workspace.getList()
	return api.queueAccesslistResponse(accessList)
}

func (api *ApiHandle) clearAccessed(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::clearAccessed").out()

	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	wsr := cmd.WorkspaceRoot
	parts := strings.Split(wsr, "/")
	workspace, ok := c.qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
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
	defer c.funcIn("ApiHandle::syncAll").out()

	c.qfs.syncAll(c)
	return api.queueErrorResponse(quantumfs.ErrorOK, "SyncAll Succeeded")
}

func (api *ApiHandle) insertInode(c *ctx, buf []byte) int {
	defer c.funcIn("Api::insertInode").out()

	var cmd quantumfs.InsertInodeRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	dst := strings.Split(cmd.DstPath, "/")
	key, type_, size, err := quantumfs.DecodeExtendedKey(cmd.Key)
	permissions := cmd.Permissions
	uid := quantumfs.ObjectUid(uint32(cmd.Uid), uint32(cmd.Uid))
	gid := quantumfs.ObjectGid(uint32(cmd.Gid), uint32(cmd.Gid))

	if type_ == quantumfs.ObjectTypeDirectoryEntry {
		c.vlog("Attemped to insert a directory")
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"InsertInode with directories is not supporte")
	}

	wsr := dst[0] + "/" + dst[1] + "/" + dst[2]
	workspace, ok := c.qfs.getWorkspaceRoot(c, dst[0], dst[1], dst[2])
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
		c.vlog("Path does not exist: %s", cmd.DstPath)
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Path %s does not exist", cmd.DstPath)
	}

	parent := p.(*Directory)
	target := dst[len(dst)-1]

	defer parent.Lock().Unlock()
	if record := parent.children.recordByName(c, target); record != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"Inode %s should not exist", target)
	}

	c.vlog("Api::insertInode put key %v into node %d - %s",
		key.Value(), parent.inodeNum(), parent.InodeCommon.name_)

	parent.duplicateInode_(c, target, permissions, 0, 0, size,
		quantumfs.UID(uid), quantumfs.GID(gid), type_, key)
	parent.self.dirty(c)

	return api.queueErrorResponse(quantumfs.ErrorOK, "Insert Inode Succeeded")
}

func (api *ApiHandle) deleteWorkspace(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::deleteWorkspace").out()

	var cmd quantumfs.DeleteWorkspaceRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	workspacePath := cmd.WorkspacePath
	parts := strings.Split(workspacePath, "/")
	if err := c.workspaceDB.DeleteWorkspace(&c.Ctx, parts[0], parts[1],
		parts[2]); err != nil {

		c.vlog("DeleteWorkspace failed: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			err.Error())
	}

	// Remove the record of the removed workspace from workspaceMutability map
	c.vlog("updating local mutability cache")
	defer c.qfs.mutabilityLock.Lock().Unlock()
	delete(c.qfs.workspaceMutability, workspacePath)

	return api.queueErrorResponse(quantumfs.ErrorOK,
		"Workspace deletion succeeded")
}

func (api *ApiHandle) enableRootWrite(c *ctx, buf []byte) int {
	defer c.funcIn("Api::enableRootWrite").out()

	var cmd quantumfs.EnableRootWriteRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	workspacePath := cmd.Workspace
	dst := strings.Split(workspacePath, "/")
	exists, _ := c.workspaceDB.WorkspaceExists(&c.Ctx, dst[0], dst[1], dst[2])
	if !exists {
		c.vlog("Workspace does not exist: %s", workspacePath)
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist", workspacePath)
	}

	immutable, err := c.workspaceDB.WorkspaceIsImmutable(&c.Ctx,
		dst[0], dst[1], dst[2])
	if err != nil {
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"Failed to get immutability of WorkspaceRoot %s",
			workspacePath)
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
	defer c.funcIn("ApiHandle::setBlock").out()

	var cmd quantumfs.SetBlockRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	if len(cmd.Key) != quantumfs.HashSize {
		c.vlog("Key incorrect size %d", len(cmd.Key))
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			fmt.Sprintf("Key must be %d bytes", quantumfs.HashSize))
	}

	var hash [quantumfs.HashSize]byte
	copy(hash[:len(hash)], cmd.Key)
	key := quantumfs.NewObjectKey(quantumfs.KeyTypeApi, hash)

	buffer := newBuffer(c, cmd.Data, key.Type())

	err := c.dataStore.durableStore.Set(&c.Ctx, key, buffer)
	if err != nil {
		c.vlog("Setting block in datastore failed: %s", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			err.Error())
	}

	return api.queueErrorResponse(quantumfs.ErrorOK, "Block set succeeded")
}

func (api *ApiHandle) getBlock(c *ctx, buf []byte) int {
	defer c.funcIn("ApiHandle::getBlock").out()

	var cmd quantumfs.GetBlockRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		c.vlog("Error unmarshaling JSON: %s ", err.Error())
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	if len(cmd.Key) != quantumfs.HashSize {
		c.vlog("Key incorrect size %d", len(cmd.Key))
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			fmt.Sprintf("Key must be %d bytes", quantumfs.HashSize))
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
	defer c.funcIn("Api::setWorkspaceImmutable").out()

	var cmd quantumfs.SetWorkspaceImmutableRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	workspacePath := cmd.WorkspacePath
	dst := strings.Split(workspacePath, "/")
	exists, _ := c.workspaceDB.WorkspaceExists(&c.Ctx, dst[0], dst[1], dst[2])
	if !exists {
		return api.queueErrorResponse(quantumfs.ErrorWorkspaceNotFound,
			"WorkspaceRoot %s does not exist", workspacePath)
	}

	err := c.workspaceDB.SetWorkspaceImmutable(&c.Ctx, dst[0], dst[1], dst[2])
	if err != nil {
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"Workspace %s can't be set immutable", workspacePath)
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
