// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains all the interaction with the quantumfs API file.
import "encoding/json"
import "encoding/binary"
import "errors"
import "fmt"
import "strings"
import "sync"
import "sync/atomic"
import "syscall"
import "time"

import "github.com/aristanetworks/quantumfs"
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
	assert(api.treeLock() != nil, "ApiInode treeLock is nil at init")
	return &api
}

type ApiInode struct {
	InodeCommon
}

func fillApiAttr(c *ctx, attr *fuse.Attr) {
	attr.Ino = quantumfs.InodeIdApi
	attr.Size = uint64(atomic.LoadInt64(&c.qfs.apiFileSize))
	attr.Blocks = BlocksRoundUp(attr.Size, statBlockSize)

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
	fillApiAttr(c, &out.Attr)
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

	c.elog("Invalid GetXAttrSize on ApiInode")
	return 0, fuse.ENODATA
}

func (api *ApiInode) GetXAttrData(c *ctx,
	attr string) (data []byte, result fuse.Status) {

	c.elog("Invalid GetXAttrData on ApiInode")
	return nil, fuse.ENODATA
}

func (api *ApiInode) ListXAttr(c *ctx) (attributes []byte, result fuse.Status) {
	c.elog("Invalid ListXAttr on ApiInode")
	return []byte{}, fuse.OK
}

func (api *ApiInode) SetXAttr(c *ctx, attr string, data []byte) fuse.Status {
	c.elog("Invalid SetXAttr on ApiInode")
	return fuse.Status(syscall.ENOSPC)
}

func (api *ApiInode) RemoveXAttr(c *ctx, attr string) fuse.Status {
	c.elog("Invalid RemoveXAttr on ApiInode")
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
	outstandingRequests int32
	responses           chan fuse.ReadResult
	responseBuffer      []byte
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
		// Sometime the kernel may request the whole response, but the client
		// does not get all from kernel, so it will send other read request.
		// In this case, offset is non-zero, it indicate that the read has
		// not done, so the outstanding Requests should be put back by 1.
		if offset > 0 {
			atomic.AddInt32(&api.outstandingRequests, 1)
		} else {
			if nonblocking {
				return nil, fuse.Status(syscall.EAGAIN)
			}

			c.vlog("No outstanding requests, returning early")
			return nil, fuse.OK
		}
	}

	if offset == 0 {
		// Subtract the file size of last time read
		c.qfs.decreaseApiFileSize(c, len(api.responseBuffer))
		response := <-api.responses

		// The kernel is supposed to be able to pass the correct size and
		// response back to the client. However, when it tries to deal with
		// multiple partial reads in parallel, it is unble to pass the right
		// size to each of the request clients and I cannot find out why. The
		// current solution is to append 4 more byte, indicating the actual
		// size of the response, in front of the actual response. When the
		// client receives the response, they know the size of the response
		buffer := make([]byte, response.Size())
		tmp, _ := response.Bytes(buffer)
		bytes := make([]byte, 4)
		length := len(tmp) + 4
		binary.LittleEndian.PutUint32(bytes, uint32(length))
		bytes = append(bytes, tmp...)
		api.responseBuffer = bytes
	}

	bytes := api.responseBuffer
	bufSize := offset + uint64(size)
	responseSize := uint64(len(bytes))
	c.vlog("API Response szie %d with offset  %d", responseSize, offset)
	if responseSize <= offset {
		// In case of slice out of boundary
		return nil, fuse.OK
	}
	if responseSize <= bufSize {
		atomic.AddInt32(&api.outstandingRequests, -1)
		return fuse.ReadResultData(bytes[offset:]), fuse.OK
	}
	return fuse.ReadResultData(bytes[offset:bufSize]), fuse.OK
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
		panic("Failed to marshall API AccessListResponse")
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
	c.vlog("writing to file")

	var cmd quantumfs.CommandCommon
	err := json.Unmarshal(buf, &cmd)

	if err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	var responseSize int
	switch cmd.CommandId {
	default:
		responseSize = api.queueErrorResponse(quantumfs.ErrorBadCommandId,
			"Unknown command number %d", cmd.CommandId)

	case quantumfs.CmdError:
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
	}

	c.vlog("done writing to file")
	c.qfs.increaseApiFileSize(c, responseSize)
	atomic.AddInt32(&api.outstandingRequests, 1)
	return size, fuse.OK
}

func (api *ApiHandle) branchWorkspace(c *ctx, buf []byte) int {
	var cmd quantumfs.BranchRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	src := strings.Split(cmd.Src, "/")
	dst := strings.Split(cmd.Dst, "/")

	c.qfs.syncAll(c)

	if err := c.workspaceDB.BranchWorkspace(&c.Ctx, src[0], src[1], src[2],
		dst[0], dst[1], dst[2]); err != nil {

		return api.queueErrorResponse(
			quantumfs.ErrorCommandFailed, err.Error())
	}

	return api.queueErrorResponse(quantumfs.ErrorOK, "Branch Succeeded")
}

func (api *ApiHandle) getAccessed(c *ctx, buf []byte) int {
	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	wsr := cmd.WorkspaceRoot
	parts := strings.Split(wsr, "/")
	workspace, ok := c.qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	if !ok {
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	accessList := workspace.getList()
	return api.queueAccesslistResponse(accessList)
}

func (api *ApiHandle) clearAccessed(c *ctx, buf []byte) int {
	var cmd quantumfs.AccessedRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	wsr := cmd.WorkspaceRoot
	parts := strings.Split(wsr, "/")
	workspace, ok := c.qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	if !ok {
		return api.queueErrorResponse(quantumfs.ErrorCommandFailed,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	workspace.clearList()
	return api.queueErrorResponse(quantumfs.ErrorOK,
		"Clear AccessList Succeeded")
}

func (api *ApiHandle) syncAll(c *ctx) int {
	c.qfs.syncAll(c)
	return api.queueErrorResponse(quantumfs.ErrorOK, "SyncAll Succeeded")
}

func (api *ApiHandle) insertInode(c *ctx, buf []byte) int {
	c.vlog("Api::insertInode Enter")
	defer c.vlog("Api::insertInode Exit")

	var cmd quantumfs.InsertInodeRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		return api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
	}

	dst := strings.Split(cmd.DstPath, "/")
	key, type_, size, err := decodeExtendedKey(cmd.Key)
	permissions := cmd.Permissions
	uid := quantumfs.ObjectUid(c.Ctx, uint32(cmd.Uid), uint32(cmd.Uid))
	gid := quantumfs.ObjectGid(c.Ctx, uint32(cmd.Gid), uint32(cmd.Gid))

	wsr := dst[0] + "/" + dst[1] + "/" + dst[2]
	workspace, ok := c.qfs.getWorkspaceRoot(c, dst[0], dst[1], dst[2])
	if !ok {
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"WorkspaceRoot %s does not exist or is not active", wsr)
	}

	if len(dst) == 3 { // only have typespace/namespace/workspace
		// duplicate the entire workspace root is illegal
		return api.queueErrorResponse(quantumfs.ErrorBadArgs,
			"WorkspaceRoot can not be duplicated")
	}

	if key.Type() != quantumfs.KeyTypeEmbedded {
		if buffer := c.dataStore.Get(&c.Ctx, key); buffer == nil {
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
		quantumfs.UID(uid), quantumfs.GID(gid),
		type_, key)

	return api.queueErrorResponse(quantumfs.ErrorOK, "Insert Inode Succeeded")
}
