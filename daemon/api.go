// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains all the interaction with the quantumfs API file.

import "encoding/json"
import "fmt"
import "strings"
import "time"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func NewApiInode() Inode {
	api := ApiInode{
		InodeCommon: InodeCommon{id: quantumfs.InodeIdApi},
	}
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

func (api *ApiInode) GetAttr(out *fuse.AttrOut) fuse.Status {
	out.AttrValid = config.CacheTimeSeconds
	out.AttrValidNsec = config.CacheTimeNsecs
	fillApiAttr(&out.Attr)
	return fuse.OK
}

func (api *ApiInode) OpenDir(context fuse.Context, flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	return fuse.ENOTDIR
}

func (api *ApiInode) Open(flags uint32, mode uint32, out *fuse.OpenOut) fuse.Status {
	out.OpenFlags = 0
	handle := newApiHandle()
	globalQfs.setFileHandle(handle.FileHandleCommon.id, handle)
	out.Fh = handle.FileHandleCommon.id
	return fuse.OK
}

func (api *ApiInode) Lookup(context fuse.Context, name string, out *fuse.EntryOut) fuse.Status {
	fmt.Println("Invalid Lookup on ApiInode")
	return fuse.ENOSYS
}

func (api *ApiInode) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	return fuse.ENOTDIR
}

func newApiHandle() *ApiHandle {
	api := ApiHandle{
		FileHandleCommon: FileHandleCommon{
			id:       globalQfs.newFileHandleId(),
			inodeNum: quantumfs.InodeIdApi,
		},
		responses: make(chan fuse.ReadResult, 10),
	}
	return &api
}

// ApiHandle represents the user's interactions with quantumfs and is not necessarily
// synchronized with other api handles.
type ApiHandle struct {
	FileHandleCommon
	responses chan fuse.ReadResult
}

func (api *ApiHandle) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fmt.Println("Invalid ReadDirPlus against ApiHandle")
	return fuse.ENOSYS
}

func (api *ApiHandle) Read(offset uint64, size uint32, buf []byte, nonblocking bool) (fuse.ReadResult, fuse.Status) {
	fmt.Println("Received read request on Api")
	var blocking chan struct{}
	if !nonblocking {
		blocking = make(chan struct{})
	}

	select {
	case response := <-api.responses:
		fmt.Println("Returning", response)
		return response, fuse.OK
	case <-blocking:
		// This is a nonblocking socket, so return that nothing is ready
		fmt.Println("Nonblocking socket, return nothing")
		return nil, fuse.OK
	}
}

func makeErrorResponse(code uint32, message string) []byte {
	response := quantumfs.ErrorResponse{
		CommandCommon: quantumfs.CommandCommon{CommandId: quantumfs.CmdError},
		ErrorCode:     code,
		Message:       message,
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

func (api *ApiHandle) Write(offset uint64, size uint32, flags uint32, buf []byte) (uint32, fuse.Status) {
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
		message := fmt.Sprintf("Invalid message %d to send to quantumfsd", cmd.CommandId)
		api.queueErrorResponse(quantumfs.ErrorBadCommandId, message)

	case quantumfs.CmdBranchRequest:
		fmt.Println("Received branch request")
		api.branchWorkspace(buf)

	}
	return size, fuse.OK
}

func (api *ApiHandle) branchWorkspace(buf []byte) {
	var cmd quantumfs.BranchRequest
	if err := json.Unmarshal(buf, &cmd); err != nil {
		api.queueErrorResponse(quantumfs.ErrorBadJson, err.Error())
		return
	}

	src := strings.Split(cmd.Src, "/")
	dst := strings.Split(cmd.Dst, "/")

	if err := config.WorkspaceDB.BranchWorkspace(src[0], src[1], dst[0], dst[1]); err != nil {
		api.queueErrorResponse(quantumfs.ErrorCommandFailed, err.Error())
		return
	}

	api.queueErrorResponse(quantumfs.ErrorOK, "Branch Succeeded")
}
