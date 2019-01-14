// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package quantumfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/aristanetworks/quantumfs/libqfs"
	"github.com/aristanetworks/quantumfs/utils"
)

// This file contains all the functions which are used by qfs (and other
// applications) to perform special quantumfs operations. Primarily this is done by
// marhalling the arguments and passing them to quantumfsd for processing, then
// interpreting the results.

// NewApi searches for the QuantumFS API files. The search order is:
// 1. The path in the environment variable QUANTUMFS_API_PATH, ie "/qfs/api"
// 2. The api file at the root of the sole mounted QuantumFS instance. If more than
//    one instance is mounted none of them will be used.
// 3. Searching upwards in the directory tree for the api file
func NewApi() (Api, error) {
	path, err := libqfs.FindApiPath()
	if err != nil {
		return nil, err
	}
	return NewApiWithPath(path)
}

func NewApiWithPath(path string) (Api, error) {
	api := apiImpl{}

	fd, err := os.OpenFile(path, os.O_RDWR|syscall.O_DIRECT, 0)
	api.fd = fd
	if err != nil {
		return nil, err
	}

	return &api, nil
}

// A description of the files and directories which were accessed within a particular
// workspace on a single instance.
//
// - readdir() marks a directory as read
// - read() marks a file as read
// - write() marks a file as updated
// - truncation marks a file as updated
// - mkdir() marks a directory as created
// - rmdir() marks a directory as deleted
// - creat() or mknod() marks a file as created
// - unlink() marks a file as unlinked
// - There are no operations that can mark a directory as updated
//
// Creation and deletion have additional special semantics in that they
// are collapsible. Specifically:
//
// - Files which are created and then deleted are removed from the listing
//   under the assumption they are temporary files and of no interest.
// - Files which are deleted and then created are recorded as being
//   truncated. That is, neither created nor deleted, but updated.
// - Directories which are created and then deleted are removed from the
//   listing under the assumption they are temporary and of no interest.
// - Directories which are deleted and then created are listed as being
//   neither created nor deleted. If the directory had previously been
//   read it is included in the list with that flag, otherwise it is
//   removed from the list.
//
// Simply accessing the attributes of a file or directory does not add it to the
// accessed list. Similarly, accessing the attribute of a file does not mark the
// containing directory as read. Opening a file or directory also does not mark it as
// read.
type PathsAccessed struct {
	Paths map[string]PathFlags
}

func NewPathsAccessed() PathsAccessed {
	return PathsAccessed{
		Paths: map[string]PathFlags{},
	}
}

const (
	PathCreated = (1 << 0)
	PathRead    = (1 << 1)
	PathUpdated = (1 << 2)
	PathDeleted = (1 << 3)
	PathIsDir   = (1 << 4)
)

type PathFlags uint

func (pf PathFlags) Created() bool {
	return utils.BitFlagsSet(uint(pf), PathCreated)
}

func (pf PathFlags) Read() bool {
	return utils.BitFlagsSet(uint(pf), PathRead)
}

func (pf PathFlags) Updated() bool {
	return utils.BitFlagsSet(uint(pf), PathUpdated)
}

func (pf PathFlags) Deleted() bool {
	return utils.BitFlagsSet(uint(pf), PathDeleted)
}

func (pf PathFlags) IsDir() bool {
	return utils.BitFlagsSet(uint(pf), PathIsDir)
}

type Api interface {
	Close()

	// Branch the src workspace into a new workspace called dst
	Branch(src string, dst string) error

	// A two way merge is equivalent to a three way merge where the base is
	// the null (empty) workspace
	Merge(remote string, local string) error

	// Local takes precedence if remote and local have a conflict and matching
	// modification times. It is also the workspace who is Advanced to the
	// resulting ID.
	Merge3Way(base string, remote string, local string,
		conflictPreference int, skipPaths []string) error

	// Get the list of accessed file from workspaceroot
	GetAccessed(wsr string) (*PathsAccessed, error)

	// Clear the list of accessed files in workspaceroot
	ClearAccessed(wsr string) error

	// Sync all the active workspaces
	SyncAll() error

	// Sync a specific workspace
	SyncWorkspace(workspace string) error

	// Duplicate an object with a given key and path
	InsertInode(dst string, key string, permissions uint32, uid uint32,
		gid uint32) error

	// Enable the chosen workspace mutable
	//
	// dst is the path relative to the filesystem root, ie. user/joe/myws
	EnableRootWrite(dst string) error

	// Make the chosen workspace irreversibly immutable
	//
	// workspacepath is the path relative to the filesystem root,
	// ie. user/joe/myws
	SetWorkspaceImmutable(workspacepath string) error

	// Delete the given workspace.
	//
	// workspacepath is the path relative to the filesystem root,
	// ie. user/joe/myws
	DeleteWorkspace(workspacepath string) error

	// Store a block in the datastore with the given key.
	//
	// The namespace this block is uploaded into the datastore is separate from
	// the other objects uploaded by QuantumFS.
	SetBlock(key []byte, data []byte) error

	// Retrieve a block in the datastore stored using SetBlock() using the given
	// key.
	GetBlock(key []byte) ([]byte, error)

	// Notify the end the use of a workspace
	WorkspaceFinished(workspace string) error

	// For testing only, may be removed in the future
	AdvanceWSDB(workspace string, refWorkspace string) error
	Refresh(workspace string) error
}

type apiImpl struct {
	fdMutex utils.DeferableMutex
	fd      *os.File
}

func (api *apiImpl) Close() {
	defer api.fdMutex.Lock().Unlock()
	api.fd.Close()
	api.fd = nil
}

type CommandCommon struct {
	CommandId uint32 // One of CmdType*
}

// The various command ID constants
// IMPORTANT: please do not change the order/values of the above constants, QFSClient
// depends on the fact that the values should not change !!!!!
const (
	CmdInvalid               = 0
	CmdError                 = 1
	CmdBranchRequest         = 2
	CmdGetAccessed           = 3
	CmdClearAccessed         = 4
	CmdSyncAll               = 5
	CmdInsertInode           = 6
	CmdDeleteWorkspace       = 7
	CmdSetBlock              = 8
	CmdGetBlock              = 9
	CmdEnableRootWrite       = 10
	CmdSetWorkspaceImmutable = 11
	CmdMergeWorkspaces       = 12
	CmdSyncWorkspace         = 13
	CmdWorkspaceFinished     = 14

	// The following commands might be removed in the future versions so we
	// do not allocate a known id for them
	CmdRefreshWorkspace = 10001 + iota
	CmdAdvanceWSDB
)

// The various error codes
// IMPORTANT: please do not change the order/values of the above constants, QFSClient
// depends on the fact that the values should not change !!!!!
const (
	ErrorOK                = 0 // Command Successful
	ErrorBadArgs           = 1 // The argument is wrong
	ErrorBadJson           = 2 // Failed to parse command
	ErrorBadCommandId      = 3 // Unknown command ID
	ErrorCommandFailed     = 4 // The Command failed, see the error for info
	ErrorKeyNotFound       = 5 // The extended key isn't stored in datastore
	ErrorBlockTooLarge     = 6 // SetBlock was passed a block that's too large
	ErrorWorkspaceNotFound = 7 // The workspace cannot be found in QuantumFS
)

const BufferSize = 4096

type ErrorResponse struct {
	CommandCommon
	ErrorCode uint32
	Message   string
}

type AccessListResponse struct {
	ErrorResponse
	PathList PathsAccessed
}

type BranchRequest struct {
	CommandCommon
	Src string
	Dst string
}

// How to handle conflicts when both sides of the merge have differences versus
// the base.
const (
	PreferNewer  = 0 // Most like filesystem semantics
	PreferRemote = 1
	PreferLocal  = 2
)

type MergeRequest struct {
	CommandCommon
	BaseWorkspace      string
	RemoteWorkspace    string
	LocalWorkspace     string
	ConflictPreference int // One of Prefer* above

	// Paths within workspace to always choose local. Relative to the workspace
	// root, ie. usr/lib/python2.7/site-packages or usr/bin/ls
	SkipPaths []string
}

type RefreshRequest struct {
	CommandCommon
	Workspace string
}

type AdvanceWSDBRequest struct {
	CommandCommon
	Workspace          string
	ReferenceWorkspace string
}

type AccessedRequest struct {
	CommandCommon
	WorkspaceRoot string
}

type SyncAllRequest struct {
	CommandCommon
}

type SyncWorkspaceRequest struct {
	CommandCommon
	Workspace string
}

type InsertInodeRequest struct {
	CommandCommon
	DstPath     string
	Key         string
	Uid         uint32
	Gid         uint32
	Permissions uint32
}

type EnableRootWriteRequest struct {
	CommandCommon
	Workspace string
}

type DeleteWorkspaceRequest struct {
	CommandCommon
	WorkspacePath string
}

type SetBlockRequest struct {
	CommandCommon
	Key  []byte
	Data []byte
}

type GetBlockRequest struct {
	CommandCommon
	Key []byte
}

type GetBlockResponse struct {
	ErrorResponse
	Data []byte
}

type SetWorkspaceImmutableRequest struct {
	CommandCommon
	WorkspacePath string
}

type WorkspaceFinishedRequest struct {
	CommandCommon
	WorkspacePath string
}

func (api *apiImpl) sendCmd(buf []byte) ([]byte, error) {
	defer api.fdMutex.Lock().Unlock()
	err := utils.WriteAll(api.fd, buf)
	if err != nil {
		return nil, err
	}

	api.fd.Seek(0, 0)
	size := BufferSize
	buf = make([]byte, BufferSize)
	result := make([]byte, 0)
	for size == BufferSize {
		size, err = api.fd.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		result = append(result, buf[:size]...)
	}

	return bytes.TrimRight(result, "\u0000"), nil
}

func (api *apiImpl) processCmd(cmd interface{}, res interface{}) error {
	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	buf, err := api.sendCmd(cmdBuf)
	if err != nil {
		return err
	}

	if res == nil {
		var errorResponse ErrorResponse
		err = json.Unmarshal(buf, &errorResponse)
		if err != nil {
			return fmt.Errorf("%s. buffer: %q", err.Error(), buf)
		}
		if errorResponse.ErrorCode != ErrorOK {
			return fmt.Errorf("qfs command Error:%s",
				errorResponse.Message)
		}
	} else {
		// The client must check res.errorResponse.ErrorCode
		return json.Unmarshal(buf, res)
	}
	return nil
}

func (api *apiImpl) Branch(src string, dst string) error {
	if !isWorkspaceNameValid(src) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", src)
	}

	if !isWorkspaceNameValid(dst) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", dst)
	}

	cmd := BranchRequest{
		CommandCommon: CommandCommon{CommandId: CmdBranchRequest},
		Src:           src,
		Dst:           dst,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) Merge(remote string, local string) error {
	return api.Merge3Way(NullWorkspaceName, remote, local, PreferNewer,
		[]string{})
}

func (api *apiImpl) Merge3Way(base string, remote string, local string,
	prefer int, skipPaths []string) error {

	if !isWorkspaceNameValid(base) {
		return fmt.Errorf("\"%s\" (as base) must be an empty string or "+
			"contain precisely two \"/\"\n", base)
	}

	if !isWorkspaceNameValid(remote) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n",
			remote)
	}

	if !isWorkspaceNameValid(local) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", local)
	}

	cmd := MergeRequest{
		CommandCommon:      CommandCommon{CommandId: CmdMergeWorkspaces},
		BaseWorkspace:      base,
		RemoteWorkspace:    remote,
		LocalWorkspace:     local,
		ConflictPreference: prefer,
		SkipPaths:          skipPaths,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) AdvanceWSDB(workspace string, refWorkspace string) error {
	if !isWorkspaceNameValid(workspace) {
		return fmt.Errorf("\"%s\" must be an empty string or "+
			"contain precisely two \"/\"\n", workspace)
	}

	if !isWorkspaceNameValid(refWorkspace) {
		return fmt.Errorf("\"%s\" must be an empty string or "+
			"contain precisely two \"/\"\n", refWorkspace)
	}
	cmd := AdvanceWSDBRequest{
		CommandCommon:      CommandCommon{CommandId: CmdAdvanceWSDB},
		Workspace:          workspace,
		ReferenceWorkspace: refWorkspace,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) Refresh(workspace string) error {
	if !isWorkspaceNameValid(workspace) {
		return fmt.Errorf("\"%s\" must be an empty string or "+
			"contain precisely two \"/\"\n", workspace)
	}
	cmd := RefreshRequest{
		CommandCommon: CommandCommon{CommandId: CmdRefreshWorkspace},
		Workspace:     workspace,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) GetAccessed(wsr string) (*PathsAccessed, error) {
	if !isWorkspaceNameValid(wsr) {
		return nil,
			fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdGetAccessed},
		WorkspaceRoot: wsr,
	}

	var accesslistResponse AccessListResponse
	err := api.processCmd(cmd, &accesslistResponse)
	if err != nil {
		return nil, err
	}
	errorResponse := accesslistResponse.ErrorResponse
	if errorResponse.ErrorCode != ErrorOK {
		return nil,
			fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}

	return &accesslistResponse.PathList, nil
}

func (api *apiImpl) ClearAccessed(wsr string) error {
	if !isWorkspaceNameValid(wsr) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdClearAccessed},
		WorkspaceRoot: wsr,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) SyncAll() error {
	cmd := SyncAllRequest{
		CommandCommon: CommandCommon{CommandId: CmdSyncAll},
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) SyncWorkspace(workspace string) error {
	if !isWorkspaceNameValid(workspace) {
		return fmt.Errorf("\"%s\" must be an empty string or "+
			"contain precisely two \"/\"\n", workspace)
	}
	cmd := SyncWorkspaceRequest{
		CommandCommon: CommandCommon{CommandId: CmdSyncWorkspace},
		Workspace:     workspace,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) InsertInode(dst string, key string, permissions uint32,
	uid uint32, gid uint32) error {

	if !isWorkspacePathValid(dst) {
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n", dst)
	}

	cmd := InsertInodeRequest{
		CommandCommon: CommandCommon{CommandId: CmdInsertInode},
		DstPath:       dst,
		Key:           key,
		Uid:           uid,
		Gid:           gid,
		Permissions:   permissions,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) EnableRootWrite(dst string) error {
	if !isWorkspaceNameValid(dst) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", dst)
	}

	cmd := EnableRootWriteRequest{
		CommandCommon: CommandCommon{CommandId: CmdEnableRootWrite},
		Workspace:     dst,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) SetWorkspaceImmutable(workspacepath string) error {
	if !isWorkspacePathValid(workspacepath) {
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n",
			workspacepath)
	}

	cmd := SetWorkspaceImmutableRequest{
		CommandCommon: CommandCommon{CommandId: CmdSetWorkspaceImmutable},
		WorkspacePath: workspacepath,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) DeleteWorkspace(workspacepath string) error {
	if !isWorkspacePathValid(workspacepath) {
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n",
			workspacepath)
	}

	cmd := DeleteWorkspaceRequest{
		CommandCommon: CommandCommon{CommandId: CmdDeleteWorkspace},
		WorkspacePath: workspacepath,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) SetBlock(key []byte, data []byte) error {
	cmd := SetBlockRequest{
		CommandCommon: CommandCommon{CommandId: CmdSetBlock},
		Key:           key,
		Data:          data,
	}
	return api.processCmd(cmd, nil)
}

func (api *apiImpl) GetBlock(key []byte) ([]byte, error) {
	cmd := GetBlockRequest{
		CommandCommon: CommandCommon{CommandId: CmdGetBlock},
		Key:           key,
	}
	var getBlockResponse GetBlockResponse
	err := api.processCmd(cmd, &getBlockResponse)
	if err != nil {
		return nil, err
	}

	errorResponse := getBlockResponse.ErrorResponse
	if errorResponse.ErrorCode != ErrorOK {
		return nil, fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}

	return getBlockResponse.Data, nil
}

func (api *apiImpl) WorkspaceFinished(workspacepath string) error {
	if !isWorkspacePathValid(workspacepath) {
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n",
			workspacepath)
	}

	cmd := WorkspaceFinishedRequest{
		CommandCommon: CommandCommon{CommandId: CmdWorkspaceFinished},
		WorkspacePath: workspacepath,
	}
	return api.processCmd(cmd, nil)
}

func isWorkspaceNameValid(wsr string) bool {
	if slashes := strings.Count(wsr, "/"); slashes != 2 {
		return false
	}
	return true
}

func isWorkspacePathValid(dst string) bool {
	if slashes := strings.Count(dst, "/"); slashes < 2 {
		return false
	}
	return true
}
