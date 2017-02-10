// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "fmt"
import "encoding/json"
import "os"
import "strings"
import "syscall"

// This file contains all the functions which are used by qfs (and other
// applications) to perform special quantumfs operations. Primarily this is done by
// marhalling the arguments and passing them to quantumfsd for processing, then
// interpretting the results.

func NewApi() *Api {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	directories := strings.Split(cwd, "/")
	path := ""

	for {
		path = strings.Join(directories, "/") + "/" + ApiPath
		stat, err := os.Lstat(path)
		if err != nil {
			if len(directories) == 1 {
				// We didn't find anything and hit the root, give up
				panic("Couldn't find api file")
			}
			directories = directories[:len(directories)-1]
			continue
		}
		if !stat.IsDir() {
			stat_t := stat.Sys().(*syscall.Stat_t)
			if stat_t.Ino == InodeIdApi {
				// No real filesystem is likely to give out inode 2
				// for a random file but quantumfs reserves that
				// inode for all the api files.
				break
			}
		}
	}

	return NewApiWithPath(path)
}
func NewApiWithPath(path string) *Api {
	api := Api{}

	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	api.fd = fd
	if err != nil {
		panic(err)
	}

	return &api
}

type Api struct {
	fd *os.File
}

func (api *Api) Close() {
	api.fd.Close()
}

func writeAll(fd *os.File, data []byte) error {
	for {
		size, err := fd.Write(data)
		if err != nil {
			return err
		}

		if len(data) == size {
			return nil
		}

		data = data[size:]
	}
}

type CommandCommon struct {
	CommandId uint32 // One of CmdType*
}

// The various command ID constants
const (
	CmdError         = iota
	CmdBranchRequest = iota
	CmdGetAccessed   = iota
	CmdClearAccessed = iota
	CmdSyncAll       = iota
	CmdInsertInode   = iota
)

// The various error codes
const (
	ErrorOK            = iota // Command Successful
	ErrorBadArgs       = iota // The argument is wrong
	ErrorBadJson       = iota // Failed to parse command
	ErrorBadCommandId  = iota // Unknown command ID
	ErrorCommandFailed = iota // The Command failed, see the error for more info
	ErrorKeyNotFound   = iota // The extended key is not stored in the datastore
)

type ErrorResponse struct {
	CommandCommon
	ErrorCode uint32
	Message   string
}

type AccessListResponse struct {
	ErrorResponse
	AccessList map[string]bool
}

type BranchRequest struct {
	CommandCommon
	Src string
	Dst string
}

type AccessedRequest struct {
	CommandCommon
	WorkspaceRoot string
}

type SyncAllRequest struct {
	CommandCommon
}

type InsertInodeRequest struct {
	CommandCommon
	DstPath     string
	Key         string
	Uid         uint32
	Gid         uint32
	Permissions uint32
}

func (api *Api) sendCmd(buf []byte) ([]byte, error) {
	err := writeAll(api.fd, buf)
	if err != nil {
		return nil, err
	}

	api.fd.Seek(0, 0)
	size := 5000
	result := make([]byte, 0)
	buf = make([]byte, 5000)
	for size == 5000 {
		size, err = api.fd.Read(buf)
		if err != nil {
			return nil, err
		}

		result = append(result, buf[:size]...)
	}

	return result, nil
}

// branch the src workspace into a new workspace called dst.
func (api *Api) Branch(src string, dst string) error {
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

	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	buf, err := api.sendCmd(cmdBuf)
	if err != nil {
		return err
	}

	var errorResponse ErrorResponse
	err = json.Unmarshal(buf, &errorResponse)
	if err != nil {
		return err
	}
	if errorResponse.ErrorCode != ErrorOK {
		return fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}

	return nil
}

// Get the list of accessed file from workspaceroot
func (api *Api) GetAccessed(wsr string) (map[string]bool, error) {
	if !isWorkspaceNameValid(wsr) {
		return nil, fmt.Errorf("\"%s\" must contain precisely two \"/\"\n",
			wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdGetAccessed},
		WorkspaceRoot: wsr,
	}

	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	buf, err := api.sendCmd(cmdBuf)
	if err != nil {
		return nil, err
	}

	var errorResponse ErrorResponse
	err = json.Unmarshal(buf, &errorResponse)
	if err != nil {
		return nil, fmt.Errorf("The length of result: %d", len(buf))
	}
	if errorResponse.ErrorCode != ErrorOK {
		return nil, fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}

	var accesslistResponse AccessListResponse
	err = json.Unmarshal(buf, &accesslistResponse)
	if err != nil {
		return nil, err
	}

	printAccessList(accesslistResponse.AccessList)
	return accesslistResponse.AccessList, nil
}

// clear the list of accessed files in workspaceroot
func (api *Api) ClearAccessed(wsr string) error {
	if !isWorkspaceNameValid(wsr) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdClearAccessed},
		WorkspaceRoot: wsr,
	}

	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	buf, err := api.sendCmd(cmdBuf)
	if err != nil {
		return err
	}

	var errorResponse ErrorResponse
	err = json.Unmarshal(buf, &errorResponse)
	if err != nil {
		return err
	}
	if errorResponse.ErrorCode != ErrorOK {
		return fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}
	return nil
}

// Sync all the active workspaces
func (api *Api) SyncAll() error {
	cmd := SyncAllRequest{
		CommandCommon: CommandCommon{CommandId: CmdSyncAll},
	}

	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	if _, err := api.sendCmd(cmdBuf); err != nil {
		return err
	}

	return nil
}

// duplicate an object with a given key and path
func (api *Api) InsertInode(dst string, key string, permissions uint32,
	uid uint32, gid uint32) error {

	if !isWorkspacePathValid(dst) {
		return fmt.Errorf("\"%s\" must contain at least one \"/\"\n", dst)
	}

	if !isKeyValid(key) {
		return fmt.Errorf("\"%s\" should be %d bytes",
			key, ExtendedKeyLength)
	}

	cmd := InsertInodeRequest{
		CommandCommon: CommandCommon{CommandId: CmdInsertInode},
		DstPath:       dst,
		Key:           key,
		Uid:           uid,
		Gid:           gid,
		Permissions:   permissions,
	}

	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	buf, err := api.sendCmd(cmdBuf)
	if err != nil {
		return err
	}

	var errorResponse ErrorResponse
	err = json.Unmarshal(buf, &errorResponse)
	if err != nil {
		return err
	}
	if errorResponse.ErrorCode != ErrorOK {
		return fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}
	return nil
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

func isKeyValid(key string) bool {
	if length := len(key); length != ExtendedKeyLength {
		return false
	}
	return true
}

func printAccessList(list map[string]bool) {
	fmt.Println("------ Created Files ------")
	for key, val := range list {
		if val {
			fmt.Println(key)
		}
	}
	fmt.Println("------ Accessed Files ------")
	for key, val := range list {
		if !val {
			fmt.Println(key)
		}
	}
}
