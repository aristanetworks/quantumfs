// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "fmt"
import "encoding/binary"
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
	CmdError           = iota
	CmdBranchRequest   = iota
	CmdGetAccessed     = iota
	CmdClearAccessed   = iota
	CmdSyncAll         = iota
	CmdDuplicateObject = iota
)

// The various error codes
const (
	ErrorOK            = iota // Command Successful
	ErrorBadJson       = iota // Failed to parse command
	ErrorBadCommandId  = iota // Unknown command ID
	ErrorCommandFailed = iota // The Command failed, see the error for more info
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

type DuplicateObject struct {
	CommandCommon
	Dst       string
	ObjectKey []byte
	Attribute []byte
	Size      uint64
}

func (api *Api) sendCmd(buf []byte) ([]byte, error) {
	err := writeAll(api.fd, buf)
	if err != nil {
		return nil, err
	}

	api.fd.Seek(0, 0)
	buf = make([]byte, 4096)
	n, err := api.fd.Read(buf)
	if err != nil {
		return nil, err
	}

	buf = buf[:n]
	return buf, nil
}

// branch the src workspace into a new workspace called dst.
func (api *Api) Branch(src string, dst string) error {
	if !isWorkspaceNameValid(src) {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", src)
	}

	if !isWorkspaceNameValid(dst) {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", dst)
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
func (api *Api) GetAccessed(wsr string) error {
	if !isWorkspaceNameValid(wsr) {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdGetAccessed},
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

	var accesslistResponse AccessListResponse
	err = json.Unmarshal(buf, &accesslistResponse)
	if err != nil {
		return err
	}

	printAccessList(accesslistResponse.AccessList)
	return nil
}

// clear the list of accessed files in workspaceroot
func (api *Api) ClearAccessed(wsr string) error {
	if !isWorkspaceNameValid(wsr) {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", wsr)
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

// duplicate an object with a given ObjectKey and path
func (api *Api) DuplicateObject(dst string, objectKey []byte, mode uint32,
	umask uint32, rdev uint32, uid uint16, gid uint16) error {

	if !isWorkspacePathValid(dst) {
		return fmt.Errorf("\"%s\" must contain at least one \"/\"\n", dst)
	}

	if !isObjectKeyValid(objectKey) {
		return fmt.Errorf("\"%s\" must be 22 bytes", objectKey)
	}

	attr := make([]byte, 16)
	binary.LittleEndian.PutUint32(attr[0:4], mode)
	binary.LittleEndian.PutUint32(attr[4:8], umask)
	binary.LittleEndian.PutUint32(attr[8:12], rdev)
	binary.LittleEndian.PutUint16(attr[12:14], uid)
	binary.LittleEndian.PutUint16(attr[14:16], gid)

	cmd := DuplicateObject{
		CommandCommon: CommandCommon{CommandId: CmdDuplicateObject},
		Dst:           dst,
		ObjectKey:     objectKey,
		Attribute:     attr,
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
	if slashes := strings.Count(wsr, "/"); slashes != 1 {
		return false
	}
	return true
}

func isWorkspacePathValid(dst string) bool {
	if slashes := strings.Count(dst, "/"); slashes < 1 {
		return false
	}
	return true
}

func isObjectKeyValid(objectKey []byte) bool {
	if length := len(objectKey); length != 30 {
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
