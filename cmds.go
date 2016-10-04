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
	CmdResponse      = iota
	CmdBranchRequest = iota
	CmdGetAccessed   = iota
	CmdClearAccessed = iota
	CmdSyncAll       = iota
)

// The various error codes
const (
	ErrorOK            = iota // Command Successful
	ErrorBadJson       = iota // Failed to parse command
	ErrorBadCommandId  = iota // Unknown command ID
	ErrorCommandFailed = iota // The Command failed, see the error for more info
)

type GenericResponse struct {
	CommandCommon
	Data []byte
}

type ErrorResponse struct {
	ErrorCode uint32
	Message   string
}

type AccessListResponse struct {
	Data map[string]bool
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

func (api *Api) sendCmd(bytes []byte) (GenericResponse, error) {
	err := writeAll(api.fd, bytes)
	if err != nil {
		return GenericResponse{}, err
	}

	api.fd.Seek(0, 0)
	buf := make([]byte, 4096)
	n, err := api.fd.Read(buf)
	if err != nil {
		return GenericResponse{}, err
	}

	buf = buf[:n]

	var response GenericResponse
	err = json.Unmarshal(buf, &response)
	if err != nil {
		return GenericResponse{}, err
	}

	return response, nil
}

func (api *Api) parseErrorResponse(bytes []byte) (ErrorResponse, error) {
	var response ErrorResponse
	err := json.Unmarshal(bytes, &response)
	if err != nil {
		return ErrorResponse{}, err
	}

	return response, nil
}

func (api *Api) parseAccessedResponse(bytes []byte) (AccessListResponse, error) {
	var response AccessListResponse
	err := json.Unmarshal(bytes, &response)
	if err != nil {
		return AccessListResponse{}, err
	}

	return response, nil
}

// branch the src workspace into a new workspace called dst.
func (api *Api) Branch(src string, dst string) error {
	if slashes := strings.Count(src, "/"); slashes != 1 {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", src)
	}

	if slashes := strings.Count(dst, "/"); slashes != 1 {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", dst)
	}

	cmd := BranchRequest{
		CommandCommon: CommandCommon{CommandId: CmdBranchRequest},
		Src:           src,
		Dst:           dst,
	}

	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	genericResponse, err := api.sendCmd(bytes)
	if err != nil {
		return err
	}

	errorResponse, err := api.parseErrorResponse(genericResponse.Data)
	if err != nil {
		return err
	}
	if errorResponse.ErrorCode != ErrorOK {
		return fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}

	return nil
}

// branch the src workspace into a new workspace called dst.
func (api *Api) GetAccessed(wsr string) error {
	if slashes := strings.Count(wsr, "/"); slashes != 1 {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdGetAccessed},
		WorkspaceRoot: wsr,
	}

	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	genericResponse, err := api.sendCmd(bytes)
	if err != nil {
		return err
	}

	if genericResponse.CommandId == CmdResponse {
		accesslist, err := api.parseAccessedResponse(genericResponse.Data)
		if err == nil {
			fmt.Println("Accesslist of workspace %s:%v", wsr, accesslist)
			return nil
		}
		return err
	}

	errorResponse, err := api.parseErrorResponse(genericResponse.Data)
	if err != nil {
		return err
	}
	return fmt.Errorf("qfs command Error:%s", errorResponse.Message)
}

// branch the src workspace into a new workspace called dst.
func (api *Api) ClearAccessed(wsr string) error {
	if slashes := strings.Count(wsr, "/"); slashes != 1 {
		return fmt.Errorf("\"%s\" must contain precisely one \"/\"\n", wsr)
	}

	cmd := AccessedRequest{
		CommandCommon: CommandCommon{CommandId: CmdClearAccessed},
		WorkspaceRoot: wsr,
	}

	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	genericResponse, err := api.sendCmd(bytes)
	if err != nil {
		return err
	}

	errorResponse, err := api.parseErrorResponse(genericResponse.Data)
	if err != nil {
		return err
	}
	if errorResponse.ErrorCode != ErrorOK {
		fmt.Println("qfs command Error:%s", errorResponse.Message)
	}

	return nil
}

// Sync all the active workspaces
func (api *Api) SyncAll() error {
	cmd := SyncAllRequest{
		CommandCommon: CommandCommon{CommandId: CmdSyncAll},
	}

	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	if _, err := api.sendCmd(bytes); err != nil {
		return err
	}

	return nil
}
