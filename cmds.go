// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "bufio"
import "fmt"
import "encoding/json"
import "os"
import "strconv"
import "strings"
import "syscall"

// This file contains all the functions which are used by qfs (and other
// applications) to perform special quantumfs operations. Primarily this is done by
// marhalling the arguments and passing them to quantumfsd for processing, then
// interpreting the results.

func fileIsApi(stat os.FileInfo) bool {
	stat_t, ok := stat.Sys().(*syscall.Stat_t)
	if ok && stat_t.Ino == InodeIdApi {
		// No real filesystem is likely to give out inode 2
		// for a random file but quantumfs reserves that
		// inode for all the api files.
		return true
	}

	return false
}

func findApiPathEnvironment() string {
	path := os.Getenv("QUANTUMFS_API_PATH")
	if path == "" {
		return ""
	}

	if !strings.HasSuffix(path, fmt.Sprintf("%c%s", os.PathSeparator, ApiPath)) {
		return ""
	}

	stat, err := os.Lstat(path)
	if err != nil {
		return ""
	}

	if !fileIsApi(stat) {
		return ""
	}

	return path
}

func findApiPathMount() string {
	// We are look in /proc/self/mountinfo for a line which indicates that
	// QuantumFS is mounted. That line looks like:
	//
	// 138 30 0:32 / /mnt/quantumfs rw,relatime - fuse.QuantumFS QuantumFS ...
	//
	// Where the number after the colon (0:32) is the FUSE connection number.
	// Since it's possible for any filesystem to me bind-mounted to multiple
	// locations we use that number to discriminate between multiple QuantumFS
	// mounts.

	mountfile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return ""
	}

	mountinfo := bufio.NewReader(mountfile)

	var path string
	connectionId := int64(-1)

	for {
		bline, _, err := mountinfo.ReadLine()
		if err != nil {
			break
		}

		line := string(bline)

		if !strings.Contains(line, "fuse.QuantumFS") {
			continue
		}

		fields := strings.SplitN(line, " ", 6)
		connectionS := strings.Split(fields[2], ":")[1]
		connectionI, err := strconv.ParseInt(connectionS, 10, 64)
		if err != nil {
			// We cannot parse this line, but we also know we have a
			// QuantumFS mount. Play it safe and fail searching for a
			// mount.
			return ""
		}
		path = fields[4]

		if connectionId != -1 && connectionId != connectionI {
			// We have a previous QuantumFS mount which doesn't match
			// this one, thus we have more than one mount. Give up.
			return ""
		}

		connectionId = connectionI
	}

	if connectionId == -1 {
		// We didn't find a mount
		return ""
	}

	// We've found precisely one mount, ensure the file is really the api file.
	path = fmt.Sprintf("%s%c%s", path, os.PathSeparator, ApiPath)
	stat, err := os.Lstat(path)
	if err != nil || !fileIsApi(stat) {
		return ""
	}

	return path
}

func findApiPathUpwards() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
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
			if fileIsApi(stat) {
				return path, nil
			}
		}
	}

	return "", nil
}

func findApiPath() (string, error) {
	path := findApiPathEnvironment()
	if path != "" {
		return path, nil
	}

	path = findApiPathMount()
	if path != "" {
		return path, nil
	}

	path, err := findApiPathUpwards()
	if err != nil {
		return "", err
	}

	if path != "" {
		return path, nil
	}

	// Give up
	return "", fmt.Errorf("Unable to find API file")
}

// NewApi searches for the QuantumFS API files. The search order is:
// 1. The path in the environment variable QUANTUMFS_API_PATH, ie "/qfs/api"
// 2. The api file at the root of the sole mounted QuantumFS instance. If more than
//    one instance is mounted none of them will be used.
// 3. Searching upwards in the directory tree for the api file
func NewApi() (*Api, error) {
	path, err := findApiPath()
	if err != nil {
		return nil, err
	}
	return NewApiWithPath(path)
}

func NewApiWithPath(path string) (*Api, error) {
	api := Api{}

	fd, err := os.OpenFile(path, os.O_RDWR, 0)
	api.fd = fd
	if err != nil {
		return nil, err
	}

	return &api, nil
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
func (api *Api) GetAccessed(wsr string) error {
	if !isWorkspaceNameValid(wsr) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", wsr)
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
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n", dst)
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

// Enable the chosen workspace mutable
//
// dst is the path relative to the filesystem root, ie. user/joe/myws
func (api *Api) EnableRootWrite(dst string) error {
	if !isWorkspaceNameValid(dst) {
		return fmt.Errorf("\"%s\" must contain precisely two \"/\"\n", dst)
	}

	cmd := EnableRootWriteRequest{
		CommandCommon: CommandCommon{CommandId: CmdEnableRootWrite},
		Workspace:     dst,
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
		return fmt.Errorf("qfs command Error: %s", errorResponse.Message)
	}
	return nil
}

// Make the chosen workspace irreversibly immutable
//
// workspacepath is the path relative to the filesystem root, ie. user/joe/myws
func (api *Api) SetWorkspaceImmutable(workspacepath string) error {
	if !isWorkspacePathValid(workspacepath) {
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n",
			workspacepath)
	}

	cmd := SetWorkspaceImmutableRequest{
		CommandCommon: CommandCommon{CommandId: CmdSetWorkspaceImmutable},
		WorkspacePath: workspacepath,
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
		return fmt.Errorf("qfs command Error: %s", errorResponse.Message)
	}
	return nil
}

// Delete the given workspace.
//
// workspacepath is the path relative to the filesystem root, ie. user/joe/myws
func (api *Api) DeleteWorkspace(workspacepath string) error {
	if !isWorkspacePathValid(workspacepath) {
		return fmt.Errorf("\"%s\" must contain at least two \"/\"\n",
			workspacepath)
	}

	cmd := DeleteWorkspaceRequest{
		CommandCommon: CommandCommon{CommandId: CmdDeleteWorkspace},
		WorkspacePath: workspacepath,
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
		return fmt.Errorf("qfs command Error: %s", errorResponse.Message)
	}
	return nil
}

func (api *Api) SetBlock(key []byte, data []byte) error {

	cmd := SetBlockRequest{
		CommandCommon: CommandCommon{CommandId: CmdSetBlock},
		Key:           key,
		Data:          data,
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

// Note that, because we report the size of Api as 1024, we can't read more than
// 1024 bytes of data... so blocks that you try to Get have a low size limit.
// See BUG185832
func (api *Api) GetBlock(key []byte) ([]byte, error) {

	cmd := GetBlockRequest{
		CommandCommon: CommandCommon{CommandId: CmdGetBlock},
		Key:           key,
	}

	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	buf, err := api.sendCmd(cmdBuf)
	if err != nil {
		return nil, err
	}

	var getBlockResponse GetBlockResponse
	err = json.Unmarshal(buf, &getBlockResponse)
	if err != nil {
		return nil, err
	}

	errorResponse := getBlockResponse.ErrorResponse
	if errorResponse.ErrorCode != ErrorOK {
		return nil, fmt.Errorf("qfs command Error:%s", errorResponse.Message)
	}

	return getBlockResponse.Data, nil
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
