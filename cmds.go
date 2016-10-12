// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "bytes"
import "fmt"
import "encoding/json"

//import "io"
import "os"
import "os/exec"
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
	CmdChrootRequest = iota
	CmdSyncAll       = iota
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

type BranchRequest struct {
	CommandCommon
	Src string
	Dst string
}

type ChrootRequest struct {
	CommandCommon
	WorkspaceRoot string
}

type SyncAllRequest struct {
	CommandCommon
}

func (api *Api) sendCmd(buf []byte) (ErrorResponse, error) {
	err := writeAll(api.fd, buf)
	if err != nil {
		return ErrorResponse{}, err
	}

	api.fd.Seek(0, 0)
	buffer := make([]byte, 4096)
	n, err := api.fd.Read(buffer)
	if err != nil {
		return ErrorResponse{}, err
	}

	buffer = buffer[:n]

	var response ErrorResponse
	err = json.Unmarshal(buffer, &response)
	if err != nil {
		return ErrorResponse{}, err
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

	buf, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	if _, err = api.sendCmd(buf); err != nil {
		return err
	}

	return nil
}

// chroot
func (api *Api) Chroot() error {
	mp, wsr, err := findLegitimateChrootPoint()
	if err != nil {
		return err
	}

	dirstat, err := os.Stat(mp + "/" + wsr)
	if err != nil {
		return err
	}
	rootstat, err := os.Stat("/")
	if err != nil {
		return err
	}

	if os.SameFile(dirstat, rootstat) {
		return fmt.Errorf("Alreadly chrooted")
	}

	wsr = mp + "/" + wsr
	err = os.Mkdir(wsr+"/etc", 0666)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	copy := exec.Command("cp", "/etc/resolv.conf", wsr+"/etc/resolv.conf")
	err = copy.Run()
	if err != nil {
		return err
	}

	copy = exec.Command("cp", "/etc/AroraKernel-compatibility",
		wsr+"/etc/AroraKernel-compatibility")
	err = copy.Run()
	if err != nil {
		return err
	}

	ns := exec.Command("netns", "-q", wsr)
	netnsd_running := true
	err = ns.Run()
	if err != nil {
		netnsd_running = false
	}

	if netnsd_running {
		ns = exec.Command("netns", wsr, "/bin/bash")
		err = ns.Run()
		if err != nil {
			fmt.Println("netns " + wsr + " /bin/bash Error")
		}
	}

	svrName := wsr + "/chroot"
	ns = exec.Command("netns", "-q", svrName)
	netnsd_running = true
	err = ns.Run()
	if err != nil {
		netnsd_running = false
	}

	if netnsd_running {
		netnsExec := exec.Command("sudo", "netns", svrName, "/usr/bin/sh", "-l", "-c", "$@", "/bin/bash", "/bin/bash")
		var netnsExecBuf bytes.Buffer
		netnsExec.Stderr = &netnsExecBuf
		err = netnsExec.Run()
		if err != nil {
			fmt.Println("netns1 Login shell error")
			fmt.Println(netnsExecBuf.String())
			return err
		}
	}

	prechrootScript := "sudo mount --rbind " + wsr + " " + wsr + ";"
	dstdev := wsr + "/dev"
	err = os.Mkdir(dstdev, 0666)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	prechrootScript = prechrootScript + "sudo mount -t tmpfs none " + dstdev + ";"
	prechrootScript = prechrootScript + "sudo cp -ax /dev/. " + dstdev + ";"

	dstdev = wsr + "/var/run/netns"
	_, err = os.Stat(dstdev)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dstdev, 0666)
			if err != nil {
				fmt.Print("Create dir " + dstdev + " Error")
			}
		}
	}
	prechrootScript = prechrootScript + "sudo mount -t tmpfs tmpfs " + dstdev + ";"

	archCommand := exec.Command("uname", "-m")
	var archBuf bytes.Buffer
	archCommand.Stdout = &archBuf
	err = archCommand.Run()
	if err != nil {
		fmt.Println("Get architecture Error")
	}
	archstr := archBuf.String()
	fmt.Println("archstr:", archstr)

	// a tmp mnt directory for mounting old root
	err = os.Mkdir(wsr+"/mnt", 0666)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	nsFlags := "m"
	fmt.Println("--chroot:", wsr)
	fmt.Println("--pre-chroot-cmd=:", prechrootScript)
	netnsdCmd := exec.Command("sudo", "/usr/bin/setarch", "i686", "/usr/bin/netnsd", "-d",
		"--no-netns-env", "-f", nsFlags, "--chroot="+wsr,
		"--pre-chroot-cmd="+prechrootScript, svrName)
	var netnsdBuf bytes.Buffer
	netnsdCmd.Stderr = &netnsdBuf
	err = netnsdCmd.Run()
	if err != nil {
		fmt.Println(netnsdBuf.String())
		fmt.Println(err.Error())
		return err
	}

	netnsExec := exec.Command("sudo", "/usr/bin/netns", svrName, "/usr/bin/sh", "-l", "-c", "$@", "/usr/bin/bash", "/usr/bin/bash")
	netnsExec.Stderr = &netnsdBuf
	err = netnsExec.Run()
	if err != nil {
		fmt.Println("netns2 Login shell error")
		fmt.Println(netnsdBuf.String())
		return err
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

func findLegitimateChrootPoint() (string, string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", "", err
	}

	cmd1 := exec.Command("cat", "/proc/self/mountstats")
	var output1 bytes.Buffer
	cmd1.Stdout = &output1
	err = cmd1.Run()
	if err != nil {
		fmt.Println("cmd1 run error")
		return "", "", err
	}

	//var mountpoint string
	//var wsr string
	dirs := strings.Split(wd, "/")
	for len(dirs) >= 3 {
		mountpoint := strings.Join(dirs[0:len(dirs)-2], "/")
		wsr := strings.Join(dirs[len(dirs)-2:len(dirs)], "/")

		arg := "mounted on " + mountpoint + " with fstype fuse.quantumfs"
		cmd2 := exec.Command("grep", arg)
		var output2 bytes.Buffer
		cmd2.Stdin = strings.NewReader(output1.String())
		cmd2.Stdout = &output2
		err = cmd2.Run()
		if err == nil {
			return mountpoint, wsr, nil
		}
		dirs = dirs[0 : len(dirs)-1]
	}
	return "", "", fmt.Errorf("Not valid path for chroot")
}
