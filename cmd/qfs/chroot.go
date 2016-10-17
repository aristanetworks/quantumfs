// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// chroot implements the quantumfs chroot tool as one option in
// qfs tools
package main

import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "os/exec"
import "os/user"
import "runtime"
import "strconv"
import "strings"
import "syscall"

const (
	sudo    = "/usr/bin/sudo"
	mount   = "/bin/mount"
	netns   = "/usr/bin/netns"
	netnsd  = "/usr/bin/netnsd"
	setarch = "/usr/bin/setarch"
	cp      = "/bin/cp"
	sh      = "/bin/sh"
	bash    = "/bin/bash"
)

var ArtoolsDir string

func init() {
	if runtime.GOOS == "darwin" {
		ArtoolsDir = "/usr/local/share/Artools"
	} else {
		ArtoolsDir = "/usr/share/Artools"
	}
}

// this function checks all parent directories of current directory
// to find the workspaceroot, that is the directory whose grandparent
// is the mountpoint of quantumfsd
// BUT: so far we still cannot make a quantumfs workspace into a proper
// workspace with "a4 newtree", so although this method should be adequate
// to find the root directory to chroot into, it is still not used
func findLegitimateChrootPoint() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	cmdMountstats := exec.Command("cat", "/proc/self/mountstats")
	var outputMountstats bytes.Buffer
	cmdMountstats.Stdout = &outputMountstats
	err = cmdMountstats.Run()
	if err != nil {
		fmt.Println("cmdMountstats run error")
		return "", err
	}

	dirs := strings.Split(wd, "/")
	for len(dirs) >= 3 {
		mountpoint := strings.Join(dirs[0:len(dirs)-2], "/")
		wsr := strings.Join(dirs[len(dirs)-2:len(dirs)], "/")

		grepArg := "mounted on " + mountpoint + " with fstype fuse.quantumfs"
		cmdGrep := exec.Command("grep", grepArg)
		var outputGrep bytes.Buffer
		cmdGrep.Stdin = strings.NewReader(outputMountstats.String())
		cmdGrep.Stdout = &outputGrep
		err = cmdGrep.Run()
		if err == nil {
			return mountpoint + "/" + wsr, nil
		}
		dirs = dirs[0 : len(dirs)-1]
	}

	return "", fmt.Errorf("Invalid path for chroot")
}

// This function comes from the implementation of chroot in Artools,
// but we are going to get rid of the dependency on Artools so it will
// become deprecated when we can make a quantumfs workspace into a proper
// workspace with "a4 newtree"
func findContainerRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	dirs := strings.Split(wd, "/")
	for len(dirs) > 1 {
		rootdir := strings.Join(dirs, "/")
		toolDir := rootdir + ArtoolsDir

		toolInfo, err := os.Stat(toolDir)
		if err == nil && toolInfo.IsDir() {
			return rootdir, nil
		}

		dirs = dirs[0 : len(dirs)-1]
	}

	return "", fmt.Errorf("Invalid path for chroot")
}

// this function makes dst given the type of src if dst does not exist
func makedest(src, dst string) bool {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false
	}

	dstInfo, err := os.Stat(dst)
	if err == nil {
		if srcInfo.IsDir() == dstInfo.IsDir() {
			return true
		}
	}
	if srcInfo.IsDir() {
		mkdir_err := os.Mkdir(dst, 0666)
		if mkdir_err != nil {
			fmt.Println("Error creating directory ", dst)
			return false
		}
		return true
	} else {
		fd, create_err := os.Create(dst)
		if create_err != nil {
			fmt.Println("Error creating file ", dst)
			return false
		}
		fd.Close()
		return true
	}
}

// get all the necessary homedirectories
func homedirs() []string {
	homes := make([]string, 0)

	arastra, err := user.Lookup("arastra")
	if err == nil {
		homes = append(homes, arastra.HomeDir)
	}

	current, err := user.Current()
	if err == nil && current.Username != "root" {
		homes = append(homes, current.HomeDir)
	}

	return homes
}

// process the architecture string
func processArchitecture(arch string) (string, error) {
	archs := strings.Split(arch, "_")
	archStr := strings.Join(archs[:len(archs)-1], "_")
	osVersion, err := strconv.Atoi(archs[len(archs)-1])
	if err != nil {
		return "", fmt.Errorf("Invalid OS version")
	}

	switch archStr {
	case "i386":
		if osVersion >= 12 {
			return "i686", nil
		}
	case "x86_64":
		return "x86_64", nil
	}
	return "", fmt.Errorf("Unrecognized architecture")
}

// get the architecture of the workspace
func getArchitecture(rootdir string) (string, error) {
	platform, err := ioutil.ReadFile(rootdir + ArtoolsDir + "/platform")
	if err != nil {
		return "", err
	}

	platStr := string(platform[:len(platform)-1])
	return processArchitecture(platStr)
}

// test whether the netns server is already running
func serverRunning(svrName string) bool {
	cmdServerRun := exec.Command("netns", "-q", svrName)
	err := cmdServerRun.Run()
	if err == nil {
		return true
	} else {
		return false
	}
}

// login the netns server and open a new login shell
func netnsLogin(rootdir string, svrName string, root bool) error {
	var err error
	env := os.Environ()
	env = append(env, "A4_CHROOT="+rootdir)
	if root {
		err = syscall.Exec(sudo,
			[]string{sudo, netns, svrName, sh, "-l", "-c",
				"\"$@\"", bash, bash},
			env)
	} else {
		err = syscall.Exec(netns,
			[]string{netns, svrName, sh, "-l", "-c",
				"\"$@\"", bash, bash},
			env)
	}

	return err
}

func chroot() {
	rootdir, err := findContainerRoot()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	svrName := rootdir + "/chroot"
	if serverRunning(svrName) {
		err = netnsLogin(rootdir, svrName, false)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	prechrootCmd := fmt.Sprintf("%s %s -n --rbind %s %s;",
		sudo, mount, rootdir, rootdir)

	dstdev := rootdir + "/dev"
	makedest("/dev", dstdev)
	dstdevCmd := fmt.Sprintf("%s %s -n -t rmpfs none %s;", sudo, mount, dstdev)
	prechrootCmd = prechrootCmd + dstdevCmd

	dstdevCmd = fmt.Sprintf("%s %s -ax /dev/. %s;", sudo, cp, dstdev)
	prechrootCmd = prechrootCmd + dstdevCmd

	dstdev = rootdir + "/var/run/netns"
	_, err = os.Stat(dstdev)
	if err != nil && os.IsNotExist(err) {
		err = os.Mkdir(dstdev, 0666)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}
	dstdevCmd = fmt.Sprintf("%s %s -n -t tmpfs tmpfs %s;", sudo, mount, dstdev)
	prechrootCmd = prechrootCmd + dstdevCmd

	paths := []string{"/proc", "/selinux", "/sys",
		"/dev/pts", "/tmp/.X11-unix", "/tmp/ArosTest.SimulatedDut"}
	homes := homedirs()
	paths = append(paths, homes...)
	for i := 0; i < len(paths); i++ {
		src := paths[i]
		dst := rootdir + paths[i]
		if !makedest(src, dst) {
			continue
		}

		dstdevCmd = fmt.Sprintf("%s %s -n --bind %s %s;",
			sudo, mount, src, dst)
		prechrootCmd = prechrootCmd + dstdevCmd
	}
	fmt.Println(prechrootCmd)

	archString, err := getArchitecture(rootdir)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	cmdNetnsd := exec.Command(sudo, setarch, archString, netnsd,
		"-d", "--no-netns-env", "-f", "m", "--chroot="+rootdir,
		"--pre-chroot-cmd="+prechrootCmd, svrName)
	var cmdNetnsdError bytes.Buffer
	cmdNetnsd.Stderr = &cmdNetnsdError
	err = cmdNetnsd.Run()
	if err != nil {
		fmt.Println(cmdNetnsdError.String())
		return
	}

	err = netnsLogin(rootdir, svrName, false)

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	return
}
