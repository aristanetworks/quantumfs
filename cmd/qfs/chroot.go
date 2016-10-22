// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// chroot runs a shell in the current workspace tree, in which
// the current workspace root becomes the filesystem root
package main

import "fmt"
import "io/ioutil"
import "os"
import "os/exec"
import "os/user"
import "strings"
import "syscall"

const (
	sudo       = "/usr/bin/sudo"
	mount      = "/bin/mount"
	netns      = "/usr/bin/netns"
	netnsd     = "/usr/bin/netnsd"
	setarch    = "/usr/bin/setarch"
	cp         = "/bin/cp"
	sh         = "/bin/sh"
	bash       = "/bin/bash"
	ArtoolsDir = "/usr/share/Artools"
)

// This function comes from the implementation of chroot in Artools,
// but we are going to get rid of the dependency on Artools so it will
// become deprecated when we can make a quantumfs workspace into a proper
// workspace with "a4 newtree"
func findWorkspaceRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	dirs := strings.Split(wd, "/")

	for len(dirs) > 1 {
		rootdir := strings.Join(dirs, "/")
		toolDir := rootdir + ArtoolsDir

		if toolInfo, err := os.Stat(toolDir); err == nil &&
			toolInfo.IsDir() {

			return rootdir, nil
		}

		dirs = dirs[0 : len(dirs)-1]
	}

	return "", fmt.Errorf("Invalid path for chroot")
}

// This function creates dst given the type of src if dst does not exist.
// It returns true if dst exists and is the same type as src, or dst is
// successfully created, otherwise returns false.
func makedest(src, dst string) bool {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return false
	}

	dstInfo, err := os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		return false
	}

	if err == nil && srcInfo.IsDir() == dstInfo.IsDir() {
		return true
	}

	if srcInfo.IsDir() {
		if err := os.Mkdir(dst, 0666); err != nil {
			return false
		} else {
			return true
		}
	} else {
		if fd, err := os.Create(dst); err != nil {
			return false
		} else {
			fd.Close()
			return true
		}
	}
}

// get all the necessary home directories
func homedirs() []string {
	homes := make([]string, 0)

	if arastra, err := user.Lookup("arastra"); err == nil {
		homes = append(homes, arastra.HomeDir)
	}

	if current, err := user.Current(); err == nil && current.Username != "root" {
		homes = append(homes, current.HomeDir)
	}

	return homes
}

// process the architecture string
func processArchitecture(arch string) (string, error) {
	archs := strings.Split(arch, "_")
	archStr := strings.Join(archs[:len(archs)-1], "_")

	switch archStr {
	case "i386":
		return "i686", nil
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

	if err := cmdServerRun.Run(); err == nil {
		return true
	}

	return false
}

// login the netns server and open a new login shell, which is not
// expected to return
func netnsLogin(rootdir string, svrName string, root bool) error {
	var err error

	env := os.Environ()
	env = append(env, "A4_CHROOT="+rootdir)

	if root {
		args := []string{sudo, netns, svrName, sh, "-l", "-c",
			"\"$@\"", bash, bash}

		err = syscall.Exec(sudo, args, env)
	} else {
		args := []string{netns, svrName, sh, "-l", "-c",
			"\"$@\"", bash, bash}

		err = syscall.Exec(netns, args, env)
	}

	return err
}

func chrootInNsd(rootdir string, svrName string) error {
	bindmountRoot := fmt.Sprintf("%s %s -n --rbind %s %s;", sudo, mount,
		rootdir, rootdir)

	dstDev := rootdir + "/dev"
	makedest("/dev", dstDev)

	mountDev := fmt.Sprintf("%s %s -n -t tmpfs none %s;",
		sudo, mount, dstDev)

	copyDev := fmt.Sprintf("%s %s -ax /dev/. %s;", sudo, cp, dstDev)

	dstVar := rootdir + "/var/run/netns"
	if err := os.MkdirAll(dstVar, 0666); err != nil {
		return err
	}

	mountVar := fmt.Sprintf("%s %s -n -t tmpfs tmpfs %s;", sudo, mount, dstVar)

	paths := []string{"/proc", "/selinux", "/sys", "/dev/pts", "/tmp/.X11-unix",
		"/tmp/ArosTest.SimulatedDut", "/mnt/quantumfs"}
	homes := homedirs()
	paths = append(paths, homes...)

	var bindmountOther string
	for i := 0; i < len(paths); i++ {
		src := paths[i]
		dst := rootdir + paths[i]
		if !makedest(src, dst) {
			continue
		}

		bindmountOther = bindmountOther +
			fmt.Sprintf("%s %s -n --bind %s %s;", sudo, mount, src, dst)
	}

	prechrootCmd := bindmountRoot + mountDev + copyDev +
		mountVar + bindmountOther

	archString, err := getArchitecture(rootdir)
	if err != nil {
		return err
	}

	cmdNetnsd := exec.Command(sudo, setarch, archString, netnsd,
		"-d", "--no-netns-env", "-f", "m", "--chroot="+rootdir,
		"--pre-chroot-cmd="+prechrootCmd, svrName)
	if err := cmdNetnsd.Run(); err != nil {
		return err
	}

	return nil
}

func chroot() {
	rootdir, err := findWorkspaceRoot()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	svrName := rootdir + "/chroot"

	if !serverRunning(svrName) {
		if err := chrootInNsd(rootdir, svrName); err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	if err := netnsLogin(rootdir, svrName, false); err != nil {
		fmt.Println(err.Error())
		return
	}

	return
}
