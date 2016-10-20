// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// chroot runs a shell in the current workspace tree, in which
// the current workspace root becomes the filesystem root
package main

import "bytes"
import "flag"
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
	umount     = "/bin/umount"
	netns      = "/usr/bin/netns"
	netnsd     = "/usr/bin/netnsd"
	setarch    = "/usr/bin/setarch"
	cp         = "/bin/cp"
	sh         = "/bin/sh"
	bash       = "/bin/bash"
	ArtoolsDir = "/usr/share/Artools"
	oldroot    = "/mnt"
	pivot_root = "/sbin/pivot_root"
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

		toolInfo, err := os.Stat(toolDir)
		if err == nil && toolInfo.IsDir() {
			return rootdir, nil
		}

		dirs = dirs[0 : len(dirs)-1]
	}

	return "", fmt.Errorf("Invalid path for chroot")
}

// this function creates dst given the type of src if dst does not exist
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

// get all the necessary home directories
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
	err := cmdServerRun.Run()
	if err == nil {
		return true
	} else {
		return false
	}
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

func setupBindMounts(rootdir string) {
	paths := []string{"/proc", "/selinux", "/sys", "/dev/pts", "/tmp/.X11-unix",
		"/tmp/ArosTest.SimulatedDut", "/mnt/quantumfs"}
	homes := homedirs()
	paths = append(paths, homes...)
	for i := 0; i < len(paths); i++ {
		src := paths[i]
		dst := rootdir + paths[i]
		if !makedest(src, dst) {
			continue
		}

		cmdBindMount := exec.Command(sudo, mount, "-n", "--bind", src, dst)
		err := cmdBindMount.Run()
		if err != nil {
			fmt.Println("Error bind mounting ", src, " on ", dst)
		}
	}
}

func chrootInNsd(rootdir string, svrName string) error {
	cmdBindMountRoot := fmt.Sprintf("%s %s -n --rbind %s %s;",
		sudo, mount, rootdir, rootdir)

	dstDev := rootdir + "/dev"
	makedest("/dev", dstDev)
	cmdMountDev := fmt.Sprintf("%s %s -n -t tmpfs none %s;",
		sudo, mount, dstDev)
	cmdCopyDev := fmt.Sprintf("%s %s -ax /dev/. %s;", sudo, cp, dstDev)

	dstVar := rootdir + "/var/run/netns"
	err := os.MkdirAll(dstVar, 0666)
	if err != nil {
		return err
	}
	cmdMountVar := fmt.Sprintf("%s %s -n -t tmpfs tmpfs %s;",
		sudo, mount, dstVar)

	var otherBindMounts string
	paths := []string{"/proc", "/selinux", "/sys", "/dev/pts", "/tmp/.X11-unix",
		"/tmp/ArosTest.SimulatedDut", "/mnt/quantumfs"}
	homes := homedirs()
	paths = append(paths, homes...)
	for i := 0; i < len(paths); i++ {
		src := paths[i]
		dst := rootdir + paths[i]
		if !makedest(src, dst) {
			continue
		}
		otherBindMounts = otherBindMounts +
			fmt.Sprintf("%s %s -n --bind %s %s;", sudo, mount, src, dst)
	}
	prechrootCmd := cmdBindMountRoot + cmdMountDev +
		cmdCopyDev + cmdMountVar + otherBindMounts

	archString, err := getArchitecture(rootdir)
	if err != nil {
		return err
	}

	cmdNetnsd := exec.Command(sudo, setarch, archString, netnsd,
		"-d", "--no-netns-env", "-f", "m", "--chroot="+rootdir,
		"--pre-chroot-cmd="+prechrootCmd, svrName)
	var cmdNetnsdError bytes.Buffer
	cmdNetnsd.Stderr = &cmdNetnsdError
	err = cmdNetnsd.Run()
	if err != nil {
		return err
	}

	return nil
}

func printHelp() {
	fmt.Println("   qfs chroot -- Run a shell in the current workspace tree\n")
	fmt.Println("   qfs chroot [-r] [--nonpersistent]\n")
	fmt.Println("   Options:")
	fmt.Println("      -r                   Run the shell as root.")
	fmt.Println("      --nonpersistent      Create a non-persistent",
		" chroot environment.")
}

func chrootOutOfNsd(rootdir string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return error
	}

	if cwd == rootdir {
		cwd = "/"
	} else if strings.HasPrefix(cwd, rootdir) {
		cwd = cwd[len(rootdir):]
	}

	if err := syscall.Chdir("/"); err != nil {
		return err
	}

	cmd := exec.Command(sudo, mount, "-n", "--rbind", rootdir, rootdir)
	if err := cmd.Run(); err != nil {
		return err
	}

	dst := rootdir + "/dev"
	makedest("/dev", dst)
	cmd = exec.Command(sudo, mount, "-n", "-t", "tmpfs", "none", dst)
	if err := cmd.Run(); err != nil {
		return err
	}
	cmd = exec.Command(sudo, cp, "-ax", "/dev/.", dst)
	if err := cmd.Run(); err != nil {
		return err
	}

	dst = rootdir + "/var/run/netns"
	if err := os.MkdirAll(dst, 0666); err != nil {
		return err
	}
	cmd = exec.Command(sudo, mount, "-n", "-t", "tmpfs", "tmpfs", dst)

	setupBindMounts(rootdir)

	rootfd, err := os.Open(rootdir)
	if err != nil {
		return err
	}

	if err := syscall.Chroot(oldroot); err != nil {
		return err
	}

	for {
		fileInfo1, err := os.Stat(".")
		if err != nil {
			return err
		}

		fileInfo2, err := os.Stat("..")
		if err != nil {
			return err
		}

		if os.SameFile(fileInfo1, fileInfo2) {
			break
		}

		os.Chdir("..")
	}

	os.Chdir(".")

	syscall.Chdir(rootfd)
	cmd = exec.Command(sudo, pivot_root, ".", "."+oldroot)
	if err := cmd.Run(); err != nil {
		return err
	}
	os.Close(rootfd)

	cmd = exec.Command(sudo, umount, "-n", "-l", oldroot)
	if err := cmd.Run(); err != nil {
		return err
	}

}

func chroot() {

	asRoot := false
	nonPersistent := false

	args := flag.Args()[1:]

	for len(args) > 0 {
		switch args[0] {
		case "-r":
			asRoot = true
		case "--nonpersistent":
			nonPersistent = true
		default:
			printHelp()
			return
		}
		args = args[1:]
	}

	rootdir, err := findWorkspaceRoot()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if nonPersistent {
		fmt.Println("nonPersistent")
		return
	}

	svrName := rootdir + "/chroot"
	if !serverRunning(svrName) {
		err = chrootInNsd(rootdir, svrName)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	err = netnsLogin(rootdir, svrName, asRoot)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	return
}
