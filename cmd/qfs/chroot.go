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
import "strconv"
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
	chns       = "/usr/bin/chns"
	sh         = "/bin/sh"
	bash       = "/bin/bash"
	ArtoolsDir = "/usr/share/Artools"
	oldroot    = "/mnt"
	pivot_root = "/sbin/pivot_root"
)

var qfs string
var asRoot bool = false
var nonPersistent bool = false
var setupNamespaces bool = false

func init() {
	gopath := os.Getenv("GOPATH")
	qfs = gopath + "/bin/qfs"
}

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
		fmt.Println("+ ", dst)
		return true
	} else {
		fd, create_err := os.Create(dst)
		if create_err != nil {
			fmt.Println("Error creating file ", dst)
			return false
		}
		fd.Close()
		fmt.Println("+ ", dst)
		return true
	}
}

// get all the necessary home directories
func homedirs() []string {
	homes := make([]string, 0)

	envHome := os.Getenv("HOME")
	homes = append(homes, envHome)

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
		cmdBindMount.Run()
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

func switchUserMode(asRoot bool) error {
	if asRoot {
		return nil
	}

	cmdLogname := exec.Command("logname")
	var lognameBuf bytes.Buffer
	cmdLogname.Stdout = &lognameBuf
	if err := cmdLogname.Run(); err != nil {
		return err
	}

	lognameStr := lognameBuf.String()
	lognameStr = strings.TrimSuffix(lognameStr, "\n")

	logUser, err := user.Lookup(lognameStr)
	if err != nil {
		return err
	}

	if gid, err := strconv.Atoi(logUser.Gid); err != nil {
		return err
	} else if err = syscall.Setregid(gid, gid); err != nil {
		return err
	}

	if logGroupIds, err := logUser.GroupIds(); err != nil {
		return err
	} else {
		groupIds := make([]int, 0)
		for i := 0; i < len(logGroupIds); i++ {
			if groupId, err := strconv.Atoi(logGroupIds[i]); err == nil {
				groupIds = append(groupIds, groupId)
			}
		}

		if err = syscall.Setgroups(groupIds); err != nil {
			return err
		}
	}

	if uid, err := strconv.Atoi(logUser.Uid); err == nil {
		if err = syscall.Setreuid(uid, uid); err != nil {
			return err
		}
	} else {
		return err
	}

	return nil
}

func chrootOutOfNsd(rootdir string) error {

	// create a new namespace and run qfs chroot tool in the new namespace
	if !setupNamespaces {
		chns_args := []string{sudo, chns, "-m", "-l", "qfschroot"}
		chroot_args := []string{qfs, "chroot", "--nonpersistent", "--setup-namespaces"}

		if asRoot {
			chroot_args = append(chroot_args, "-r")
		}

		chns_args = append(chns_args, chroot_args...)
		chns_env := os.Environ()
		if err := syscall.Exec(chns_args[0], chns_args, chns_env); err != nil {
			return err
		}
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	// modify current working directory into a relative path to
	// the workspaceroot we are chrooting in
	if cwd == rootdir {
		cwd = "/"
	} else if strings.HasPrefix(cwd, rootdir) {
		cwd = cwd[len(rootdir):]
	}

	if err := syscall.Chdir("/"); err != nil {
		return err
	}

	rootdirInfo, err := os.Stat(rootdir)
	if err != nil {
		return err
	}

	fsrootInfo, err := os.Stat("/")
	if err != nil {
		return err
	}

	if !os.SameFile(rootdirInfo, fsrootInfo) {
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
		if err := cmd.Run(); err != nil {
			return err
		}

		setupBindMounts(rootdir)

		// Remember the current directory so that we can restore it later
		rootfd, err := os.Open(rootdir)
		if err != nil {
			return err
		}

		// In a chroot escape so pivot_root will work
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

			if err := os.Chdir(".."); err != nil {
				return err
			}
		}

		if err := syscall.Chroot("."); err != nil {
			return err
		}

		// pivot_root to the root that we want to keep
		if err := rootfd.Chdir(); err != nil {
			return err
		}

		cmd = exec.Command(sudo, pivot_root, ".", "."+oldroot)
		if err := cmd.Run(); err != nil {
			return err
		}

		if err := rootfd.Close(); err != nil {
			return err
		}

		// unmount the old file system
		cmd = exec.Command(sudo, umount, "-n", "-l", oldroot)
		if err := cmd.Run(); err != nil {
			return err
		}
	}

	// change the current directory
	cwdInfo, err := os.Stat(cwd)
	if err != nil {
		return err
	}

	if !cwdInfo.IsDir() {
		cwd = "/"
	}

	if err := os.Chdir(cwd); err != nil {
		return err
	}

	archStr, err := getArchitecture("/")
	if err != nil {
		return err
	}

	// switch to non-root user if not specified to run as root
	if err := switchUserMode(asRoot); err != nil {
		return err
	}

	setarch_cmd := []string{setarch, archStr}
	shell_cmd := []string{sh, "-l", "-c", "\"$@\"", bash, bash}

	setarch_cmd = append(setarch_cmd, shell_cmd...)

	setarch_env := os.Environ()
	setarch_env = append(setarch_env, "A4_CHROOT="+rootdir)

	if err := syscall.Exec(setarch_cmd[0],
		setarch_cmd, setarch_env); err != nil {

		return err
	}

	return nil
}

func chroot() {
	args := flag.Args()[1:]
	for len(args) > 0 {
		switch args[0] {
		case "-r":
			asRoot = true
		case "--nonpersistent":
			nonPersistent = true
		case "--setup-namespaces":
			setupNamespaces = true
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
		if err := chrootOutOfNsd(rootdir); err != nil {
			fmt.Println(err.Error())
		}
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
