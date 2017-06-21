// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

// chroot runs a shell in the current workspace tree, in which
// the current workspace root becomes the filesystem root

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/aristanetworks/quantumfs/utils"
)

const (
	sudo       = "/usr/bin/sudo"
	umount     = "/usr/bin/umount"
	sh         = "/usr/bin/sh"
	ArtoolsDir = "/usr/share/Artools"
	oldroot    = "/mnt"
	pivot_root = "/usr/sbin/pivot_root"
)

const (
	SYSFS_MAGIC = 0x62656572
)

var qfs string

func init() {
	if qfspath, err := os.Executable(); err != nil {
		fmt.Println("Unable to locate qfs directory")
		qfs = "./qfs"
	} else {
		qfs = qfspath
	}
}

// A helper function to test whether a path is a legitimate workspaceroot
// by checking whether /usr/share/Artools directory is present
func isLegitimateWorkspaceRoot(wsr string) bool {
	toolDir := wsr + ArtoolsDir
	if toolInfo, err := os.Stat(toolDir); err == nil && toolInfo.IsDir() {
		return true
	}
	return false
}

// Helper function to find the workspace name from the workspace root
func findWorkspaceName(wsr string) (string, error) {
	dirs := strings.Split(wsr, "/")
	if len(dirs) < 3 {
		return "", fmt.Errorf("Invalid path for workspace root")
	}

	return strings.Join(dirs[len(dirs)-3:], "/"), nil
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
		fmt.Println("Destination Inode already exists: ", err)
		return false
	}

	if err == nil && srcInfo.IsDir() == dstInfo.IsDir() {
		return true
	}

	if srcInfo.IsDir() {
		if err := syscall.Mkdir(dst, uint32(srcInfo.Mode())); err != nil {
			fmt.Println("Fail to create a directory: ", err)
			return false
		} else {
			return true
		}
	} else {
		if fd, err := os.Create(dst); err != nil {
			fmt.Println("Fail to create another type: ", err)
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

	envHome := os.Getenv("HOME")
	homes = append(homes, envHome)

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

func setArchitecture(arch string) error {
	var flag uintptr
	if arch == "i686" {
		flag = 0x0008
	} else if arch == "x86_64" {
		flag = 0x0000
	} else {
		return fmt.Errorf("Unsupported architecture: %s", arch)
	}

	_, _, errno := syscall.Syscall(syscall.SYS_PERSONALITY, flag, 0, 0)
	if errno != 0 {
		return fmt.Errorf("Change Personality error: %d", errno)
	}

	return nil
}

func setupBindMounts(rootdir string) error {
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

		if err := syscall.Mount(src, dst, "", syscall.MS_BIND,
			""); err != nil {

			return fmt.Errorf("Bindmounting %s to %s error: %s",
				src, dst, err.Error())
		}
	}

	return nil
}

func switchUser(username string) error {
	logUser, err := user.Lookup(username)
	if err != nil {
		return err
	}

	if gid, err := strconv.Atoi(logUser.Gid); err != nil {
		return err
	} else if err = syscall.Setregid(gid, gid); err != nil {
		return err
	}

	logGroupIds, err := logUser.GroupIds()
	if err != nil {
		return err
	}

	groupIds := make([]int, 0)
	for i := 0; i < len(logGroupIds); i++ {
		if groupId, err := strconv.Atoi(logGroupIds[i]); err == nil {
			groupIds = append(groupIds, groupId)
		}
	}

	if err := syscall.Setgroups(groupIds); err != nil {
		return err
	}

	if uid, err := strconv.Atoi(logUser.Uid); err != nil {
		return err
	} else if err = syscall.Setreuid(uid, uid); err != nil {
		return err
	}

	if err = os.Setenv("USER", username); err != nil {
		return err
	}

	if err = os.Setenv("USERNAME", username); err != nil {
		return err
	}

	if err = os.Setenv("HOME", logUser.HomeDir); err != nil {
		return err
	}

	return nil
}

func copyDirStayOnFs(src string, dst string) error {
	var srcfs syscall.Statfs_t
	if err := syscall.Statfs(src, &srcfs); err != nil {
		return fmt.Errorf("Statfs directory %s error: %s",
			src, err.Error())
	}

	syscall.Umask(0)

	return filepath.Walk(src, func(name string, finfo os.FileInfo,
		err error) error {

		if err != nil {
			return fmt.Errorf("Walking file/directory %s error: %s",
				name, err.Error())
		}

		if finfo.IsDir() {
			var dirfs syscall.Statfs_t
			errDirfs := syscall.Statfs(name, &dirfs)
			if errDirfs != nil {
				return fmt.Errorf("Statfs directory %s error: %s",
					name, errDirfs.Error())
			}

			// If filesystem types are different, then this directory
			// is a mountpoint, we shouldn't copy the device across
			// filesystem boundary.
			if srcfs.Type != dirfs.Type {
				return filepath.SkipDir
			}

			nameDst := filepath.Join(dst, name[len(src):])
			errMkdir := utils.MkdirAll(nameDst, finfo.Mode())
			if errMkdir != nil {
				return fmt.Errorf("Create directory %s error: %s",
					nameDst, errMkdir.Error())
			}
			if name == "/dev/shm" {
				// /dev/shm is generally a tmpfs filesystem full of
				// things which make no sense to copy into the
				// chroot. We want to create the directory, but not
				// copy any of its contents.
				return filepath.SkipDir
			}
		} else if finfo.Mode().IsRegular() {
			// There should not be any ordinary files in /dev directory,
			// though in rare cases like /dev/shm there may be. Warn, but
			// skip the file and continue
			fmt.Printf("Skipping ordinary file in /dev: %s\n", name)
		} else if (finfo.Mode() & os.ModeSymlink) != 0 {
			oldPath, errOldPath := os.Readlink(name)
			if errOldPath != nil {
				return fmt.Errorf("Readlink %s error: %s",
					name, errOldPath.Error())
			}

			nameDst := filepath.Join(dst, name[len(src):])
			errSymlink := syscall.Symlink(oldPath, nameDst)
			if errSymlink != nil {
				return fmt.Errorf("Symlink %s->%s error: %s",
					nameDst, oldPath, errSymlink.Error())
			}
		} else {
			// If it is a device, create the new node.
			if fstat, ok := finfo.Sys().(*syscall.Stat_t); ok {
				nameDst := filepath.Join(dst, name[len(src):])

				errMknod := syscall.Mknod(nameDst, fstat.Mode,
					int(fstat.Rdev))
				if errMknod != nil {
					return fmt.Errorf("Mknod %s error: %s",
						nameDst, errMknod.Error())
				}

				dstUid := int(fstat.Uid)
				dstGid := int(fstat.Gid)
				errChown := syscall.Chown(nameDst, dstUid, dstGid)
				if errChown != nil {
					return fmt.Errorf("Change ownership of"+
						" device %s error: %s",
						nameDst, errChown.Error())
				}
			}
		}

		return nil
	})
}

func nonPersistentChroot(username string, rootdir string, workingdir string,
	cmd []string) error {

	// isolate the mount namespace of this process from the rest of the machine
	if err := syscall.Unshare(syscall.CLONE_NEWNS); err != nil {
		return fmt.Errorf("Unshare error: %s", err.Error())
	}

	var buf syscall.Statfs_t
	if err := syscall.Statfs("/sys", &buf); err != nil {
		return fmt.Errorf("Getting filesystem stat of /sys error:%s",
			err.Error())
	}

	// remount /sys to reflect the new namespace
	if buf.Type == SYSFS_MAGIC {
		if err := syscall.Mount("/sys", "/sys", "sysfs", syscall.MS_REMOUNT,
			""); err != nil {

			return fmt.Errorf("Remount /sys error: %s", err.Error())
		}
	}

	if err := syscall.Chdir("/"); err != nil {
		return fmt.Errorf("Changing directory to / error: %s", err.Error())
	}

	rootdirInfo, err := os.Stat(rootdir)
	if err != nil {
		return fmt.Errorf("Stating %s error: %s", rootdir, err.Error())
	}

	fsrootInfo, err := os.Stat("/")
	if err != nil {
		return fmt.Errorf("Stating / error: %s", err.Error())
	}

	if !os.SameFile(rootdirInfo, fsrootInfo) {
		// pivot_root will only work when root directory is a mountpoint
		if err := syscall.Mount(rootdir, rootdir, "",
			syscall.MS_BIND|syscall.MS_REC, ""); err != nil {

			return fmt.Errorf("Recursively bindmounting %s error: %s",
				rootdir, err.Error())
		}

		dst := rootdir + "/dev"
		if makedest("/dev", dst) {
			errMnt := syscall.Mount("none", dst, "tmpfs", 0, "")
			if errMnt != nil {
				return fmt.Errorf("Mounting %s error: %s",
					dst, errMnt.Error())
			}

			if err := copyDirStayOnFs("/dev", dst); err != nil {
				return fmt.Errorf("Copying /dev error: %s",
					err.Error())
			}
		}

		dst = rootdir + "/var/run/netns"
		if err := utils.MkdirAll(dst, 0666); err != nil {
			return fmt.Errorf("Creating directory %s error: %s",
				dst, err.Error())
		}

		if err := syscall.Mount("tmpfs", dst, "tmpfs", 0, ""); err != nil {
			return fmt.Errorf("Mounting %s error: %s", dst, err.Error())
		}

		dst = rootdir + "/tmp"
		if err := utils.MkdirAll(dst, os.ModeSticky|0777); err != nil {

			return fmt.Errorf("Mounting /tmp as tmpfs error: %s",
				err.Error())
		}
		if err := syscall.Mount("tmpfs", dst, "tmpfs", 0, ""); err != nil {
			return fmt.Errorf("Mounting tmp as tmpfs error: %s",
				err.Error())
		}

		if err := setupBindMounts(rootdir); err != nil {
			return err
		}

		// Remember the current directory so that we can restore it later
		rootfd, err := os.Open(rootdir)
		if err != nil {
			return fmt.Errorf("opening %s error: %s",
				rootdir, err.Error())
		}

		// In a chroot escape so pivot_root will work
		if err := syscall.Chroot(oldroot); err != nil {
			return fmt.Errorf("chrooting into %s error: %s",
				oldroot, err.Error())
		}

		// Keep changing to parent directory up to the root
		for {
			fileInfo1, err := os.Stat(".")
			if err != nil {
				return fmt.Errorf("stating . error: %s",
					err.Error())
			}

			fileInfo2, err := os.Stat("..")
			if err != nil {
				return fmt.Errorf("stating .. error: %s",
					err.Error())
			}

			if os.SameFile(fileInfo1, fileInfo2) {
				break
			}

			if err := os.Chdir(".."); err != nil {
				return fmt.Errorf("Changing directory .. error: %s",
					err.Error())
			}
		}

		if err := syscall.Chroot("."); err != nil {
			return fmt.Errorf("Chrooting into . error: %s",
				err.Error())
		}

		// pivot_root to the root that we want to keep
		if err := rootfd.Chdir(); err != nil {
			return fmt.Errorf("Restoring %s error: %s",
				rootdir, err.Error())
		}

		if err := syscall.PivotRoot(".", "."+oldroot); err != nil {
			return fmt.Errorf("PivotRoot error: %s", err.Error())
		}

		if err := rootfd.Close(); err != nil {
			return fmt.Errorf("Closing rootfd error: %s", err.Error())
		}

		// start a process to unmount oldroot, and in order to save time,
		// we never wait for this process
		cmdUnmount := exec.Command(umount, "-l", oldroot)
		if err := cmdUnmount.Start(); err != nil {
			return fmt.Errorf("Error starting unmounting process: %s",
				err.Error())
		}
	}

	// change the current directory
	wdInfo, err := os.Stat(workingdir)
	if err != nil {
		return fmt.Errorf("Stating %s error: %s", workingdir, err.Error())
	}

	if !wdInfo.IsDir() {
		return fmt.Errorf("Invalid working directory %s", workingdir)
	}

	if err := os.Chdir(workingdir); err != nil {
		return fmt.Errorf("Changing directory to %s error: %s",
			workingdir, err.Error())
	}

	archStr, err := getArchitecture("/")
	if err != nil {
		return fmt.Errorf("Getting architecture string error: %s",
			err.Error())
	}

	// Switch to non-root user
	if err := switchUser(username); err != nil {
		return fmt.Errorf("Switching user error: %s", err.Error())
	}

	if err := setArchitecture(archStr); err != nil {
		return fmt.Errorf("Set architecture error: %s", err.Error())
	}

	wsn, err := findWorkspaceName(rootdir)
	if err != nil {
		return fmt.Errorf("findWorkspaceName error: %s", err.Error())
	}

	shell_cmd := []string{sh, "-l", "-c", "\"$@\"", cmd[0]}
	shell_cmd = append(shell_cmd, cmd...)

	shell_env := os.Environ()
	shell_env = append(shell_env, "A4_CHROOT="+rootdir)
	shell_env = append(shell_env, "QFS_WORKSPACE="+wsn)

	if err := syscall.Exec(shell_cmd[0],
		shell_cmd, shell_env); err != nil {

		return fmt.Errorf("Exec'ing shell command error:%s", err.Error())
	}

	return nil
}

func chroot() {
	// If we do not have root privilege, then gain it now
	if syscall.Geteuid() != 0 {
		sudo_cmd := []string{sudo}
		sudo_cmd = append(sudo_cmd, os.Args...)
		env := os.Environ()

		if err := syscall.Exec(sudo_cmd[0],
			sudo_cmd, env); err != nil {

			fmt.Printf("Exec'ing sudo command error: %s\n",
				err.Error())
			os.Exit(1)
		}

		return
	}

	if len(os.Args) < 6 {
		fmt.Fprintln(os.Stderr, "Not enough arguments.")
		os.Exit(exitBadArgs)
	}

	username := os.Args[2]

	var wsr string
	if absdir, err := filepath.Abs(os.Args[3]); err != nil {
		fmt.Fprintf(os.Stderr, "Error converting <wsr> path %s to absolute "+
			"path: %s\n", os.Args[3], err.Error())
		os.Exit(exitBadArgs)
	} else {
		wsr = absdir
	}

	dir := os.Args[4]

	cmd := make([]string, 0)

	cmd = append(cmd, os.Args[5:]...)

	if !isLegitimateWorkspaceRoot(wsr) {
		fmt.Fprintf(os.Stderr,
			"Invalid workspaceroot: %s, <wsr> must be a"+
				" legitimate workspaceroot\n", wsr)
		os.Exit(exitBadArgs)
	}

	if err := nonPersistentChroot(username, wsr, dir, cmd); err != nil {
		fmt.Fprintf(os.Stderr, "error chrooting: %s", err.Error())
		os.Exit(exitInternalError)
	}
}
