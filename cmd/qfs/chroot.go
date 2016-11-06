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
import "path/filepath"
import "strconv"
import "strings"
import "syscall"
import "time"

import "github.com/kardianos/osext"

const (
	sudo       = "/usr/bin/sudo"
	mount      = "/usr/bin/mount"
	umount     = "/usr/bin/umount"
	netns      = "/usr/bin/netns"
	netnsd     = "/usr/bin/netnsd"
	setarch    = "/usr/bin/setarch"
	cp         = "/usr/bin/cp"
	chns       = "/usr/bin/chns"
	sh         = "/usr/bin/sh"
	bash       = "/usr/bin/bash"
	ArtoolsDir = "/usr/share/Artools"
	oldroot    = "/mnt"
	pivot_root = "/usr/sbin/pivot_root"
)

var qfs string
var persistent bool = true
var setupNamespaces bool = false

func init() {
	if qfspath, err := osext.Executable(); err != nil {
		fmt.Println("Unable to locate qfs directory")
		qfs = "./qfs"
	} else {
		qfs = qfspath
	}
}

// A helper function to run command which gives better error information
func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("Error getting stderr pipe of runCommand: %s\n"+
			"Command: %s %v",
			err.Error(), name, args)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Error starting process in runCommand: %s\n"+
			"Command: %s %v",
			err.Error(), name, args)
	}

	buf, err := ioutil.ReadAll(stderr)
	if err != nil {
		return fmt.Errorf("Error reading stderr in runCommand: %s\n"+
			"Command: %s %v",
			err.Error(), name, args)
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("Error waiting process in runCommand: %s\n"+
			"Command: %s %v\nStderr: %s",
			err.Error(), name, args, string(buf))
	}

	return nil
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

		if isLegitimateWorkspaceRoot(rootdir) {
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

// test whether the netns server is already running
func serverRunning(svrName string) bool {
	cmdServerRun := exec.Command(netns, "-q", svrName)

	if err := cmdServerRun.Run(); err == nil {
		return true
	}

	return false
}

// login the netns server and open a new login shell, which is not
// expected to return
func netnsLogin(rootdir string, svrName string) error {
	env := os.Environ()
	env = append(env, "A4_CHROOT="+rootdir)

	args := []string{netns, svrName, sh, "-l", "-c",
		"\"$@\"", bash, bash}
	if err := syscall.Exec(netns, args, env); err != nil {
		return fmt.Errorf("netnsLogin Exec error: %s", err.Error())
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

		if err := runCommand(mount, "-n", "--bind", src, dst); err != nil {
			return err
		}
	}

	return nil
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
		return fmt.Errorf("Creating directory %s error: %s",
			dstVar, err.Error())
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
		return fmt.Errorf("Getting architecture of workspaceroot"+
			" %s error: %s",
			rootdir, err.Error())
	}

	if err := runCommand(sudo, setarch, archString, netnsd,
		"-d", "--no-netns-env", "-f", "m", "--chroot="+rootdir,
		"--pre-chroot-cmd="+prechrootCmd, svrName); err != nil {
		return err

	}

	return nil
}

func printHelp() {
	fmt.Println("   qfs chroot -- Run a command or shell in the current")
	fmt.Println("                 workspace tree. The chroot environment")
	fmt.Println("                 can be specified to be nonpersistent,")
	fmt.Println("                 or by default it is persistent.\n")
	fmt.Println("   qfs chroot")
	fmt.Println("   qfs chroot --nonpersistent <WSR> <DIR> <CMD>\n")
	fmt.Println("   Options:")
	fmt.Println("      --nonpersistent <WSR> <DIR> <CMD>  Change <WSR> as",
		" the filesystem root,")
	fmt.Println("        enter working directory <DIR> and run command <CMD>")
}

func switchUserMode() error {
	lognameStr := os.Getenv("SUDO_USER")

	logUser, err := user.Lookup(lognameStr)
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

	return nil
}

func profileLog(info string) {
	t := time.Now()
	timestamp := t.Format("00:00:00.000000000")

	fmt.Printf("[%s] %s\n", timestamp, info)
}

func chrootOutOfNsd(rootdir string, workingdir string, cmd []string) error {
	profileLog("Enter chrootOutOfNsd")
	// create a new namespace and run qfs chroot tool in the new namespace
	if !setupNamespaces {
		chroot_args := []string{qfs, "chroot", "--setup-namespaces",
			"--nonpersistent", rootdir, workingdir}
		chroot_args = append(chroot_args, cmd...)

		chns_args := []string{sudo, chns, "-m", "-l", "qfschroot"}
		chns_args = append(chns_args, chroot_args...)

		chns_env := os.Environ()

		profileLog("Run chns")
		if err := syscall.Exec(chns_args[0], chns_args,
			chns_env); err != nil {

			return fmt.Errorf("Execing chns error: %s", err.Error())
		}
	}

	profileLog(" cp 1")
	if err := syscall.Chdir("/"); err != nil {
		return fmt.Errorf("Changing directory to / error: %s", err.Error())
	}

	profileLog(" cp 2")
	rootdirInfo, err := os.Stat(rootdir)
	if err != nil {
		return fmt.Errorf("Stating %s error: %s", rootdir, err.Error())
	}

	profileLog(" cp 3")
	fsrootInfo, err := os.Stat("/")
	if err != nil {
		return fmt.Errorf("Stating / error: %s", err.Error())
	}

	if !os.SameFile(rootdirInfo, fsrootInfo) {
		profileLog(" cp 4")
		if err := runCommand(mount, "-n", "--rbind", rootdir,
			rootdir); err != nil {

			return err
		}

		profileLog(" cp 5")
		dst := rootdir + "/dev"
		makedest("/dev", dst)
		profileLog(" cp 6")
		if err := runCommand(mount, "-n", "-t", "tmpfs", "none",
			dst); err != nil {

			return err
		}
		profileLog(" cp 7")

		if err := runCommand(cp, "-ax", "/dev/.", dst); err != nil {
			return err
		}
		profileLog(" cp 8")

		dst = rootdir + "/var/run/netns"
		if err := os.MkdirAll(dst, 0666); err != nil {
			return fmt.Errorf("Creating directory %s error: %s",
				dst, err.Error())
		}
		profileLog(" cp 9")

		if err := runCommand(mount, "-n", "-t", "tmpfs", "tmpfs",
			dst); err != nil {

			return err
		}
		profileLog(" cp 10")

		if err := setupBindMounts(rootdir); err != nil {
			return err
		}
		profileLog(" cp 11")

		// Remember the current directory so that we can restore it later
		rootfd, err := os.Open(rootdir)
		if err != nil {
			return fmt.Errorf("opening %s error: %s",
				rootdir, err.Error())
		}
		profileLog(" cp 12")

		// In a chroot escape so pivot_root will work
		if err := syscall.Chroot(oldroot); err != nil {
			return fmt.Errorf("chrooting into %s error: %s",
				oldroot, err.Error())
		}
		profileLog(" cp 13")

		// Keep changing to parent directory up to the root
		for {
			fileInfo1, err := os.Stat(".")
			if err != nil {
				return fmt.Errorf("stating . error: %s",
					err.Error())
			}
			profileLog(" cp 14")

			fileInfo2, err := os.Stat("..")
			if err != nil {
				return fmt.Errorf("stating .. error: %s",
					err.Error())
			}
			profileLog(" cp 15")

			if os.SameFile(fileInfo1, fileInfo2) {
				break
			}
			profileLog(" cp 16")

			if err := os.Chdir(".."); err != nil {
				return fmt.Errorf("Changing directory .. error: %s",
					err.Error())
			}
			profileLog(" cp 17")

		}

		if err := syscall.Chroot("."); err != nil {
			return fmt.Errorf("Chrooting into . error: %s",
				err.Error())
		}
		profileLog(" cp 18")

		// pivot_root to the root that we want to keep
		if err := rootfd.Chdir(); err != nil {
			return fmt.Errorf("Retoring %s error: %s",
				rootdir, err.Error())
		}
		profileLog(" cp 19")

		if err := runCommand(pivot_root, ".", "."+oldroot); err != nil {
			return err
		}
		profileLog(" cp 20")

		if err := rootfd.Close(); err != nil {
			return fmt.Errorf("Closing rootfd error: %s", err.Error())
		}
		profileLog(" cp 21")

		// unmount the old file system
		if err := runCommand(umount, "-n", "-l", oldroot); err != nil {
			return err
		}
		profileLog(" cp 22")

	}

	// change the current directory
	wdInfo, err := os.Stat(workingdir)
	if err != nil {
		return fmt.Errorf("Stating %s error: %s", workingdir, err.Error())
	}
	profileLog(" cp 23")

	if !wdInfo.IsDir() {
		return fmt.Errorf("Invalid working directory %s", workingdir)
	}
	profileLog(" cp 24")

	if err := os.Chdir(workingdir); err != nil {
		return fmt.Errorf("Changing directory to %s error: %s",
			workingdir, err.Error())
	}
	profileLog(" cp 25")

	archStr, err := getArchitecture("/")
	if err != nil {
		return fmt.Errorf("Getting architecture string error: %s",
			err.Error())
	}
	profileLog(" cp 26")

	// switch to non-root user
	if err := switchUserMode(); err != nil {
		return fmt.Errorf("Switching usermode error: %s", err.Error())
	}
	profileLog(" cp 27")

	shell_cmd := []string{sh, "-l", "-c", "\"$@\"", cmd[0]}
	shell_cmd = append(shell_cmd, cmd...)

	setarch_cmd := []string{setarch, archStr}
	setarch_cmd = append(setarch_cmd, shell_cmd...)

	setarch_env := os.Environ()
	setarch_env = append(setarch_env, "A4_CHROOT="+rootdir)

	profileLog(" cp 28")
	if err := syscall.Exec(setarch_cmd[0],
		setarch_cmd, setarch_env); err != nil {

		return fmt.Errorf("Exec'ing setarch command error:%s", err.Error())
	}

	return nil
}

func chroot() {
	profileLog("Enter chroot")
	args := os.Args[2:]

	var wsr string
	var dir string
	cmd := make([]string, 0)

ArgumentProcessingLoop:
	for len(args) > 0 {
		switch args[0] {
		case "--nonpersistent":
			persistent = false
			args = args[1:]
			if len(args) < 3 {
				fmt.Fprintln(os.Stderr, "Not enough arguments.")
				printHelp()
				os.Exit(1)
			}

			if absdir, err := filepath.Abs(args[0]); err != nil {
				fmt.Fprintf(os.Stderr, "Error converting path %s"+
					" to absolute path: %s\n",
					args[0], err.Error())
				os.Exit(1)
			} else {
				wsr = absdir
			}

			dir = args[1]

			cmd = append(cmd, args[2:]...)
			break ArgumentProcessingLoop

		case "--setup-namespaces":
			setupNamespaces = true

		default:
			fmt.Fprintln(os.Stderr, "unknown argument:", args[0])
			printHelp()
			os.Exit(1)

		}

		args = args[1:]
	}

	profileLog("Parsing arguments done")
	if !persistent {
		profileLog("Enter test legitimate wsr")
		if !isLegitimateWorkspaceRoot(wsr) {
			fmt.Fprintf(os.Stderr,
				"Invalid workspaceroot: %s, <WSR> must be a"+
					" legitimate workspaceroot\n", wsr)
			printHelp()
			os.Exit(1)
		}
		profileLog("Test legitimate wsr done")

		if err := chrootOutOfNsd(wsr, dir, cmd); err != nil {
			fmt.Fprintln(os.Stderr,
				"chrootOutOfNsd Error: ", err.Error())
			os.Exit(1)
		}

		return
	}

	rootdir, err := findWorkspaceRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr,
			"findWorkspaceRoot Error: ", err.Error())
		os.Exit(1)
	}

	svrName := rootdir + "/chroot"
	if !serverRunning(svrName) {
		if err := chrootInNsd(rootdir, svrName); err != nil {
			fmt.Fprintln(os.Stderr,
				"chrootInNsd Error:", err.Error())
			os.Exit(1)
		}
	}

	err = netnsLogin(rootdir, svrName)
	if err != nil {
		fmt.Fprintln(os.Stderr,
			"netnsLogin Error:", err.Error())
		os.Exit(1)
	}

	return
}
