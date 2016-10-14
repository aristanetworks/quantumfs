// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qfs is a command line tool used to interact with quantumfs and perform various
// special operations not available through normal POSIX interfaces.
package main

import "bytes"
import "flag"
import "fmt"
import "os"
import "os/exec"
import "strings"

import "github.com/aristanetworks/quantumfs"

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk      = iota
	exitBadCmd  = iota
	exitBadArgs = iota
)

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("usage: qfs [options] <command> [ARG1[,ARG2[,...]]]")
		flag.PrintDefaults()
		fmt.Println("Available commands:")
		fmt.Println("  branch <workspaceO> <workspaceN>")
		fmt.Println("         - create a new workspaceN which is a copy of" +
			" workspaceO")
		fmt.Println("           as of this point in time")
		fmt.Println("  chroot")
		fmt.Println("         - Run shell in the specified workspace tree, ")
		fmt.Println("           persistent session and not isolated from")
		fmt.Println("           the rest of the machine")
		fmt.Println("  accessedFiles <workspace>")
		fmt.Println("         - get the access list of workspace")
		fmt.Println("  clearAccessedFiles <workspace>")
		fmt.Println("         - clear the access list of workspace")
		os.Exit(exitBadCmd)
	}

	cmd := flag.Arg(0)
	switch cmd {
	default:
		fmt.Printf("Unknown command \"%s\"\n", cmd)
		os.Exit(exitBadCmd)

	case "branch":
		branch()
	case "chroot":
		chroot()
	case "accessedFiles":
		getAccessed()
	case "clearAccessedFiles":
		clearAccessed()
	}
}

// Implement the branch command, which takes a workspace at the current spot, and
// creates a new workspace with the given name which is identical to the first
// workspace as of right now.
func branch() {
	if flag.NArg() != 3 {
		fmt.Println("Too few arguments for branch command")
		os.Exit(exitBadArgs)
	}

	src := flag.Arg(1)
	dst := flag.Arg(2)

	fmt.Printf("Branching workspace \"%s\" into \"%s\"\n", src, dst)
	api := quantumfs.NewApi()
	err := api.Branch(src, dst)

	if err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

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
func chroot() error {
	// have it hard coded because we still don't have
	// proper workspaces under quantumfs
	rootdir := "/home/bernardy/b160310"
	sudo := "/usr/bin/sudo"
	mount := "/bin/mount"
	//netns := "/usr/bin/netns"
	//netnsd := "/usr/bin/netnsd"
	//svrName := rootdir + "/chroot"
	//setarch := "/usr/bin/setarch"
	cp := "/bin/cp"
	//sh := "/bin/sh"
	bash := "/bin/bash"
	cmd := make([]string, 0)
	cmd = append(cmd, bash)

	msg := fmt.Sprintf("%s %s -n --rbind %s %s;", sudo, mount, rootdir, rootdir)
	prechrootScript := msg
	dstdev := rootdir + "/dev"
	makedest("/dev", dstdev)
	msg = fmt.Sprintf("%s %s -n -t rmpfs none %s;", sudo, mount, dstdev)
	prechrootScript = prechrootScript + msg
	msg = fmt.Sprintf("%s %s -ax /dev/. %s;", sudo, cp, dstdev)
	prechrootScript = prechrootScript + msg
	dstdev = rootdir + "/var/run/netns"
	_, err := os.Stat(dstdev)
	if err != nil && os.IsNotExist(err) {
		err = os.Mkdir(dstdev, 0666)
		if err != nil {
			return err
		}
	}
	msg = fmt.Sprintf("%s %s -n -t tmpfs tmpfs %s;", sudo, mount, dstdev)
	prechrootScript = prechrootScript + msg
	paths := []string{"/proc", "/selinux", "/sys",
		"/dev/pts", "/tmp/.X11-unix", "/tmp/ArosTest.SimulatedDut"}
	paths = append(paths, "/home/bernardy")
	paths = append(paths, "/home/arastra")

	for i := 0; i < len(paths); i++ {
		src := paths[i]
		dst := rootdir + paths[i]
		if !makedest(src, dst) {
			continue
		}

		msg = fmt.Sprintf("%s %s -n --bind %s %s;", sudo, mount, src, dst)
		prechrootScript = prechrootScript + msg
	}
	fmt.Println(prechrootScript)
	return nil
}

// This function will be used to find the correct workpsace and
// mountpoint to chroot into by checking the legitimacy of all
// parent directories
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

// Implement the accessed command
func getAccessed() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for getAccessed command")
		os.Exit(exitBadArgs)
	}

	workspaceName := flag.Arg(1)

	fmt.Printf("Getting the accessed list of Workspace:\"%s\"\n", workspaceName)
	api := quantumfs.NewApi()
	err := api.GetAccessed(workspaceName)

	if err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

// Implement the clearaccessed command
func clearAccessed() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for clearAccessed command")
		os.Exit(exitBadArgs)
	}

	wsr := flag.Arg(1)

	fmt.Printf("Clearing the accessed list of WorkspaceRoot:\"%s\"\n", wsr)
	api := quantumfs.NewApi()
	err := api.ClearAccessed(wsr)

	if err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}
