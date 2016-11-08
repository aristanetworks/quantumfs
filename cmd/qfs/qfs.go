// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qfs is a command line tool used to interact with quantumfs and perform various
// special operations not available through normal POSIX interfaces.
package main

import "flag"
import "fmt"
import "os"
import "strconv"

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
		fmt.Println("         - Run shell in the specified workspace tree")
		fmt.Println("  accessedFiles <workspace>")
		fmt.Println("         - get the access list of workspace")
		fmt.Println("  clearAccessedFiles <workspace>")
		fmt.Println("         - clear the access list of workspace")
		fmt.Println("  duplicate <dstPath> <key> <uid> <gid> <permission>")
		fmt.Println("         - copy an inode correponding to a extended" +
			" key under the location of dstPath with specifications of" +
			" user <uid>, group <gid>, and RWX permission <permission>")
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
	case "duplicate":
		duplicate()
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

	if err := api.Branch(src, dst); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
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

	if err := api.GetAccessed(workspaceName); err != nil {
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

	if err := api.ClearAccessed(wsr); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

// Implement the duplicate command
func duplicate() {
	if flag.NArg() != 6 {
		fmt.Println("Too few arguments for duplicate command")
		os.Exit(exitBadArgs)
	}

	dst := flag.Arg(1)
	key := []byte(flag.Arg(2))
	Uid, err := strconv.ParseUint(flag.Arg(3), 10, 16)
	if err != nil {
		fmt.Println("Invalid Uid:", err)
		os.Exit(exitBadArgs)
	}
	uid := uint16(Uid)

	Gid, err := strconv.ParseUint(flag.Arg(4), 10, 16)
	if err != nil {
		fmt.Println("Invalid Gid:", err)
		os.Exit(exitBadArgs)
	}
	gid := uint16(Gid)

	Permission, err := strconv.ParseUint(flag.Arg(5), 10, 32)
	if err != nil {
		fmt.Println("Invalid Permission:", err)
		os.Exit(exitBadArgs)
	}
	permission := uint32(Permission)

	fmt.Printf("Duplicate inode \"%v\" into \"%s\" with %d, %d and 0%o\n",
		key, dst, uid, gid, permission)
	api := quantumfs.NewApi()

	if err := api.DuplicateObject(dst, key, permission, uid, gid); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}
