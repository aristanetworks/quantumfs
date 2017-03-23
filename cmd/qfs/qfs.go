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

var version string

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk      = iota
	exitBadCmd  = iota
	exitBadArgs = iota
)

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("qfs version", version)
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
		fmt.Println("  insertInode <dstPath> <key> <uid> <gid> <permission>")
		fmt.Println("         - copy an inode correponding to a extended" +
			" key under the location of dstPath with specifications of" +
			" user <uid>, group <gid>, and RWX permission <permission>" +
			" in octal format")
		fmt.Println("  deleteWorkspace <workspace>")
		fmt.Println("         - delete <workspace> from the WorkspaceDB")
		fmt.Println("  enableRootWrite <workspace>")
		fmt.Println("         - enable <workspace> the write permission")
		fmt.Println("  setWorkspaceImmutable <workspace>")
		fmt.Println("         - make <workspace> irreversibly immutable")
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
	case "insertInode":
		insertInode()
	case "sync":
		sync()
	case "deleteWorkspace":
		deleteWorkspace()
	case "enableRootWrite":
		enableRootWrite()
	case "setWorkspaceImmutable":
		setWorkspaceImmutable()
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

// Implement the insertInode command
func insertInode() {
	if flag.NArg() != 6 {
		fmt.Println("Too few arguments for insertInode command")
		os.Exit(exitBadArgs)
	}

	dst := flag.Arg(1)
	key := flag.Arg(2)
	Uid, err := strconv.ParseUint(flag.Arg(3), 10, 16)
	if err != nil {
		fmt.Println("Invalid Uid:", err)
		os.Exit(exitBadArgs)
	}
	uid := uint32(Uid)

	Gid, err := strconv.ParseUint(flag.Arg(4), 10, 16)
	if err != nil {
		fmt.Println("Invalid Gid:", err)
		os.Exit(exitBadArgs)
	}
	gid := uint32(Gid)

	Permission, err := strconv.ParseUint(flag.Arg(5), 8, 32)
	if err != nil {
		fmt.Println("Invalid Permission:", err)
		os.Exit(exitBadArgs)
	}
	permission := uint32(Permission)

	fmt.Printf("Insert inode \"%v\" into \"%s\" with %d, %d and 0%o\n",
		key, dst, uid, gid, permission)
	api := quantumfs.NewApi()

	if err := api.InsertInode(dst, key, permission, uid, gid); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

func sync() {
	api := quantumfs.NewApi()

	api.SyncAll()
	fmt.Println("Synced.")
}

func deleteWorkspace() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for delete workspace command")
		os.Exit(exitBadArgs)
	}

	workspace := flag.Arg(1)

	fmt.Printf("Deleting workspace \"%s\"\n", workspace)
	api := quantumfs.NewApi()

	if err := api.DeleteWorkspace(workspace); err != nil {
		fmt.Println("Delete failed:", err)
		os.Exit(exitBadArgs)
	}
}

func enableRootWrite() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for enable workspace" +
			" write permission")
		os.Exit(exitBadArgs)
	}

	workspace := flag.Arg(1)

	fmt.Printf("Enabling workspace \"%s\" the write permission\n", workspace)
	api := quantumfs.NewApi()

	if err := api.EnableRootWrite(workspace); err != nil {
		fmt.Println("EnableRootWrite failed:", err)
		os.Exit(exitBadArgs)
	}
}

func setWorkspaceImmutable() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for enable workspace" +
			" write permission")
		os.Exit(exitBadArgs)
	}

	workspace := flag.Arg(1)

	fmt.Printf("Set workspace \"%s\" immutable\n", workspace)
	api := quantumfs.NewApi()

	if err := api.SetWorkspaceImmutable(workspace); err != nil {
		fmt.Println("SetWorkspaceImmutable failed:", err)
		os.Exit(exitBadArgs)
	}
}
