// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qfs is a command line tool used to interact with QuantumFS and perform various
// special operations not available through normal POSIX interfaces.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/aristanetworks/quantumfs"
)

var version string

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk            = iota
	exitBadCmd        = iota
	exitBadArgs       = iota
	exitApiNotFound   = iota
	exitInternalError = iota
)

func printUsage() {
	fmt.Println("qfs version", version)
	fmt.Println("usage: qfs [options] <command> [ARG1[,ARG2[,...]]]")
	flag.PrintDefaults()
	fmt.Println()
	fmt.Println("Available commands:")
	fmt.Println("  branch <workspaceO> <workspaceN>")
	fmt.Println("         - create a new workspaceN which is a copy of" +
		" workspaceO")
	fmt.Println("           as of this point in time")
	fmt.Println("  chroot <username> <wsr> <dir> <cmd>")
	fmt.Println("         - Run shell chrooted into the specified workspace")
	fmt.Println("           username - Username to use inside chroot")
	fmt.Println("           wsr - Path to workspace root")
	fmt.Println("           dir - Path to change to within the chroot")
	fmt.Println("           cmd - Command to run after chrooting, may")
	fmt.Println("                 be several arguments long")
	fmt.Println("  accessedFiles <workspace>")
	fmt.Println("         - get the access list of workspace")
	fmt.Println("  clearAccessedFiles <workspace>")
	fmt.Println("         - clear the access list of workspace")
	fmt.Println("  cp [-o] <srcPath> <dstPath> - Copy a directory using " +
		"insertInode")
	fmt.Println("         -o Overwrite files which exist in the destination")
	fmt.Println("  insertInode <dstPath> <key> <uid> <gid> <permission>")
	fmt.Println("         - copy an inode corresponding to an extended" +
		" key under the location of dstPath with specifications of" +
		" user <uid>, group <gid>, and RWX permission <permission>" +
		" in octal format")
	fmt.Println("  deleteWorkspace <workspace>")
	fmt.Println("         - delete <workspace> from the WorkspaceDB")
	fmt.Println("  enableRootWrite <workspace>")
	fmt.Println("         - enable <workspace> the write permission")
	fmt.Println("  setWorkspaceImmutable <workspace>")
	fmt.Println("         - make <workspace> irreversibly immutable")
	fmt.Println("  advanceWSDB <workspace> <referenceWorkspace>")
	fmt.Println("  refresh <workspace>")
	fmt.Println("  merge [-nlr] <base> <remote> <local> [[path/to/skip] ...]")
	fmt.Println("          - Three-way workspace merge")
	fmt.Println("          -n - Prefer newer in conflicts (default)")
	fmt.Println("          -l - Prefer local in conflicts")
	fmt.Println("          -r - Prefer remote in conflicts")
	fmt.Println("          dir/to/skip - List of paths to not merge")
	fmt.Println("  syncWorkspace <workspace>")
}

func main() {
	displayHelp := false
	flag.BoolVar(&displayHelp, "help", false, "Display usage help")
	flag.Parse()

	if flag.NArg() == 0 || displayHelp {
		printUsage()
		os.Exit(exitOk)
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
	case "cp":
		cp()
	case "insertInode":
		insertInode()
	case "sync":
		syncAll()
	case "deleteWorkspace":
		deleteWorkspace()
	case "enableRootWrite":
		enableRootWrite()
	case "setWorkspaceImmutable":
		setWorkspaceImmutable()
	case "refresh":
		refresh()
	case "merge":
		merge()
	case "advanceWSDB":
		advanceWSDB()
	case "syncWorkspace":
		syncWorkspace()
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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

	pathList, err := api.GetAccessed(workspaceName)
	if err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}

	for path, flags := range pathList.Paths {
		created := "-"
		if flags.Created() {
			created = "C"
		}
		read := "-"
		if flags.Read() {
			read = "R"
		}
		updated := "-"
		if flags.Updated() {
			updated = "U"
		}
		deleted := "-"
		if flags.Deleted() {
			deleted = "D"
		}
		fmt.Printf("%s: directory-%t %s%s%s%s\n", path, flags.IsDir(),
			created, read, updated, deleted)
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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

	if err := api.ClearAccessed(wsr); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

func refresh() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for refresh command")
		os.Exit(exitBadArgs)
	}
	workspace := flag.Arg(1)

	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}
	if err := api.Refresh(workspace); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

func merge() {
	if flag.NArg() < 4 {
		fmt.Println("Too few arguments for merge command")
		os.Exit(exitBadArgs)
	}

	prefer := quantumfs.PreferNewer

	base := flag.Arg(1)
	remote := flag.Arg(2)
	local := flag.Arg(3)
	skipPathStart := 4

	if flag.NArg() > 4 && flag.Arg(1)[0] == '-' {
		base = flag.Arg(2)
		remote = flag.Arg(3)
		local = flag.Arg(4)
		skipPathStart = 5

		for _, char := range flag.Arg(1)[1:] {
			switch char {
			default:
				fmt.Printf("Unknown flag %c\n", char)
				os.Exit(exitBadArgs)
			case 'n':
				prefer = quantumfs.PreferNewer
			case 'l':
				prefer = quantumfs.PreferLocal
			case 'r':
				prefer = quantumfs.PreferRemote
			}
		}
	}

	skipPaths := make([]string, 0, flag.NArg()-3)

	for i := skipPathStart; i < flag.NArg(); i++ {
		skipPaths = append(skipPaths, flag.Arg(i))
	}

	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}
	err = api.Merge3Way(base, remote, local, prefer, skipPaths)
	if err != nil {
		fmt.Println("Operation failed:", err)
		os.Exit(exitBadArgs)
	}
}

func advanceWSDB() {
	if flag.NArg() != 3 {
		fmt.Println("Too few arguments for advanceWSDB command")
		os.Exit(exitBadArgs)
	}
	workspace := flag.Arg(1)
	referenceWorkspace := flag.Arg(2)

	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}
	if err := api.AdvanceWSDB(workspace, referenceWorkspace); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

func syncWorkspace() {
	if flag.NArg() != 2 {
		fmt.Println("Too few arguments for syncWorkspace command")
		os.Exit(exitBadArgs)
	}
	workspace := flag.Arg(1)

	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}
	if err := api.SyncWorkspace(workspace); err != nil {
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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

	if err := api.InsertInode(dst, key, permission, uid, gid); err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

func syncAll() {
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

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
	api, err := quantumfs.NewApi()
	if err != nil {
		fmt.Println("Failed to find API:", err)
		os.Exit(exitApiNotFound)
	}

	if err := api.SetWorkspaceImmutable(workspace); err != nil {
		fmt.Println("SetWorkspaceImmutable failed:", err)
		os.Exit(exitBadArgs)
	}
}
