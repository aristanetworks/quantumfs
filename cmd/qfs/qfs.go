// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qfs is a command line tool used to interact with quantumfs and perform various
// special operations not available through normal POSIX interfaces.
package main

import "flag"
import "fmt"
import "os"

import "arista.com/quantumfs"

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
		fmt.Println("         - create a new workspaceN which is a copy of workspaceO")
		fmt.Println("           as of this point in time")
		os.Exit(exitBadCmd)
	}

	cmd := flag.Arg(0)
	switch cmd {
	default:
		fmt.Printf("Unknown command \"%s\"\n", cmd)
		os.Exit(exitBadCmd)

	case "branch":
		branch()
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
