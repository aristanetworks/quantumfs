// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qfs is a command line tool used to interact with quantumfs and perform various
// special operations not available through normal POSIX interfaces.
package main

import "fmt"
import "flag"

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("usage: qfs [options] <command> [ARG1[,ARG2[,...]]]")
		flag.PrintDefaults()
		fmt.Println("Available commands:")
		fmt.Println("  branch <workspaceO> <workspaceN>")
		fmt.Println("         - create a new workspaceN which is a copy of workspaceO")
		fmt.Println("           as of this point in time")
	}
}
