// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "fmt"
import "os"
import "github.com/aristanetworks/quantumfs/qlog"

func main() {

	if len(os.Args) < 2 {
		fmt.Println("Please provide log filepath as argument")
		return
	}

	fmt.Println(qlog.ParseLogs(os.Args[1]))
}
