// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "flag"
import "fmt"
import "os"
import "github.com/aristanetworks/quantumfs/qlog"

var tabSpaces *int
var file *string

func init() {
	tabSpaces = flag.Int("tab", 0, "Indent function logs with n spaces")
	file = flag.String("f", "", "Log file to parse (required)")

	flag.Usage = func() {
		fmt.Printf("Usage: %s -f <filepath> [flags]\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if len(*file) == 0 {
		flag.Usage()
		return
	}

	fmt.Println(qlog.ParseLogsExt(*file, *tabSpaces))
}
