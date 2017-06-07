// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import "flag"
import "fmt"
import "os"

import "github.com/aristanetworks/quantumfs/qlog"

func init() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <qlogPath>\n", os.Args[0])
	}
}

func main() {
	if len(os.Args) < 2 {
		flag.Usage()
		return
	}
	reader := qlog.NewReader(os.Args[1])

	reader.ProcessLogs(qlog.ReadThenTail, func(v qlog.LogOutput) {
		fmt.Printf(v.ToString())
	})
}
