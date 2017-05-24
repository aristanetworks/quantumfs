// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import "flag"
import "fmt"
import "os"
import "time"

import "github.com/aristanetworks/quantumfs/qlog"

func init() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <qlogPath>\n")
	}
}

func main() {

	if len(os.Args) < 2 {
		flag.Usage()
		return
	}
	reader := qlog.NewReader(os.Args[1])

	// Run indefinitely
	for {
		newLogs := reader.ReadMore()
		for _, v := range newLogs {
			fmt.Printf(v.ToString())
		}

		time.Sleep(10 * time.Millisecond)
	}
}
