// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import "flag"
import "fmt"

import "github.com/aristanetworks/quantumfs/qlog"

func init() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <qlogPath>")
	}
}

func main() {

	if len(os.Args) < 2 {
		flag.Usage()
		exit(0)
	}
	reader := newReader(os.Args[1])

	// Run indefinitely
	for {


	}
}
