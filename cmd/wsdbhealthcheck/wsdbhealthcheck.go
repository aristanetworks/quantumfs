// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Utility to check the health of wsdb serivce by connecting, executing a couple
// simple API calls and then existing with the status.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends"
	"github.com/aristanetworks/quantumfs/qlog"
)

const (
	HealthOK   = 0
	HealthBad  = 1
	CheckError = 2
)

func healthIsBad(msg string, err error) {
	errMsg := "No error"
	if err != nil {
		errMsg = err.Error()
	}

	fmt.Printf("Health check failed: %s: %s\n", msg, errMsg)
	os.Exit(HealthBad)
}

func main() {
	address := flag.String("address", "",
		"Address of the service to connect to. "+
			"e.g. localhost or service:2222")
	flag.Parse()

	if *address == "" {
		fmt.Printf("Usage: wsdbhealthcheck -address <address>\n")
		os.Exit(CheckError)
	}

	qlog, err := qlog.NewQlog("")
	if err != nil {
		fmt.Printf("Failed to init qlog: %s\n", err.Error())
		os.Exit(CheckError)
	}

	c := &quantumfs.Ctx{
		Qlog:      qlog,
		RequestId: 1,
	}

	// Connection check
	connectWait := make(chan quantumfs.WorkspaceDB)

	go func() {
		wsdb, err := backends.ConnectWorkspaceDB("grpc", *address)
		if err != nil {
			healthIsBad("Connection error", err)
		}
		connectWait <- wsdb
	}()

	var wsdb quantumfs.WorkspaceDB

	select {
	case <-time.After(30 * time.Second):
		// We must be able to connect within the limited time
		healthIsBad("Connection timeout", nil)
	case wsdb = <-connectWait:
	}

	// API check
	apiWait := make(chan struct{})

	go func() {
		typespaces, err := wsdb.TypespaceList(c)
		if err != nil {
			healthIsBad("Typespacelist error", err)
		}

		if len(typespaces) == 0 {
			healthIsBad("No typespaces returned", nil)
		}

		_, _, err = wsdb.Workspace(c, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		if err != nil {
			healthIsBad("Error retreiving null workspace", err)
		}

		close(apiWait)
	}()

	select {
	case <-time.After(2 * time.Second):
		// We must be able to perform the sanity API checks within the
		// limited time.
		healthIsBad("Timeout running API check", nil)
	case <-apiWait:
	}

	fmt.Printf("Health ok\n")
	os.Exit(HealthOK)
}
