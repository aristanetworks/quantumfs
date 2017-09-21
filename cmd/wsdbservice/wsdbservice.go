// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// wsdbservice is the daemon which implements pub-sub support for all the quantumfsd
// instances on top of an existing persistent workspace DB implementation.
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/aristanetworks/quantumfs/grpc/rpc"
	"github.com/aristanetworks/quantumfs/grpc/server"
	"github.com/aristanetworks/quantumfs/qlog"
)

const (
	exitOK = iota
	exitFailed
	exitInitFailed
)

var version string

var logPath string
var wsdbName string
var wsdbConfig string
var port uint

func init() {
	fmt.Printf("wsdbService version %s\n", version)

	flag.StringVar(&logPath, "logPath", "", "Path to place qlog file")

	flag.UintVar(&port, "port", uint(rpc.Ports_Default), "Port to listen on")

	flag.StringVar(&wsdbName, "workspaceDB", "processlocal",
		"Name of the WorkspaceDB to use")
	flag.StringVar(&wsdbConfig, "workspaceDBconf", "",
		"Options to pass to workspaceDB")
}

func processArgs() {
	flag.Parse()
}

func main() {
	processArgs()

	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var logger *qlog.Qlog
	var err error
	if logPath == "" {
		logger, err = qlog.NewQlog("")
	} else {
		logger, err = qlog.NewQlogExt(logPath, 100*1024*1024, version,
			qlog.PrintToStdout)
	}

	if err != nil {
		fmt.Printf("Failed to initialize logger: %s\n", err.Error())
		os.Exit(exitInitFailed)
	}

	_, err = server.StartWorkspaceDbd(logger, uint16(port), wsdbName,
		wsdbConfig)
	if err != nil {
		fmt.Printf("Failed to initialize: %s\n", err.Error())
		os.Exit(exitFailed)
	}

	select {}
}
