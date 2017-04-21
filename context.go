// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"fmt"
	"os"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
)

type Ctx struct {
	qctx     *quantumfs.Ctx
	wsdb     quantumfs.WorkspaceDB
	confFile string
}

func getWalkerDaemonContext(mode string, config string,
	logdir string) *Ctx {

	// Connect to ether.cql WorkSpaceDB
	db, err := thirdparty_backends.ConnectWorkspaceDB("ether.cql", config)
	if err != nil {
		fmt.Printf("Connection to workspaceDB failed\n")
		os.Exit(exitBadConfig)
	}

	return &Ctx{
		qctx:     newCtx(logdir),
		wsdb:     db,
		confFile: config,
	}
}

func newCtx(logdir string) *quantumfs.Ctx {
	log := qlog.NewQlogTiny()
	if logdir != "" {
		log = qlog.NewQlog(logdir)
	}

	c := &quantumfs.Ctx{
		Qlog:      log,
		RequestId: 1,
	}
	return c
}
