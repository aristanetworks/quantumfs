// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/cql"
	qubit "github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
	"github.com/aristanetworks/qubit/tools/utils/cmdproc"
)

var walkFlags *flag.FlagSet
var version string

// options usable by all commands
type commonOpts struct {
	config   string
	progress bool
}

var co commonOpts

// state usable by all commands
type commonState struct {
	ttlCfg *qubitutils.TTLConfig
	cqlds  blobstore.BlobStore
	cqldb  qubit.WorkspaceDB
	qfsds  quantumfs.DataStore
	qfsdb  quantumfs.WorkspaceDB
	ctx    *Ctx
}

var cs commonState

func setupCommonState() error {
	var err error
	if co.config == "" {
		return cmdproc.NewPreCmdExitErr("configuration file must be specified")
	}

	cs.ttlCfg, err = qubitutils.LoadTTLConfig(co.config)
	if err != nil {
		return cmdproc.NewPreCmdExitErr("Failed to load TTL values: %s", err)
	}

	cs.qfsds, err = thirdparty_backends.ConnectDatastore("ether.cql",
		co.config)
	if err != nil {
		return cmdproc.NewPreCmdExitErr("Connection to DataStore failed: %s", err)
	}
	v, ok := cs.qfsds.(*thirdparty_backends.EtherBlobStoreTranslator)
	if !ok {
		return cmdproc.NewPreCmdExitErr("Non-ether datastore found")
	}
	v.ApplyTTLPolicy = false
	cs.cqlds = v.Blobstore

	cs.qfsdb, err = thirdparty_backends.ConnectWorkspaceDB("ether.cql", co.config)
	if err != nil {
		return cmdproc.NewPreCmdExitErr("Connection to workspaceDB failed: %s", err)
	}
	cs.cqldb = cql.NewUncachedWorkspaceDB(co.config)

	cs.ctx = newCtx()
	return nil
}

func main() {
	setupTemplates()

	walkFlags = flag.NewFlagSet("Walker cmd", flag.ExitOnError)
	walkFlags.StringVar(&co.config, "cfg", "", "datastore and workspaceDB config file")
	walkFlags.BoolVar(&co.progress, "progress", false, "show progress")

	walkFlags.Usage = cmdproc.Usage
	walkFlags.Parse(os.Args[1:])
	args := walkFlags.Args()
	start := time.Now()

	cmdproc.ProcessCommands(setupCommonState, args)

	commandTime := time.Since(start)
	fmt.Printf("Duration: %v\n", commandTime)
}
