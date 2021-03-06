// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/cmd/cqlwalker/utils"
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
	ttlCfg *utils.TTLConfig
	cqlds  cql.BlobStore
	cqldb  cql.WorkspaceDB
	qfsds  quantumfs.DataStore
	qfsdb  quantumfs.WorkspaceDB
	ctx    *Ctx
}

var cs commonState

func setupCommonState() error {
	var err error
	if co.config == "" {
		return NewPreCmdExitErr("configuration file must be " +
			"specified")
	}

	cs.ttlCfg, err = utils.LoadTTLConfig(co.config)
	if err != nil {
		return NewPreCmdExitErr("Failed to load TTL values: %s", err)
	}

	cs.qfsds, err = backends.ConnectDatastore("cql", co.config)
	if err != nil {
		return NewPreCmdExitErr("Connection to DataStore failed: %s",
			err)
	}
	v, ok := cs.qfsds.(*cql.CqlBlobStoreTranslator)
	if !ok {
		return NewPreCmdExitErr("Non-cql datastore found")
	}
	v.ApplyTTLPolicy = false
	cs.cqlds = v.Blobstore

	cs.qfsdb, err = backends.ConnectWorkspaceDB("cql", co.config)
	if err != nil {
		return NewPreCmdExitErr("Connection to workspaceDB "+
			"failed: %s", err)
	}
	cs.cqldb = cql.NewUncachedWorkspaceDB(co.config)

	cs.ctx = newCtx()
	return nil
}

func main() {
	setupTemplates()

	walkFlags = flag.NewFlagSet("Walker cmd", flag.ExitOnError)
	walkFlags.StringVar(&co.config, "cfg", "",
		"datastore and workspaceDB config file")
	walkFlags.BoolVar(&co.progress, "progress", false, "show progress")

	walkFlags.Usage = Usage
	walkFlags.Parse(os.Args[1:])
	args := walkFlags.Args()
	start := time.Now()

	ProcessCommands(setupCommonState, args)

	commandTime := time.Since(start)
	fmt.Printf("Duration: %v\n", commandTime)
}
