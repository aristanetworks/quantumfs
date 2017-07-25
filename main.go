// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk        = iota
	exitBadCmd    = iota
	exitBadArgs   = iota
	exitBadConfig = iota
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
	qfsds  quantumfs.DataStore
	qfsdb  quantumfs.WorkspaceDB
	ctx    *quantumfs.Ctx
}

var cs commonState

func setupCommonState() {
	var err error

	if co.config == "" {
		fmt.Fprintf(os.Stderr, "Error: configuration file must be specified")
		os.Exit(exitBadConfig)
	}

	cs.ttlCfg, err = qubitutils.LoadTTLConfig(co.config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to load TTL values: %s\n", err)
		os.Exit(exitBadConfig)
	}

	cs.qfsds, err = thirdparty_backends.ConnectDatastore("ether.cql",
		co.config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Connection to DataStore failed: %s\n",
			err)
		os.Exit(exitBadConfig)
	}
	v, ok := cs.qfsds.(*thirdparty_backends.EtherBlobStoreTranslator)
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: Non-ether datastore found")
		os.Exit(exitBadConfig)
	}
	v.ApplyTTLPolicy = false
	cs.cqlds = v.Blobstore

	cs.qfsdb, err = thirdparty_backends.ConnectWorkspaceDB("ether.cql", co.config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Connection to workspaceDB failed: %s\n",
			err)
		os.Exit(exitBadConfig)
	}

	cs.ctx = newCtx()
}

func main() {

	walkFlags = flag.NewFlagSet("Walker cmd", flag.ExitOnError)
	walkFlags.Usage = func() {
		fmt.Println("qubit-walkercmd version", version)
		fmt.Println("usage: walker -cfg <config> [-progress] <sub-command> ARG1[,ARG2[,...]]")
		fmt.Println()
		fmt.Println("This tool walks all the keys within a workspace")
		fmt.Println("and invokes a sub-command on selected objects")
		fmt.Println()
		fmt.Println("Available sub-commands:")
		fmt.Println("  du <workspace> <path>")
		fmt.Println("           - calculate the size(approx) on disk for the given workspace.")
		fmt.Println("           - path is from the root of workspace.")
		fmt.Println("  findconstantkeys <workspace> <num_days>")
		fmt.Println("           - list all keys and their paths, where key type is Constant")
		fmt.Println("             but it is not in the constant datastore and its ttl is more")
		fmt.Println("             than num_days.")
		fmt.Println("  forceTTL <workspace> <new ttl(hrs)>")
		fmt.Println("           - update ttl of all the blocks in the workspace")
		fmt.Println("             in the given path to the given ttl value.")
		fmt.Println("           - ttl is updated only if it is less than new ttl value.")
		fmt.Println("             path is from the root of workspace.")
		fmt.Println("  keycount <workspace> [dedupe]")
		fmt.Println("           - count the number of keys in given workspace.")
		fmt.Println("           - optionally show the dedupe details within this workspace.")
		fmt.Println("  keydiffcount <workspace1> <workspace2> [keys]")
		fmt.Println("           - count the diff in number of keys in between")
		fmt.Println("             the given workspaces.")
		fmt.Println("           - optionally show the unique keys.")
		fmt.Println("  list")
		fmt.Println("           - list all workspaces.")
		fmt.Println("  path2key <workspace> <path>")
		fmt.Println("           - given a path in a workspace, print its key.")
		fmt.Println("  ttl <workspace>")
		fmt.Println("           - update ttl of all the blocks in the workspace as")
		fmt.Println("             per the ttl values in the config file.")
		fmt.Println("  ttlHistogram <workspace>")
		fmt.Println("           - bucket all the blocks in the workspace")
		fmt.Println("             into different ttl values.")
		fmt.Println()
		walkFlags.PrintDefaults()
	}

	var err error
	walkFlags.StringVar(&co.config, "cfg", "", "datastore and workspaceDB config file")
	walkFlags.BoolVar(&co.progress, "progress", false, "show progress")

	walkFlags.Parse(os.Args[1:])

	// failure in setting up common state causes error exit
	setupCommonState()

	start := time.Now()

	switch walkFlags.Arg(0) {
	case "du":
		err = handleDiskUsage()
	case "keycount":
		err = handleKeyCount()
	case "keydiffcount":
		err = handleKeyDiffCount()
	case "ttl":
		err = handleTTL()
	case "forceTTL":
		err = handleForceTTL()
	case "list":
		err = printList()
	case "ttlHistogram":
		err = printTTLHistogram()
	case "path2key":
		err = printPath2Key()
	case "findconstantkeys":
		err = printConstantKeys()
	default:
		fmt.Fprintf(os.Stderr, "Unsupported walk sub-command: %s", walkFlags.Arg(0))
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	walkTime := time.Since(start)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(exitBadCmd)
	}

	fmt.Printf("Duration: %v\n", walkTime)
}
