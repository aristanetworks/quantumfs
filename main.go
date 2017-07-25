// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/ether/blobstore"
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

func main() {

	walkFlags = flag.NewFlagSet("Walker cmd", flag.ExitOnError)

	config := walkFlags.String("cfg", "", "datastore and workspaceDB config file")
	progress := walkFlags.Bool("progress", false, "show progress")

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

	walkFlags.Parse(os.Args[1:])

	if *config == "" {
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	ttlCfg, err := qubitutils.LoadTTLConfig(*config)
	if err != nil {
		fmt.Printf("Failed to init ether.cql TTL values: %s\n",
			err.Error())
		os.Exit(exitBadConfig)
	}

	qfsds, err := thirdparty_backends.ConnectDatastore("ether.cql", *config)
	if err != nil {
		fmt.Printf("Connection to DataStore failed")
		os.Exit(exitBadConfig)
	}
	var cqlds blobstore.BlobStore
	if v, ok := qfsds.(*thirdparty_backends.EtherBlobStoreTranslator); ok {
		cqlds = v.Blobstore
		v.ApplyTTLPolicy = false
	}

	qfsdb, err := thirdparty_backends.ConnectWorkspaceDB("ether.cql", *config)
	if err != nil {
		fmt.Printf("Connection to workspaceDB failed")
		os.Exit(exitBadConfig)
	}

	c := newCtx()
	start := time.Now()

	switch walkFlags.Arg(0) {
	case "du":
		err = handleDiskUsage(c, *progress, qfsds, qfsdb)
	case "keycount":
		err = handleKeyCount(c, *progress, qfsds, qfsdb)
	case "keydiffcount":
		err = handleKeyDiffCount(c, *progress, qfsds, qfsdb)
	case "ttl":
		err = handleTTL(c, *progress, qfsds, cqlds, qfsdb, ttlCfg)
	case "forceTTL":
		err = handleForceTTL(c, *progress, qfsds, cqlds, qfsdb)
	case "list":
		err = printList(c, *progress, qfsds, cqlds, qfsdb)
	case "ttlHistogram":
		err = printTTLHistogram(c, *progress, qfsds, cqlds, qfsdb)
	case "path2key":
		err = printPath2Key(c, *progress, qfsds, qfsdb)
	case "findconstantkeys":
		err = printConstantKeys(c, *progress, qfsds, cqlds, qfsdb)
	default:
		fmt.Println("Unsupported walk sub-command: ", walkFlags.Arg(0))
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	walkTime := time.Since(start)
	if err != nil {
		fmt.Printf("Error: %v\n", err.Error())
		os.Exit(exitBadCmd)
	}

	fmt.Printf("Duration: %v\n", walkTime)
}
