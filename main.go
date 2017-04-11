// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk        = iota
	exitBadCmd    = iota
	exitBadArgs   = iota
	exitBadConfig = iota
)

var walkFlags *flag.FlagSet

func main() {

	walkFlags = flag.NewFlagSet("Walker cmd", flag.ExitOnError)

	config := walkFlags.String("cfg", "", "datastore and workspaceDB config file")

	walkFlags.Usage = func() {
		fmt.Println("This tool walks all the keys within a workspace" +
			"and executes a subcommand on selected objects")

		fmt.Println("Available commands:")
		fmt.Println("usage: walker <config> <command> ARG1[,ARG2[,...]]")

		fmt.Println("  keycount <workspace>")
		fmt.Println("           - count the number of keys in given workspace")
		fmt.Println("  keydiffcount <workspace1> <workspace2>")
		fmt.Println("           - count the diff in number of keys in between")
		fmt.Println("	          the given workspaces")
		fmt.Println("  du <workspace>  <path>")
		fmt.Println("           - calculate the size on disk for the given workspace.")
		fmt.Println("             path is from the root of workspace")
		fmt.Println("  ttl <workspace>")
		fmt.Println("           - update TTL of all the blocks in the worksace as ")
		fmt.Println("             per the TTL values in the config file")
		fmt.Println("  setttl <workspace> <path> <threshold ttl(hrs)> <new ttl(hrs)>")
		fmt.Println("           - update TTL of all the blocks in the worksace ")
		fmt.Println("             in the given path to the given TTL value.")
		fmt.Println("             path is from the root of workspace")
		fmt.Println("  list")
		fmt.Println("           - list all workspaces")
		fmt.Println()
		walkFlags.PrintDefaults()
	}

	walkFlags.Parse(os.Args[1:])

	if *config == "" {
		walkFlags.Usage()
		os.Exit(exitBadConfig)
	}

	err := loadCqlWalkerConfig(*config)
	if err != nil {
		fmt.Printf("Failed to init ether.cql TTL values: %s\n",
			err.Error())
		os.Exit(exitBadConfig)
	}

	cqlds, err := cql.NewCqlBlobStore(*config)
	if err != nil {
		fmt.Printf("Failed to init ether.cql datastore: %s\n",
			err.Error())
		os.Exit(exitBadConfig)
	}

	qfsds := getDataStore(cqlds)
	if qfsds == nil {
		fmt.Printf("Connection to dataStore failed")
		os.Exit(exitBadConfig)
	}

	qfsdb, err := thirdparty_backends.ConnectWorkspaceDB("ether.cql", *config)
	if err != nil {
		fmt.Printf("Connection to workspaceDB failed")
		os.Exit(exitBadConfig)
	}

	c := newCtx()
	start := time.Now()

	// TODO: build a progress bar to indicate tool progress
	// With option only
	switch walkFlags.Arg(0) {
	case "du":
		err = handleDiskUsage(c, qfsds, qfsdb)
	case "keycount":
		err = handleKeyCount(c, qfsds, qfsdb)
	case "keydiffcount":
		err = handleKeyDiffCount(c, qfsds, qfsdb)
	case "ttl":
		err = handleTTL(c, qfsds, cqlds, qfsdb)
	case "setttl":
		err = handleSetTTL(c, qfsds, cqlds, qfsdb)
	case "list":
		err = printList(c, qfsds, cqlds, qfsdb)
	default:
		fmt.Println("Unsupported walk sub-command: ", walkFlags.Arg(0))
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	walkTime := time.Since(start)
	if err != nil {
		fmt.Printf("Walk failed after %v with error: %v\n", walkTime, err.Error())
		os.Exit(exitBadCmd)
	}

	fmt.Printf("Walk completed successfully after %v\n", walkTime)
}

func handleDiskUsage(c *quantumfs.Ctx, qfsds quantumfs.DataStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("du subcommand takes 2 args: wsname path")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	wsname := walkFlags.Arg(1)
	searchPath := walkFlags.Arg(2)
	searchPath = filepath.Clean("/" + searchPath)

	// Get RootID
	var err error
	var rootID quantumfs.ObjectKey
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}

	var totalSize uint64
	sizer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey, size uint64) error {
		if !strings.HasPrefix(path, searchPath) {
			return nil
		}
		totalSize += size
		return nil
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, sizer); err != nil {
		return err
	}

	fmt.Println("Total Size = ", humanizeBytes(totalSize))
	return nil
}

func handleKeyCount(c *quantumfs.Ctx, qfsds quantumfs.DataStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 2 {
		fmt.Println("keycount subcommand takes 1 args: wsname ")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)

	var totalKeys, totalSize, uniqKeys, uniqSize uint64
	var mapLock utils.DeferableMutex
	keysRecorded := make(map[string]bool)
	sizer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey, size uint64) error {

		defer mapLock.Lock().Unlock()
		if _, exists := keysRecorded[key.String()]; !exists {
			keysRecorded[key.String()] = true
			uniqKeys++
			uniqSize += size
		}
		totalSize += size
		totalKeys++
		return nil
	}

	// Get RootID
	var err error
	var rootID quantumfs.ObjectKey
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, sizer); err != nil {
		return err
	}

	fmt.Println("Unique Keys = ", uniqKeys)
	fmt.Println("Unique Size = ", humanizeBytes(uniqSize))
	fmt.Println("Total Keys = ", totalKeys)
	fmt.Println("Total Size = ", humanizeBytes(totalSize))
	return nil
}

// TODO(sid) combine this with handleKeyCount
func handleKeyDiffCount(c *quantumfs.Ctx, qfsds quantumfs.DataStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("keydiffcount subcommand takes 2 args: wsname1 wsname2 ")
		fmt.Println()
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	// Get RootIDs
	wsname1 := walkFlags.Arg(1)
	wsname2 := walkFlags.Arg(2)
	var rootID1, rootID2 quantumfs.ObjectKey
	var err error
	if rootID1, err = getWorkspaceRootID(c, qfsdb, wsname1); err != nil {
		return err
	}

	if rootID2, err = getWorkspaceRootID(c, qfsdb, wsname2); err != nil {
		return err
	}

	keys := make(map[string]uint64)
	var mapLock utils.DeferableMutex
	keyRecorder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey, size uint64) error {
		defer mapLock.Lock().Unlock()
		keys[key.String()] = size
		return nil
	}

	if err = walker.Walk(c, qfsds, rootID1, keyRecorder); err != nil {
		return err
	}
	keysRootID1 := keys
	keys = make(map[string]uint64)

	// Walk
	if err = walker.Walk(c, qfsds, rootID2, keyRecorder); err != nil {
		return err
	}
	keysRootID2 := keys

	fmt.Printf("UniqueKeys\t\tUniqueSize\n")
	fmt.Printf("==========\t\t==========\n")
	diffKey, diffSize := mapCompare(keysRootID1, keysRootID2)
	fmt.Printf("%v\t\t%v in %v\n", diffKey, humanizeBytes(diffSize), wsname1)
	diffKey, diffSize = mapCompare(keysRootID2, keysRootID1)
	fmt.Printf("%v\t\t%v in %v\n", diffKey, humanizeBytes(diffSize), wsname2)
	return nil
}

// Given 2 maps m1 and m2, return the (numKeys, numSize) unique to m1 wrt m2
func mapCompare(m1 map[string]uint64, m2 map[string]uint64) (uint64, uint64) {

	var uniqueKey, uniqueSize uint64
	for k, size := range m1 {
		if _, seen := m2[k]; !seen {
			uniqueKey++
			uniqueSize += size
		}
	}
	return uniqueKey, uniqueSize
}

func handleTTL(c *quantumfs.Ctx, qfsds quantumfs.DataStore,
	cqlds blobstore.BlobStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 2 {
		fmt.Println("ttl subcommand takes 1 arg: wsname")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	// Get RootID
	wsname := walkFlags.Arg(1)
	var err error
	var rootID quantumfs.ObjectKey
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}
	// Internal Walker for TTL.
	var ttlWalker = func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64) error {

		if key.Type() == quantumfs.KeyTypeConstant ||
			key.Type() == quantumfs.KeyTypeEmbedded {
			return nil
		}

		ks := key.String()
		metadata, err := cqlds.Metadata(ks)
		if err != nil {
			return err
		}

		err = refreshTTL(cqlds, ks, metadata)
		if err != nil {
			return err
		}

		//  TODO if isDir() and TTL is good, return walker.SkipDir
		return nil
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, ttlWalker); err != nil {
		fmt.Println("Walk failed for ttl with rootID: ", rootID)
		return err
	}
	return nil
}

func handleSetTTL(c *quantumfs.Ctx, qfsds quantumfs.DataStore,
	cqlds blobstore.BlobStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 5 {
		fmt.Println("setttl subcommand takes 4 arg: wsname path " +
			" <threshold TTL(hrs)> <new TTL(hrs)>")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	searchPath := walkFlags.Arg(2)
	searchPath = filepath.Clean("/" + searchPath)

	var err error
	var setTTL int64
	if setTTL, err = strconv.ParseInt(walkFlags.Arg(3), 10, 64); err != nil {
		fmt.Println("TTL val is not a valid integer")
		fmt.Println("setttl subcommand takes 2 args: wsname path")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	// Get RootID
	var rootID quantumfs.ObjectKey
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}

	// Internal Walker for TTL.
	var ttlWalker = func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64) error {

		if !strings.HasPrefix(path, searchPath) {
			return nil
		}

		if key.Type() == quantumfs.KeyTypeConstant ||
			key.Type() == quantumfs.KeyTypeEmbedded {
			return nil
		}

		ks := key.String()
		metadata, err := cqlds.Metadata(ks)
		if err != nil {
			return err
		}

		err = refreshTTL(cqlds, ks, metadata)
		if err != nil {
			return err
		}

		//  TODO if isDir() and TTL is good, return walker.SkipDir
		return nil
	}
	// Walk with these new thresholds
	refreshTTLValueSecs = setTTL
	refreshTTLTimeSecs = setTTL
	if err = walker.Walk(c, qfsds, rootID, ttlWalker); err != nil {
		return err
	}
	return nil
}

func printList(c *quantumfs.Ctx, qfsds quantumfs.DataStore,
	cqlds blobstore.BlobStore, wsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 1 {
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	tsl, err := wsdb.TypespaceList(c)
	if err != nil {
		fmt.Println("Error in getting list of Typespaces")
		return err
	}
	for _, ts := range tsl {
		// Assuming we do not have _/X/Y
		if ts == quantumfs.NullSpaceName {
			continue
		}
		nsl, err := wsdb.NamespaceList(c, ts)
		if err != nil {
			fmt.Printf("Error in getting list of Namespaces for TS:%s\n", ts)
			continue
		}
		for _, ns := range nsl {
			wsl, err := wsdb.WorkspaceList(c, ts, ns)
			if err != nil {
				fmt.Println("Error in getting list of Workspaces "+
					"for TS:%s NS:%s", ts, ns)
				continue
			}
			for _, ws := range wsl {
				fmt.Printf("%s/%s/%s\n", ts, ns, ws)
			}
		}
	}
	return nil
}
