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
	"sync/atomic"
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
	progress := walkFlags.Bool("progress", false, "show progress")

	walkFlags.Usage = func() {
		fmt.Println("This tool walks all the keys within a workspace" +
			"and executes a subcommand on selected objects")

		fmt.Println("Available commands:")
		fmt.Println("usage: walker <-cfg config> [-progress] <command> ARG1[,ARG2[,...]]")

		fmt.Println("  keycount <workspace>")
		fmt.Println("           - count the number of keys in given workspace.")
		fmt.Println("  keydiffcount <workspace1> <workspace2>")
		fmt.Println("           - count the diff in number of keys in between")
		fmt.Println("	          the given workspaces.")
		fmt.Println("  du <workspace>  <path>")
		fmt.Println("           - calculate the size on disk for the given workspace.")
		fmt.Println("             path is from the root of workspace.")
		fmt.Println("  ttl <workspace>")
		fmt.Println("           - update TTL of all the blocks in the workspace as")
		fmt.Println("             per the TTL values in the config file.")
		fmt.Println("  forceTTL <workspace> <new ttl(hrs)>")
		fmt.Println("           - update TTL of all the blocks in the workspace")
		fmt.Println("             in the given path to the given TTL value.")
		fmt.Println("             TTL is updated only if it is less than new TTL value.")
		fmt.Println("             path is from the root of workspace.")
		fmt.Println("  list")
		fmt.Println("           - list all workspaces")
		fmt.Println("  ttlHistogram <workspace>")
		fmt.Println("           - bucket all the blocks in the workspace")
		fmt.Println("             into different TTL values.")
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
		err = handleTTL(c, *progress, qfsds, cqlds, qfsdb)
	case "forceTTL":
		err = handleForceTTL(c, *progress, qfsds, cqlds, qfsdb)
	case "list":
		err = printList(c, *progress, qfsds, cqlds, qfsdb)
	case "ttlHistogram":
		err = printTTLHistogram(c, *progress, qfsds, cqlds, qfsdb)
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

	fmt.Printf("Finished: %v\n", walkTime)
}

func handleDiskUsage(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

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

	start := time.Now()
	var totalSize, totalKeys uint64
	sizer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
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

	fmt.Println()
	fmt.Println("Total Size = ", humanizeBytes(totalSize))
	return nil
}

func handleKeyCount(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 2 {
		fmt.Println("keycount subcommand takes 1 args: wsname ")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	start := time.Now()

	var totalKeys, totalSize, uniqKeys, uniqSize uint64
	var mapLock utils.DeferableMutex
	keysRecorded := make(map[string]bool)
	sizer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		defer showProgress(progress, start, totalKeys)
		defer mapLock.Lock().Unlock()
		if _, exists := keysRecorded[key.String()]; !exists {
			keysRecorded[key.String()] = true
			uniqKeys++
			uniqSize += size
		}
		totalSize += size
		atomic.AddUint64(&totalKeys, 1)
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

	fmt.Println()
	fmt.Println("Unique Keys = ", uniqKeys)
	fmt.Println("Unique Size = ", humanizeBytes(uniqSize))
	fmt.Println("Total Keys = ", totalKeys)
	fmt.Println("Total Size = ", humanizeBytes(totalSize))
	return nil
}

// TODO(sid) combine this with handleKeyCount
func handleKeyDiffCount(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

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
	start := time.Now()
	var rootID1, rootID2 quantumfs.ObjectKey
	var totalKeys uint64
	var err error
	if rootID1, err = getWorkspaceRootID(c, qfsdb, wsname1); err != nil {
		return err
	}

	if rootID2, err = getWorkspaceRootID(c, qfsdb, wsname2); err != nil {
		return err
	}

	keys := make(map[string]uint64)
	var mapLock utils.DeferableMutex
	keyRecorder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
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

	fmt.Println()
	fmt.Printf("UniqueKeys\t\tUniqueSize\n")
	fmt.Printf("==========\t\t==========\n")
	diffKey, diffSize := mapCompare(keysRootID1, keysRootID2)
	fmt.Printf("%v\t\t%v in %v\n", diffKey, humanizeBytes(diffSize), wsname1)
	diffKey, diffSize = mapCompare(keysRootID2, keysRootID1)
	fmt.Printf("%v\t\t%v in %v\n", diffKey, humanizeBytes(diffSize), wsname2)
	return nil
}

func handleTTL(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, cqlds blobstore.BlobStore,
	qfsdb quantumfs.WorkspaceDB) error {

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
	var totalKeys uint64
	start := time.Now()
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}
	// Internal Walker for TTL.
	var ttlWalker = func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		if walker.SkipKey(c, key) {
			return nil
		}

		ks := key.String()
		metadata, err := cqlds.Metadata(ks)
		if err != nil {
			return fmt.Errorf("path: %v key %v: %v", path, ks, err)
		}

		err = refreshTTL(cqlds, ks, metadata)
		if err != nil {
			return fmt.Errorf("path: %v key %v: %v", path, ks, err)
		}

		//  TODO if isDir() and TTL is good, return walker.SkipDir
		return nil
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, ttlWalker); err != nil {
		return fmt.Errorf("rootID: %s err: %v", rootID, err)
	}
	return nil
}

func handleForceTTL(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, cqlds blobstore.BlobStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("forceTTL subcommand takes 2 arg: wsname <new TTL(hrs)>")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	start := time.Now()

	var err error
	var newTTL int64
	if newTTL, err = strconv.ParseInt(walkFlags.Arg(2), 10, 64); err != nil {
		fmt.Println("TTL val is not a valid integer")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	newTTL = newTTL * 3600 // Hours to seconds

	// Walk with these new thresholds
	refreshTTLTimeSecs = newTTL  // new val
	refreshTTLValueSecs = newTTL // threshold

	// Get RootID
	var rootID quantumfs.ObjectKey
	var totalKeys uint64
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}

	// Internal Walker for TTL.
	var ttlWalker = func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		if walker.SkipKey(c, key) {
			return nil
		}

		ks := key.String()
		metadata, err := cqlds.Metadata(ks)
		if err != nil {
			return fmt.Errorf("key %v: %v", ks, err)
		}

		err = refreshTTL(cqlds, ks, metadata)
		if err != nil {
			return fmt.Errorf("path: %v key %v: %v", path, ks, err)
		}

		//  TODO if isDir() and TTL is good, return walker.SkipDir
		return nil
	}
	if err = walker.Walk(c, qfsds, rootID, ttlWalker); err != nil {
		return err
	}
	return nil
}

func printList(c *quantumfs.Ctx, progress bool, qfsds quantumfs.DataStore,
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

func printTTLHistogram(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, cqlds blobstore.BlobStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 2 {
		fmt.Println("ttlHistogram subcommand takes 1 args: wsname ")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	start := time.Now()

	var totalKeys uint64
	hist := newHistogram()
	bucketer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		if walker.SkipKey(c, key) {
			return nil
		}

		ks := key.String()
		metadata, err := cqlds.Metadata(ks)
		if err != nil {
			return fmt.Errorf("key %v: %v", ks, err)
		}
		ttl, ok := metadata[cql.TimeToLive]
		if !ok {
			return fmt.Errorf("Store must return metadata with " +
				"TimeToLive")
		}
		ttlVal, err := strconv.ParseInt(ttl, 10, 64)
		if err != nil {
			return fmt.Errorf("Invalid TTL value in metadata %s ",
				ttl)
		}

		oneDaySecs := int64((24 * time.Hour) / time.Second)
		bucket := ttlVal / oneDaySecs
		hist.Increment(bucket)
		return nil
	}

	// Get RootID
	var err error
	var rootID quantumfs.ObjectKey
	if rootID, err = getWorkspaceRootID(c, qfsdb, wsname); err != nil {
		return err
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, bucketer); err != nil {
		return err
	}

	hist.Print()
	return nil
}
