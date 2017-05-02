// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"encoding/hex"
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
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
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

	fmt.Printf("Finished: %v\n", walkTime)
}

func handleDiskUsage(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("du sub-command takes 2 args: wsname path")
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
	var totalKeys uint64
	tracker := newTracker(false)
	var mapLock utils.DeferableMutex
	sizer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		defer mapLock.Lock().Unlock()
		if !strings.HasPrefix(path, searchPath) {
			return nil
		}
		tracker.addKey(hex.EncodeToString(key.Value()),
			path, size)
		return nil
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, sizer); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("Total Size = ", humanizeBytes(tracker.totalSize()))
	return nil
}

func handleKeyCount(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() < 2 || walkFlags.NArg() > 3 {
		fmt.Println("keycount sub-command args: wsname [dedupe]")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	start := time.Now()
	showDedupeInfo := false
	if walkFlags.Arg(2) == "dedupe" {
		showDedupeInfo = true
	}

	var mapLock utils.DeferableMutex
	var totalKeys uint64
	tracker := newTracker(showDedupeInfo)
	sizer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		defer mapLock.Lock().Unlock()
		tracker.addKey(hex.EncodeToString(key.Value()), path, size)
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
	fmt.Println("Unique Keys = ", tracker.uniqueKeys())
	fmt.Println("Unique Size = ", humanizeBytes(tracker.uniqueSize()))
	fmt.Println("Total Keys = ", tracker.totalKeys())
	fmt.Println("Total Size = ", humanizeBytes(tracker.totalSize()))
	tracker.printDedupeReport()
	return nil
}

// TODO(sid) combine this with handleKeyCount
func handleKeyDiffCount(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() < 3 || walkFlags.NArg() > 4 {
		fmt.Println("keydiffcount sub-command args: wsname1 wsname2 [keys]")
		fmt.Println()
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	// Get RootIDs
	wsname1 := walkFlags.Arg(1)
	wsname2 := walkFlags.Arg(2)
	start := time.Now()
	showKeys := false
	if walkFlags.Arg(3) == "keys" {
		showKeys = true
	}
	var rootID1, rootID2 quantumfs.ObjectKey
	var totalKeys uint64
	var err error
	if rootID1, err = getWorkspaceRootID(c, qfsdb, wsname1); err != nil {
		return err
	}

	tracker := newTracker(showKeys)
	var mapLock utils.DeferableMutex
	keyRecorder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		defer mapLock.Lock().Unlock()
		tracker.addKey(hex.EncodeToString(key.Value()), path, size)
		return nil
	}
	if err = walker.Walk(c, qfsds, rootID1, keyRecorder); err != nil {
		return err
	}
	tracker1 := tracker

	if rootID2, err = getWorkspaceRootID(c, qfsdb, wsname2); err != nil {
		return err
	}

	tracker = newTracker(showKeys)
	// Walk
	if err = walker.Walk(c, qfsds, rootID2, keyRecorder); err != nil {
		return err
	}
	tracker2 := tracker

	fmt.Println()
	fmt.Printf("UniqueKeys\t\tUniqueSize\n")
	fmt.Printf("==========\t\t==========\n")
	diffKeys, diffSize := tracker1.trackerKeyDiff(tracker2)
	fmt.Printf("%v\t\t%v in %v\n",
		len(diffKeys), humanizeBytes(diffSize), wsname1)
	tracker1.printKeyPathInfo(diffKeys)

	diffKeys, diffSize = tracker2.trackerKeyDiff(tracker1)
	fmt.Printf("%v\t\t%v in %v\n",
		len(diffKeys), humanizeBytes(diffSize), wsname2)
	tracker2.printKeyPathInfo(diffKeys)
	return nil
}

func handleTTL(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, cqlds blobstore.BlobStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 2 {
		fmt.Println("ttl sub-command takes 1 arg: wsname")
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
			return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
		}

		err = refreshTTL(cqlds, ks, metadata)
		if err != nil {
			return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
		}

		//  TODO if isDir() and TTL is good, return walker.SkipDir
		return nil
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, ttlWalker); err != nil {
		return fmt.Errorf("rootID: %s err: %v", rootID.Text(), err)
	}
	return nil
}

func handleForceTTL(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, cqlds blobstore.BlobStore,
	qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("forceTTL sub-command takes 2 arg: wsname <new TTL(hrs)>")
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
			return fmt.Errorf("key %v: %v", key.Text(), err)
		}

		err = refreshTTL(cqlds, ks, metadata)
		if err != nil {
			return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
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
				fmt.Printf("Error in getting list of Workspaces "+
					"for TS:%s NS:%s", ts, ns)
				continue
			}
			for _, ws := range wsl {
				var rootID quantumfs.ObjectKey
				wsname := ts + "/" + ns + "/" + ws
				if rootID, err = getWorkspaceRootID(c, wsdb, wsname); err != nil {
					return fmt.Errorf("RootId not found for %v err: %v", wsname, err)
				}
				fmt.Printf("%v : %s\n", rootID.Text(), wsname)
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
		fmt.Println("ttlHistogram sub-command takes 1 args: wsname ")
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
			return fmt.Errorf("key %v: %v", key.Text(), err)
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

// path2key sub-command walks the entire workspace even after it
// has found the path. In essence this is like the "du" sub-command
// When we will fix du, we can visit this sub-command as well.
// It is inefficient, not wrong.
// One way to fix it would be to not pursure paths where we know
// there is no possibility of finding the searchPath.
func printPath2Key(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("path2key sub-command takes 2 args: wsname path")
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
	var listLock utils.DeferableMutex
	keyList := make([]quantumfs.ObjectKey, 0, 10)
	var totalKeys uint64
	finder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)
		if strings.Compare(path, searchPath) == 0 {
			defer listLock.Lock().Unlock()
			keyList = append(keyList, key)
		}
		return nil
	}

	// Walk
	if err = walker.Walk(c, qfsds, rootID, finder); err != nil {
		return err
	}

	fmt.Println()
	if len(keyList) == 0 {
		return fmt.Errorf("Key not found for path %v", searchPath)
	}

	fmt.Printf("Search path: %v\n", searchPath)
	for _, key := range keyList {
		fmt.Println(key)
	}
	return nil
}

const oneDaySecs = int64((24 * time.Hour) / time.Second)

func printConstantKeys(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, cqlds blobstore.BlobStore,
	wsdb quantumfs.WorkspaceDB) error {

	// Cleanup Args
	if walkFlags.NArg() != 3 {
		fmt.Println("findconstantkeys sub-command takes 2 args: wsname num_days")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	var numDays int64
	var err error
	if numDays, err = strconv.ParseInt(walkFlags.Arg(2), 10, 32); err != nil {
		fmt.Println("num_days is not a valid integer")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	// Get RootID
	var rootID quantumfs.ObjectKey
	if rootID, err = getWorkspaceRootID(c, wsdb, wsname); err != nil {
		return err
	}

	cds := quantumfs.ConstantStore
	start := time.Now()
	var mapLock utils.DeferableMutex
	matchKey := make(map[quantumfs.ObjectKey]struct {
		p string
		t int64
	})
	var totalKeys uint64
	finder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		atomic.AddUint64(&totalKeys, 1)
		defer showProgress(progress, start, totalKeys)

		// Print the key if:
		// - It is of type Constant,
		// - It is not present in the Constant DataStore,
		// - Its TTL value is more than numDays days.
		if key.Type() == quantumfs.KeyTypeConstant {
			buf := simplebuffer.New(nil, key)
			if err := cds.Get(nil, key, buf); err != nil {

				ks := key.String()
				metadata, err := cqlds.Metadata(ks)
				if err != nil {
					return fmt.Errorf("path: %v key %v: %v", path, key.Text(), err)
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

				if ttlVal > (numDays * oneDaySecs) {
					defer mapLock.Lock().Unlock()
					matchKey[key] = struct {
						p string
						t int64
					}{
						p: path,
						t: ttlVal / oneDaySecs,
					}
				}
			}
		}
		return nil
	}

	// Walk
	err = walker.Walk(c, qfsds, rootID, finder)

	// Print all matches that we have collected so far
	// even though we hit an error.
	for k, v := range matchKey {
		fmt.Println(k, ": ", v)
	}

	return err
}
