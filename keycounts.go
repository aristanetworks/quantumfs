// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"

	"github.com/aristanetworks/quantumfs"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func handleKeyCount(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	if walkFlags.NArg() < 2 || walkFlags.NArg() > 3 {
		fmt.Println("keycount sub-command args: wsname [dedupe]")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)
	showDedupeInfo := false
	if walkFlags.Arg(2) == "dedupe" {
		showDedupeInfo = true
	}

	tracker, keyCounter := getTrackerHandler(showDedupeInfo, nil)
	showRootIDStatus := false
	if err := walkHelper(c, qfsds, qfsdb, wsname, progress, showRootIDStatus,
		keyCounter); err != nil {
		return err
	}
	fmt.Println("Unique Keys = ", tracker.uniqueKeys())
	fmt.Println("Unique Size = ", qubitutils.HumanizeBytes(tracker.uniqueSize()))
	fmt.Println("Total Keys = ", tracker.totalKeys())
	fmt.Println("Total Size = ", qubitutils.HumanizeBytes(tracker.totalSize()))
	tracker.printDedupeReport()
	return nil
}

func handleKeyDiffCount(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	if walkFlags.NArg() < 3 || walkFlags.NArg() > 4 {
		fmt.Println("keydiffcount sub-command args: wsname1 wsname2 [keys]")
		fmt.Println()
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	// Get RootIDs
	wsname1 := walkFlags.Arg(1)
	wsname2 := walkFlags.Arg(2)
	showKeys := false
	if walkFlags.Arg(3) == "keys" {
		showKeys = true
	}

	tracker1, keyCounter1 := getTrackerHandler(showKeys, nil)
	showRootIDStatus := false
	if err := walkHelper(c, qfsds, qfsdb, wsname1, progress, showRootIDStatus,
		keyCounter1); err != nil {
		return err
	}
	tracker2, keyCounter2 := getTrackerHandler(showKeys, nil)
	if err := walkHelper(c, qfsds, qfsdb, wsname2, progress, showRootIDStatus,
		keyCounter2); err != nil {
		return err
	}
	fmt.Printf("UniqueKeys\t\tUniqueSize\n")
	fmt.Printf("==========\t\t==========\n")
	diffKeys, diffSize := tracker1.trackerKeyDiff(tracker2)
	fmt.Printf("%v\t\t%v in %v\n",
		len(diffKeys), qubitutils.HumanizeBytes(diffSize), wsname1)
	tracker1.printKeyPathInfo(diffKeys)

	diffKeys, diffSize = tracker2.trackerKeyDiff(tracker1)
	fmt.Printf("%v\t\t%v in %v\n",
		len(diffKeys), qubitutils.HumanizeBytes(diffSize), wsname2)
	tracker2.printKeyPathInfo(diffKeys)
	return nil
}
