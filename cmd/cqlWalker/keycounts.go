// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"

	qubitutils "github.com/aristanetworks/quantumfs/utils/qutils"
)

func init() {
	registerKeyCountCmd()
	registerKeyDiffCountCmd()
}

func registerKeyCountCmd() {
	var cmd CommandInfo
	cmd.Name = "keycount"
	cmd.Usage = "workspace [dedupe | sizehist]"
	cmd.Short = "shows keycount information along with dedupe or size " +
		"histogram for a workspace"
	cmd.Details = `
workspace
	name of the workspace
dedupe
	prints dedupe information
sizehist
	prints a histogram of block sizes
`
	cmd.Run = handleKeyCount

	RegisterCommand(cmd)
}

func handleKeyCount(args []string) error {
	wsopts := map[string]func(t *tracker){
		"dedupe":   func(t *tracker) { t.printDedupeReport() },
		"sizehist": func(t *tracker) { t.printSizeHistogram() },
	}

	var wsname, wsoption string
	switch len(args) {
	case 1:
		wsname = args[0]
	case 2:
		wsname = args[0]
		wsoption = args[1]
	default:
		return NewBadArgExitErr("incorrect arguments")
	}
	if wsoption != "" {
		if _, exist := wsopts[wsoption]; !exist {
			return NewBadArgExitErr("unsupported argument")
		}
	}

	tracker, keyCounter := getTrackerHandler(nil)
	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, keyCounter); err != nil {
		return NewBadCmdExitErr("%s", err)
	}
	fmt.Println("Unique Keys = ", tracker.uniqueKeys())
	fmt.Println("Unique Size = ", qubitutils.HumanizeBytes(tracker.uniqueSize()))
	fmt.Println("Total Keys = ", tracker.totalKeys())
	fmt.Println("Total Size = ", qubitutils.HumanizeBytes(tracker.totalSize()))
	if wsoption != "" {
		wsopts[wsoption](tracker)
	}
	return nil
}

func registerKeyDiffCountCmd() {
	var cmd CommandInfo
	cmd.Name = "keydiffcount"
	cmd.Usage = "workspace1 workspace2 [keys]"
	cmd.Short = "compares two workspaces in terms of keycounts and " +
		"optionally shows unique keys"
	cmd.Details = `
workspace1 workspace2
	name of the workspaces
keys
	prints unique key information
`
	cmd.Run = handleKeyDiffCount

	RegisterCommand(cmd)
}

func handleKeyDiffCount(args []string) error {

	wsopts := map[string]func(t *tracker, d []string){
		"keys": func(t *tracker, d []string) { t.printKeyPathInfo(d) },
	}

	var wsname1, wsname2, wsoption string
	switch len(args) {
	case 1:
		wsname1 = args[0]
	case 2:
		wsname1 = args[0]
		wsname2 = args[1]
	case 3:
		wsname1 = args[0]
		wsname2 = args[1]
		wsoption = args[2]
	default:
		return NewBadArgExitErr("incorrect arguments")
	}

	if wsoption != "" {
		if _, exist := wsopts[wsoption]; !exist {
			return NewBadArgExitErr("unsupported argument")
		}
	}

	tracker1, keyCounter1 := getTrackerHandler(nil)
	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname1, co.progress,
		showRootIDStatus, keyCounter1); err != nil {
		return NewBadCmdExitErr("%s", err)
	}
	tracker2, keyCounter2 := getTrackerHandler(nil)
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname2,
		co.progress, showRootIDStatus, keyCounter2); err != nil {
		return NewBadCmdExitErr("%s", err)
	}
	fmt.Printf("UniqueKeys\t\tUniqueSize\n")
	fmt.Printf("==========\t\t==========\n")
	diffKeys, diffSize := tracker1.trackerKeyDiff(tracker2)
	fmt.Printf("%v\t\t%v in %v\n",
		len(diffKeys), qubitutils.HumanizeBytes(diffSize), wsname1)

	if wsoption != "" {
		wsopts[wsoption](tracker1, diffKeys)
	}

	diffKeys, diffSize = tracker2.trackerKeyDiff(tracker1)
	fmt.Printf("%v\t\t%v in %v\n",
		len(diffKeys), qubitutils.HumanizeBytes(diffSize), wsname2)

	if wsoption != "" {
		wsopts[wsoption](tracker2, diffKeys)
	}

	return nil
}
