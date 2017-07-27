// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"

	"github.com/aristanetworks/qubit/tools/qwalker/cmd/cmdproc"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func init() {
	registerKeyCountCmd()
	registerKeyDiffCountCmd()
}

func registerKeyCountCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "keycount"
	cmd.Usage = "workspace [dedupe]"
	cmd.Short = "shows total and unique keys and size within a workspace"
	cmd.Details = `
workspace
	name of the workspace
dedupe
	prints dedupe information
`
	cmd.Run = handleKeyCount

	cmdproc.RegisterCommand(cmd)
}

func handleKeyCount(args []string) error {
	if len(args) != 1 && len(args) != 2 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}
	wsname := args[0]
	showDedupeInfo := false
	if len(args) == 2 && args[1] == "dedupe" {
		showDedupeInfo = true
	}

	tracker, keyCounter := getTrackerHandler(showDedupeInfo, nil)
	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, keyCounter); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	fmt.Println("Unique Keys = ", tracker.uniqueKeys())
	fmt.Println("Unique Size = ", qubitutils.HumanizeBytes(tracker.uniqueSize()))
	fmt.Println("Total Keys = ", tracker.totalKeys())
	fmt.Println("Total Size = ", qubitutils.HumanizeBytes(tracker.totalSize()))
	tracker.printDedupeReport()
	return nil
}

func registerKeyDiffCountCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "keydiffcount"
	cmd.Usage = "workspace1 workspace2 [keys]"
	cmd.Short = "compares two workspaces in terms of keycounts and optionally shows unique keys"
	cmd.Details = `
workspace1 workspace2
	name of the workspaces
keys
	prints unique key information
`
	cmd.Run = handleKeyDiffCount

	cmdproc.RegisterCommand(cmd)
}

func handleKeyDiffCount(args []string) error {
	if len(args) != 2 && len(args) != 3 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}

	// Get RootIDs
	wsname1 := args[0]
	wsname2 := args[1]
	showKeys := false
	if len(args) == 3 && args[2] == "keys" {
		showKeys = true
	}

	tracker1, keyCounter1 := getTrackerHandler(showKeys, nil)
	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname1, co.progress,
		showRootIDStatus, keyCounter1); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	tracker2, keyCounter2 := getTrackerHandler(showKeys, nil)
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname2,
		co.progress, showRootIDStatus, keyCounter2); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
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
