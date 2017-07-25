// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aristanetworks/quantumfs"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func handleDiskUsage(c *quantumfs.Ctx, progress bool,
	qfsds quantumfs.DataStore, qfsdb quantumfs.WorkspaceDB) error {

	if walkFlags.NArg() != 3 {
		fmt.Println("du sub-command takes 2 args: wsname path")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	wsname := walkFlags.Arg(1)
	searchPath := walkFlags.Arg(2)
	searchPath = filepath.Clean("/" + searchPath)

	filter := func(path string) bool { return !strings.HasPrefix(path, searchPath) }

	showDedupeInfo := false
	tracker, sizer := getTrackerHandler(showDedupeInfo, filter)
	showRootIDStatus := false
	if err := walkHelper(c, qfsds, qfsdb, wsname, progress, showRootIDStatus,
		sizer); err != nil {
		return err
	}
	fmt.Println("Total Size = ", qubitutils.HumanizeBytes(tracker.totalSize()))
	return nil
}
