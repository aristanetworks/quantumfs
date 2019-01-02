// +build ether

// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"path/filepath"
	"strings"

	qubitutils "github.com/aristanetworks/quantumfs/cmd/qutils"
	"github.com/aristanetworks/quantumfs/cmd/qutils/cmdproc"
)

func init() {
	registerDuCmd()
}

func registerDuCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "du"
	cmd.Usage = "workspace path"
	cmd.Short = "shows the disk usage of path in a workspace"
	cmd.Details = `
workspace
	name of the workspace
path
	absolute path from root of the workspace
`
	cmd.Run = handleDiskUsage

	cmdproc.RegisterCommand(cmd)
}

func handleDiskUsage(args []string) error {
	if len(args) != 2 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}

	wsname := args[0]
	searchPath := args[1]
	searchPath = filepath.Clean("/" + searchPath)

	filter := func(path string) bool {
		return !strings.HasPrefix(path, searchPath)
	}

	tracker, sizer := getTrackerHandler(filter)
	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, sizer); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	fmt.Println("Total Size = ", qubitutils.HumanizeBytes(tracker.totalSize()))
	return nil
}
