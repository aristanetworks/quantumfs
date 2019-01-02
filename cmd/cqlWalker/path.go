// +build ether

// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/cmd/qutils/cmdproc"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
)

func init() {
	registerPathCmd()
}

// path2key sub-command walks the entire workspace even after it
// has found the path. In essence this is like the "du" sub-command
// When we will fix du, we can visit this sub-command as well.
// It is inefficient, not wrong.
// One way to fix it would be to not pursure paths where we know
// there is no possibility of finding the searchPath.
func registerPathCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "path2key"
	cmd.Usage = "workspace path"
	cmd.Short = "print key for path in workspace"
	cmd.Details = `
workspace
	name of the workspace
path
	absolute path from the root of the workspace
`
	cmd.Run = printPath2Key

	cmdproc.RegisterCommand(cmd)
}

func printPath2Key(args []string) error {
	if len(args) != 2 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}

	wsname := args[0]
	searchPath := args[1]
	searchPath = filepath.Clean("/" + searchPath)

	var listLock utils.DeferableMutex
	keyList := make([]quantumfs.ObjectKey, 0, 10)
	finder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, objType quantumfs.ObjectType, err error) error {

		if strings.Compare(path, searchPath) == 0 {
			defer listLock.Lock().Unlock()
			keyList = append(keyList, key)
		}
		return nil
	}

	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, finder); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	if len(keyList) == 0 {
		fmt.Println("Key not found for path ", searchPath)
	}

	fmt.Printf("Search path: %s\n", searchPath)
	for _, key := range keyList {
		fmt.Println(key.String())
	}
	return nil
}
