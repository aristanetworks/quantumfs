// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
)

// path2key sub-command walks the entire workspace even after it
// has found the path. In essence this is like the "du" sub-command
// When we will fix du, we can visit this sub-command as well.
// It is inefficient, not wrong.
// One way to fix it would be to not pursure paths where we know
// there is no possibility of finding the searchPath.
func printPath2Key() error {
	if walkFlags.NArg() != 3 {
		fmt.Println("path2key sub-command takes 2 args: wsname path")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	wsname := walkFlags.Arg(1)
	searchPath := walkFlags.Arg(2)
	searchPath = filepath.Clean("/" + searchPath)

	var listLock utils.DeferableMutex
	keyList := make([]quantumfs.ObjectKey, 0, 10)
	finder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		if strings.Compare(path, searchPath) == 0 {
			defer listLock.Lock().Unlock()
			keyList = append(keyList, key)
		}
		return nil
	}

	showRootIDStatus := false
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, finder); err != nil {
		return err
	}
	if len(keyList) == 0 {
		return fmt.Errorf("Key not found for path %v", searchPath)
	}

	fmt.Printf("Search path: %v\n", searchPath)
	for _, key := range keyList {
		fmt.Println(key.String())
	}
	return nil
}
