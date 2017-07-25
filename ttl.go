// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
	walkutils "github.com/aristanetworks/qubit/tools/qwalker/utils"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func handleTTL() error {
	if walkFlags.NArg() != 2 {
		fmt.Println("ttl sub-command takes 1 arg: wsname")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	wsname := walkFlags.Arg(1)

	walkFunc := func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		return walkutils.RefreshTTL(c, path, key, size, isDir,
			cs.cqlds, cs.ttlCfg.TTLThreshold, cs.ttlCfg.TTLNew)
	}

	showRootIDStatus := true
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname,
		co.progress, showRootIDStatus, walkFunc); err != nil {
		return err
	}
	return nil
}

func handleForceTTL() error {
	if walkFlags.NArg() != 3 {
		fmt.Println("forceTTL sub-command takes 2 arg: wsname <new TTL(hrs)>")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)

	var err error
	var newTTL int64
	if newTTL, err = strconv.ParseInt(walkFlags.Arg(2), 10, 64); err != nil {
		fmt.Println("TTL val is not a valid integer")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	newTTL = newTTL * 3600 // Hours to seconds

	// Internal Walker for TTL.
	walkFunc := func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		return walkutils.RefreshTTL(c, path, key, size, isDir,
			cs.cqlds, newTTL, newTTL)
	}

	showRootIDStatus := true
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, walkFunc); err != nil {
		return err
	}
	return nil
}

func printTTLHistogram() error {
	if walkFlags.NArg() != 2 {
		fmt.Println("ttlHistogram sub-command takes 1 args: wsname ")
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}
	wsname := walkFlags.Arg(1)

	keymap := make(map[quantumfs.ObjectKey]bool)
	var maplock utils.DeferableMutex
	hist := qubitutils.NewHistogram()
	bucketer := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		if walker.SkipKey(c, key) {
			return nil
		}

		// So that the lock is not held during cql ops.
		present := func() bool {
			defer maplock.Lock().Unlock()
			if _, ok := keymap[key]; ok {
				return true
			}
			keymap[key] = true
			return false
		}()
		if present {
			return nil
		}

		metadata, err := cs.cqlds.Metadata(walkutils.ToECtx(c), key.Value())
		if err != nil {
			return fmt.Errorf("path:%v key %v: %v", path, key.String(), err)
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

	showRootIDStatus := true
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname,
		co.progress, showRootIDStatus, bucketer); err != nil {
		return err
	}
	fmt.Printf("Days(s)   %5s\n", "Count")
	hist.Print()
	return nil
}
