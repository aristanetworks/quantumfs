// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
	walkutils "github.com/aristanetworks/qubit/tools/qwalker/utils"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
	"github.com/aristanetworks/qubit/tools/utils/cmdproc"
)

func init() {
	registerTTLCmd()
	registerForceTTLCmd()
	registerTTLHistogramCmd()
}

func registerTTLCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "ttl"
	cmd.Usage = "workspace"
	cmd.Short = "update all blocks in workspace with TTL values from configuration"
	cmd.Details = `
workspace
	name of the workspace
`
	cmd.Run = handleTTL

	cmdproc.RegisterCommand(cmd)
}

func handleTTL(args []string) error {
	if len(args) != 1 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}

	wsname := args[0]

	walkFunc := func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		return walkutils.RefreshTTL(c, path, key, size, isDir,
			cs.cqlds, cs.ttlCfg.TTLNew,
			cs.ttlCfg.SkipMapResetAfter_ms/1000,
			nil, nil)
	}

	showRootIDStatus := true
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname,
		co.progress, showRootIDStatus, walkFunc); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	return nil
}

func registerForceTTLCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "forceTTL"
	cmd.Usage = "workspace new_ttl_hours"
	cmd.Short = "update all blocks in workspace with lower TTL to given TTL"
	cmd.Details = `
workspace
	name of the workspace
new_ttl_hours
	TTL of the block is set to this value if its lower than this value
`
	cmd.Run = handleForceTTL

	cmdproc.RegisterCommand(cmd)
}

func handleForceTTL(args []string) error {
	if len(args) != 2 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}
	wsname := args[0]

	var err error
	var newTTL int64
	if newTTL, err = strconv.ParseInt(args[1], 10, 64); err != nil {
		return cmdproc.NewBadArgExitErr("TTL value is not a valid integer")
	}
	newTTL = newTTL * 3600 // Hours to seconds

	// Internal Walker for TTL.
	walkFunc := func(c *walker.Ctx, path string,
		key quantumfs.ObjectKey, size uint64, isDir bool) error {

		return walkutils.RefreshTTL(c, path, key, size, isDir,
			cs.cqlds, newTTL, newTTL, nil, nil)
	}

	showRootIDStatus := true
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, walkFunc); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	return nil
}

func registerTTLHistogramCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "ttlHistogram"
	cmd.Usage = "workspace"
	cmd.Short = "show a histogram of TTL values of blocks within a workspace"
	cmd.Details = `
workspace
	name of the workspace
`
	cmd.Run = handleTTLHistogram

	cmdproc.RegisterCommand(cmd)
}

func handleTTLHistogram(args []string) error {
	if len(args) != 1 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}
	wsname := args[0]

	keymap := make(map[string]bool)
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
			if _, ok := keymap[key.String()]; ok {
				return true
			}
			keymap[key.String()] = true
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
		hist.Increment(fmt.Sprintf("%d", bucket))
		return nil
	}

	showRootIDStatus := true
	if err := walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname,
		co.progress, showRootIDStatus, bucketer); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	fmt.Printf("Days(s)   %5s\n", "Count")
	hist.Print()
	return nil
}
