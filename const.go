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
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
	"github.com/aristanetworks/quantumfs/walker"
	"github.com/aristanetworks/qubit/tools/qwalker/cmd/cmdproc"
	walkutils "github.com/aristanetworks/qubit/tools/qwalker/utils"
)

func init() {
	registerConstCmd()
}

func registerConstCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "findconstantkeys"
	cmd.Usage = "workspace num_days"
	cmd.Short = "list constant keys with TTL greater than num_days along with their paths"
	cmd.Details = `
workspace
	name of the workspace
num_days
	select keys with TTL greater than num_days
`
	cmd.Run = printConstantKeys

	cmdproc.RegisterCommand(cmd)
}

const oneDaySecs = int64((24 * time.Hour) / time.Second)

func printConstantKeys(args []string) error {
	if len(args) != 2 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}
	wsname := args[0]
	var numDays int64
	var err error
	if numDays, err = strconv.ParseInt(args[1], 10, 32); err != nil {
		return cmdproc.NewBadArgExitErr("num_days is not a valid integer")
	}

	cds := quantumfs.ConstantStore
	var mapLock utils.DeferableMutex
	matchKey := make(map[quantumfs.ObjectKey]struct {
		p string
		t int64
	})
	finder := func(c *walker.Ctx, path string, key quantumfs.ObjectKey,
		size uint64, isDir bool) error {

		// Print the key if:
		// - It is of type Constant,
		// - It is not present in the Constant DataStore,
		// - Its TTL value is more than numDays days.
		if key.Type() == quantumfs.KeyTypeConstant {
			buf := simplebuffer.New(nil, key)
			if err := cds.Get(nil, key, buf); err != nil {

				metadata, err := cs.cqlds.Metadata(
					walkutils.ToECtx(c), key.Value())
				if err != nil {
					return fmt.Errorf("path: %v key %v: %v", path, key.String(), err)
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

	showRootIDStatus := false
	err = walkHelper(cs.ctx, cs.qfsds, cs.qfsdb, wsname, co.progress,
		showRootIDStatus, finder)
	if err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}
	// Print all matches that we have collected so far
	// even though we hit an error.
	for k, v := range matchKey {
		fmt.Println(k, ": ", v)
	}
	return nil
}
