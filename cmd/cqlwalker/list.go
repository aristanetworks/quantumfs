// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package main

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/cmd/cqlwalker/utils"
)

func init() {
	registerListCmd()
}

func registerListCmd() {
	var cmd CommandInfo
	cmd.Name = "list"
	cmd.Usage = ""
	cmd.Short = "list all workspaces"
	cmd.Details = ""
	cmd.Run = printList

	RegisterCommand(cmd)
}

func printList(args []string) error {
	if len(args) != 0 {
		return NewBadArgExitErr("incorrect arguments")
	}

	tsl, err := cs.qfsdb.TypespaceList(&cs.ctx.Ctx)
	if err != nil {
		return NewBadCmdExitErr("Listing Typespaces failed: %s", err)
	}
	for _, ts := range tsl {
		// Assuming we do not have _/X/Y
		if ts == quantumfs.NullSpaceName {
			continue
		}
		nsl, err := cs.qfsdb.NamespaceList(&cs.ctx.Ctx, ts)
		if err != nil {
			fmt.Printf("Listing Namespaces for TS:%s failed: %s\n", ts,
				err)
			continue
		}
		for _, ns := range nsl {
			wsMap, err := cs.qfsdb.WorkspaceList(&cs.ctx.Ctx, ts, ns)
			if err != nil {
				fmt.Printf("Listing Workspaces "+
					"for TS:%s NS:%s failed: %s\n", ts, ns, err)
				continue
			}
			for ws, nonce := range wsMap {
				var rootID quantumfs.ObjectKey
				wsname := ts + "/" + ns + "/" + ws
				if rootID, _, err = utils.GetWorkspaceRootID(
					&cs.ctx.Ctx, cs.qfsdb, wsname); err != nil {
					return NewBadCmdExitErr("RootId "+
						"not found for %v err: %v",
						wsname, err)
				}

				var lastWrite time.Time
				if lastWrite, err =
					cs.cqldb.WorkspaceLastWriteTime(cs.ctx, ts,
						ns, ws); err != nil {
					return NewBadCmdExitErr("Cannot "+
						"find lastWriteTime for %s: %v",
						wsname, err)
				}
				fmt.Println()
				fmt.Printf("[%s] %v : %s : %d\n",
					lastWrite.Local().Format(time.UnixDate),
					rootID.String(), wsname, nonce)
			}
		}
	}
	return nil
}
