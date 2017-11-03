// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/qubit/tools/qwalker/cmd/cmdproc"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func init() {
	registerListCmd()
}

func registerListCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "list"
	cmd.Usage = ""
	cmd.Short = "list all workspaces"
	cmd.Details = ""
	cmd.Run = printList

	cmdproc.RegisterCommand(cmd)
}

func printList(args []string) error {
	if len(args) != 0 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}

	tsl, err := cs.qfsdb.TypespaceList(&cs.ctx.Ctx)
	if err != nil {
		return cmdproc.NewBadCmdExitErr("Listing Typespaces failed: %s", err)
	}
	for _, ts := range tsl {
		// Assuming we do not have _/X/Y
		if ts == quantumfs.NullSpaceName {
			continue
		}
		nsl, err := cs.qfsdb.NamespaceList(&cs.ctx.Ctx, ts)
		if err != nil {
			fmt.Printf("Listing Namespaces for TS:%s failed: %s\n", ts, err)
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
				if rootID, _, err = qubitutils.GetWorkspaceRootID(&cs.ctx.Ctx, cs.qfsdb, wsname); err != nil {
					return cmdproc.NewBadCmdExitErr("RootId not found for %v err: %v", wsname, err)
				}

				var lastWrite time.Time
				if lastWrite, err = cs.cqldb.WorkspaceLastWriteTime(cs.ctx, ts, ns, ws); err != nil {
					return cmdproc.NewBadCmdExitErr("Cannot find lastWriteTime for %s: %v", wsname, err)
				}
				fmt.Println()
				fmt.Printf("[%s] %v : %s : %d\n", lastWrite.Local().Format(time.UnixDate), rootID.String(), wsname, nonce)
			}
		}
	}
	return nil
}
