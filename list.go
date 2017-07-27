// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"

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

	tsl, err := cs.qfsdb.TypespaceList(cs.ctx)
	if err != nil {
		return cmdproc.NewBadCmdExitErr("Listing Typespaces failed: %s", err)
	}
	for _, ts := range tsl {
		// Assuming we do not have _/X/Y
		if ts == quantumfs.NullSpaceName {
			continue
		}
		nsl, err := cs.qfsdb.NamespaceList(cs.ctx, ts)
		if err != nil {
			fmt.Printf("Listing Namespaces for TS:%s failed: %s\n", ts, err)
			continue
		}
		for _, ns := range nsl {
			wsl, err := cs.qfsdb.WorkspaceList(cs.ctx, ts, ns)
			if err != nil {
				fmt.Printf("Listing Workspaces "+
					"for TS:%s NS:%s failed: %s\n", ts, ns, err)
				continue
			}
			for _, ws := range wsl {
				var rootID quantumfs.ObjectKey
				wsname := ts + "/" + ns + "/" + ws
				if rootID, err = qubitutils.GetWorkspaceRootID(cs.ctx, cs.qfsdb, wsname); err != nil {
					return cmdproc.NewBadCmdExitErr("RootId not found for %v err: %v", wsname, err)
				}
				fmt.Println()
				fmt.Printf("%v : %s\n", rootID.String(), wsname)
			}
		}
	}
	return nil
}
