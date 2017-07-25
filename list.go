// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"

	"github.com/aristanetworks/quantumfs"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func printList() error {
	if walkFlags.NArg() != 1 {
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	tsl, err := cs.qfsdb.TypespaceList(cs.ctx)
	if err != nil {
		fmt.Println("Error in getting list of Typespaces")
		return err
	}
	for _, ts := range tsl {
		// Assuming we do not have _/X/Y
		if ts == quantumfs.NullSpaceName {
			continue
		}
		nsl, err := cs.qfsdb.NamespaceList(cs.ctx, ts)
		if err != nil {
			fmt.Printf("Error in getting list of Namespaces for TS:%s\n", ts)
			continue
		}
		for _, ns := range nsl {
			wsl, err := cs.qfsdb.WorkspaceList(cs.ctx, ts, ns)
			if err != nil {
				fmt.Printf("Error in getting list of Workspaces "+
					"for TS:%s NS:%s", ts, ns)
				continue
			}
			for _, ws := range wsl {
				var rootID quantumfs.ObjectKey
				wsname := ts + "/" + ns + "/" + ws
				if rootID, err = qubitutils.GetWorkspaceRootID(cs.ctx, cs.qfsdb, wsname); err != nil {
					return fmt.Errorf("RootId not found for %v err: %v", wsname, err)
				}
				fmt.Println()
				fmt.Printf("%v : %s\n", rootID.String(), wsname)
			}
		}
	}
	return nil
}
