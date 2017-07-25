// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/quantumfs"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func printList(c *quantumfs.Ctx, progress bool, qfsds quantumfs.DataStore,
	cqlds blobstore.BlobStore, wsdb quantumfs.WorkspaceDB) error {

	if walkFlags.NArg() != 1 {
		walkFlags.Usage()
		os.Exit(exitBadCmd)
	}

	tsl, err := wsdb.TypespaceList(c)
	if err != nil {
		fmt.Println("Error in getting list of Typespaces")
		return err
	}
	for _, ts := range tsl {
		// Assuming we do not have _/X/Y
		if ts == quantumfs.NullSpaceName {
			continue
		}
		nsl, err := wsdb.NamespaceList(c, ts)
		if err != nil {
			fmt.Printf("Error in getting list of Namespaces for TS:%s\n", ts)
			continue
		}
		for _, ns := range nsl {
			wsl, err := wsdb.WorkspaceList(c, ts, ns)
			if err != nil {
				fmt.Printf("Error in getting list of Workspaces "+
					"for TS:%s NS:%s", ts, ns)
				continue
			}
			for _, ws := range wsl {
				var rootID quantumfs.ObjectKey
				wsname := ts + "/" + ns + "/" + ws
				if rootID, err = qubitutils.GetWorkspaceRootID(c, wsdb, wsname); err != nil {
					return fmt.Errorf("RootId not found for %v err: %v", wsname, err)
				}
				fmt.Println()
				fmt.Printf("%v : %s\n", rootID.String(), wsname)
			}
		}
	}
	return nil
}
