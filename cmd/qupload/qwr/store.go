// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "strings"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/testutils"

func createAdvance(wsdb quantumfs.WorkspaceDB,
	wsname string,
	newKey quantumfs.ObjectKey) error {
	wsParts := strings.Split(wsname, "/")

	fmt.Println("Checking ", wsname)
	curKey, err := wsdb.Workspace(nil,
		wsParts[0], wsParts[1], wsParts[2])
	if err != nil {
		if wsdbErrCode, ok := err.(*quantumfs.WorkspaceDbErr); ok {
			if wsdbErrCode.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND {
				fmt.Println(wsname, " doesn't exist, so creating...")
				err = wsdb.BranchWorkspace(nil,
					quantumfs.NullSpaceName,
					quantumfs.NullSpaceName,
					quantumfs.NullSpaceName,
					wsParts[0], wsParts[1], wsParts[2])
			}
		}

		if err != nil {
			return err
		}

		fmt.Println("Checking ", wsname)
		curKey, err = wsdb.Workspace(nil,
			wsParts[0], wsParts[1], wsParts[2])
	}

	fmt.Println("Advancing ", wsname, curKey, newKey)
	_, err = wsdb.AdvanceWorkspace(nil,
		wsParts[0], wsParts[1], wsParts[2],
		curKey, newKey)
	if err != nil {
		return err
	}

	return nil
}

func CreateWorkspace(wsdb quantumfs.WorkspaceDB, ws string,
	advance string, newWsrKey quantumfs.ObjectKey) error {

	err := createAdvance(wsdb, ws, newWsrKey)
	if err != nil {
		return err
	}

	if advance != "" {
		err := createAdvance(wsdb, advance, newWsrKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeBlock(data []byte, keyType quantumfs.KeyType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	key := quantumfs.NewObjectKey(keyType, hash.Hash(data))
	buf := testutils.NewSimpleBuffer(data, key)
	return key, ds.Set(nil, key, buf)
}
