// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "strings"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/testutils"

func createWsIfNotExist(wsdb quantumfs.WorkspaceDB,
	wsname string) (quantumfs.ObjectKey, error) {
	wsParts := strings.Split(wsname, "/")

	curKey, err := wsdb.Workspace(nil,
		wsParts[0], wsParts[1], wsParts[2])
	if err != nil {
		if wsdbErrCode, ok := err.(*quantumfs.WorkspaceDbErr); ok {
			if wsdbErrCode.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND {
				err = wsdb.BranchWorkspace(nil,
					quantumfs.NullSpaceName,
					quantumfs.NullSpaceName,
					quantumfs.NullSpaceName,
					wsParts[0], wsParts[1], wsParts[2])
			}
		}

		if err != nil {
			return curKey, err
		}

		curKey, err = wsdb.Workspace(nil,
			wsParts[0], wsParts[1], wsParts[2])
	}

	return curKey, err

}

func CreateWorkspace(wsdb quantumfs.WorkspaceDB, ws string,
	advance string, newWsrKey quantumfs.ObjectKey) error {

	wsParts := strings.Split(ws, "/")
	curKey, err := createWsIfNotExist(wsdb, ws)
	if err != nil {
		return err
	}
	_, err = wsdb.AdvanceWorkspace(nil,
		wsParts[0], wsParts[1], wsParts[2],
		quantumfs.EmptyWorkspaceKey, newWsrKey)
	if err != nil {
		return err
	}

	if advance != "" {
		wsParts = strings.Split(advance, "/")
		curKey, err = createWsIfNotExist(wsdb, advance)
		if err != nil {
			_, err = wsdb.AdvanceWorkspace(nil,
				wsParts[0], wsParts[1], wsParts[2],
				curKey, newWsrKey)
			if err != nil {
				return err
			}
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
