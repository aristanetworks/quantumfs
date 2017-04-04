// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "strings"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/testutils"

// create the workspace if it doesn't exist
// then advance it to the newKey
func createAdvance(qctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB,
	wsname string,
	newKey quantumfs.ObjectKey) error {

	wsParts := strings.Split(wsname, "/")
	curKey, err := wsdb.Workspace(qctx,
		wsParts[0], wsParts[1], wsParts[2])
	if err != nil {
		if wsdbErrCode, ok := err.(*quantumfs.WorkspaceDbErr); ok {
			if wsdbErrCode.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND {
				err = wsdb.BranchWorkspace(qctx,
					quantumfs.NullSpaceName,
					quantumfs.NullSpaceName,
					quantumfs.NullSpaceName,
					wsParts[0], wsParts[1], wsParts[2])
			}
		}
		if err != nil {
			return err
		}
		curKey, err = wsdb.Workspace(qctx,
			wsParts[0], wsParts[1], wsParts[2])
	}

	_, err = wsdb.AdvanceWorkspace(qctx,
		wsParts[0], wsParts[1], wsParts[2],
		curKey, newKey)
	if err != nil {
		return err
	}

	return nil
}

func CreateWorkspace(qctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB, ws string,
	advance string, newWsrKey quantumfs.ObjectKey) error {

	err := createAdvance(qctx, wsdb, ws, newWsrKey)
	if err != nil {
		return err
	}

	if advance != "" {
		err := createAdvance(qctx, wsdb, advance, newWsrKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeBlock(qctx *quantumfs.Ctx, data []byte, keyType quantumfs.KeyType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	key := quantumfs.NewObjectKey(keyType, hash.Hash(data))
	buf := testutils.NewSimpleBuffer(data, key)
	return key, ds.Set(qctx, key, buf)
}
