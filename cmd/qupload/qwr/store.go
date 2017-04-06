// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "strings"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/testutils"

// It is possible for multiple uploads to be destined to the same
// workspace. At the same time, re-trying a failed upload into same
// workspace must be supported.
//
// When multiple uploads are uploading to same workspace only one will
// succeed, others will get WSDB_OUT_OF_DATE error from Advance API and
// such uploads are failed since their content won't be accessible without
// a successful Advance.
//
// WSDB_WORKSPACE_EXISTS error in Branch API can happen during multi-upload
// or upload-retry scenarios. The error should be ignored in upload-rety
// scenario. The error can be ignored in multi-upload scenario too since such
// scenarios are handled by error check in Advance API.
func branchThenAdvance(qctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB,
	wsname string,
	newKey quantumfs.ObjectKey) error {

	wsParts := strings.Split(wsname, "/")
	err := wsdb.BranchWorkspace(qctx,
		quantumfs.NullSpaceName,
		quantumfs.NullSpaceName,
		quantumfs.NullSpaceName,
		wsParts[0], wsParts[1], wsParts[2])
	if err != nil {
		if wsdbErrCode, ok := err.(*quantumfs.WorkspaceDbErr); ok {
			if wsdbErrCode.Code != quantumfs.WSDB_WORKSPACE_EXISTS {
				// see note above on why we can ignore
				// this error
				return err
			}
		}
	}

	var curKey quantumfs.ObjectKey
	curKey, err = wsdb.Workspace(qctx,
		wsParts[0], wsParts[1], wsParts[2])
	if err != nil {
		return err
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

	err := branchThenAdvance(qctx, wsdb, ws, newWsrKey)
	if err != nil {
		return err
	}

	if advance != "" {
		err := branchThenAdvance(qctx, wsdb, advance, newWsrKey)
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
