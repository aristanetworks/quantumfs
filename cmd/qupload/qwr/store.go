// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "strings"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/thirdparty_backends"

func ConnectDatastore(name string,
	config string) (quantumfs.DataStore, error) {

	for _, datastore := range thirdparty_backends.Datastores {
		if datastore.Name != name {
			continue
		}

		store := datastore.Constructor(config)
		if store == nil {
			return nil, fmt.Errorf("Datastore connection failed '%s:%s'\n",
				name, config)
		}
		return store, nil
	}

	return nil, fmt.Errorf("Failed to find datastore '%s'\n", name)
}

func ConnectWorkspaceDB(name string,
	config string) (quantumfs.WorkspaceDB, error) {

	for _, db := range thirdparty_backends.WorkspaceDBs {
		if db.Name != name {
			continue
		}

		ws := db.Constructor(config)
		if ws == nil {
			return nil, fmt.Errorf("WorkspaceDB connection failed '%s:%s'\n",
				name, config)
		}
		return ws, nil
	}

	return nil, fmt.Errorf("Failed to find workspaceDB '%s'\n", name)
}

func CreateWorkspace(wsdb quantumfs.WorkspaceDB, ws string,
	advance string, newWsrKey quantumfs.ObjectKey) error {

	wsParts := strings.Split(ws, "/")
	err := wsdb.BranchWorkspace(nil, "_null", "_null", "null",
		wsParts[0], wsParts[1], wsParts[2])
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

		curKey, err := wsdb.Workspace(nil,
			wsParts[0], wsParts[1], wsParts[2])
		if err != nil {
			return err
		}
		_, err = wsdb.AdvanceWorkspace(nil,
			wsParts[0], wsParts[1], wsParts[2],
			curKey, newWsrKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeBlob(data []byte, keyType quantumfs.KeyType,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	key := quantumfs.NewObjectKey(keyType, hash.Hash(data))
	buf := testutils.NewSimpleBuffer(data, key)
	return key, ds.Set(nil, key, buf)
}
