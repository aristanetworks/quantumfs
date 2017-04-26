// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package thirdparty_backends

import "fmt"

import "github.com/aristanetworks/quantumfs"

// Select the datastore with the given name and construct it with the given
// configuration string.
func ConnectDatastore(name string,
	config string) (quantumfs.DataStore, error) {

	for _, datastore := range datastores {
		if datastore.Name != name {
			continue
		}

		store := datastore.Constructor(config)
		if store == nil {
			return nil, fmt.Errorf("Datastore connection "+
				"failed '%s:%s'\n", name, config)
		}
		return store, nil
	}

	return nil, fmt.Errorf("Failed to find datastore '%s'\n", name)
}

// Select the workspaceDB with the given name and construct it with the given
// configuration string.
func ConnectWorkspaceDB(name string,
	config string) (quantumfs.WorkspaceDB, error) {

	for _, db := range workspaceDBs {
		if db.Name != name {
			continue
		}

		ws := db.Constructor(config)
		if ws == nil {
			return nil, fmt.Errorf("WorkspaceDB connection "+
				"failed '%s:%s'\n", name, config)
		}
		return ws, nil
	}

	return nil, fmt.Errorf("Failed to find workspaceDB '%s'\n", name)
}
