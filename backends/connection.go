// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package backends

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
)

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
				"failed '%s:%s'", name, config)
		}
		return store, nil
	}

	return nil, fmt.Errorf("Failed to find datastore '%s'", name)
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
				"failed '%s:%s'", name, config)
		}
		return ws, nil
	}

	return nil, fmt.Errorf("Failed to find workspaceDB '%s'", name)
}

func ConnectTimeSeriesDB(name string,
	config string) (quantumfs.TimeSeriesDB, error) {

	for _, db := range timeseriesDBs {
		if db.Name != name {
			continue
		}

		tsdb := db.Constructor(config)
		if tsdb == nil {
			return nil, fmt.Errorf("TimeSeriesDB connection "+
				"failed '%s:%s'", name, config)
		}
		return tsdb, nil
	}

	return nil, fmt.Errorf("Failed to find timeseriesDB '%s'", name)
}
