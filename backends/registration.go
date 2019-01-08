// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Utility functions necessary for various backend to register themselves to be valid
// options for quantumfsd to use.
package backends

import "github.com/aristanetworks/quantumfs"

type DatastoreConstructor func(conf string) quantumfs.DataStore
type datastore struct {
	Name        string
	Constructor DatastoreConstructor
}

var datastores []datastore

func registerDatastore(name string, constructor DatastoreConstructor) {
	store := datastore{
		Name:        name,
		Constructor: constructor,
	}

	datastores = append(datastores, store)
}

type WorkspaceDBConstructor func(conf string) quantumfs.WorkspaceDB
type workspaceDB struct {
	Name        string
	Constructor WorkspaceDBConstructor
}

var workspaceDBs []workspaceDB

func registerWorkspaceDB(name string, constructor WorkspaceDBConstructor) {
	db := workspaceDB{
		Name:        name,
		Constructor: constructor,
	}

	workspaceDBs = append(workspaceDBs, db)
}

type TimeSeriesDBConstructor func(conf string) quantumfs.TimeSeriesDB
type timeseriesDB struct {
	Name        string
	Constructor TimeSeriesDBConstructor
}

var timeseriesDBs []timeseriesDB

func registerTimeSeriesDB(name string, constructor TimeSeriesDBConstructor) {
	db := timeseriesDB{
		Name:        name,
		Constructor: constructor,
	}

	timeseriesDBs = append(timeseriesDBs, db)
}
