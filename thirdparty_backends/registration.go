// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Utility functions necessary for various backend to register themselves to be valid
// options for quantumfsd to use.
package thirdparty_backends

import "github.com/aristanetworks/quantumfs"

type DatastoreConstructor func(conf string) quantumfs.DataStore
type Datastore struct {
	Name        string
	Constructor DatastoreConstructor
}

var Datastores []Datastore

func registerDatastore(name string, constructor DatastoreConstructor) {
	store := Datastore{
		Name:        name,
		Constructor: constructor,
	}

	Datastores = append(Datastores, store)
}
