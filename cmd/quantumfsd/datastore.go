// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import "arista.com/quantumfs"

// This file contains all the interaction with the datastore.
var DataStore = newDataStore()

func newDataStore() *dataStore {
	return &dataStore{}
}

type dataStore struct {
}

func (store *dataStore) Get(key quantumfs.ObjectKey) *quantumfs.Buffer {
	buffer := quantumfs.Buffer{}
	err := quantumfs.ConstantStore.Get(key, &buffer)
	if err != nil {
		return nil
	}

	return &buffer
}
