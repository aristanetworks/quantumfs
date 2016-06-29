// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"

// This file contains all the interaction with the datastore.
var DataStore = newDataStore()

func newDataStore() *dataStore {
	return &dataStore{}
}

type dataStore struct {
}

func (store *dataStore) Get(c *ctx, key quantumfs.ObjectKey) *quantumfs.Buffer {
	buf, err := quantumfs.ConstantStore.Get(key)
	if err == nil {
		return &buf
	}

	buf, err = c.durableStore.Get(key)
	if err == nil {
		return &buf
	}
	c.elog("Couldn't get from any store: %v. Key %s", err, key)

	return nil
}

func (store *dataStore) Set(c *ctx, buffer *quantumfs.Buffer) error {

	return c.durableStore.Set(buffer.Key(), buffer)
}
