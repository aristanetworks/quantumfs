// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "github.com/aristanetworks/quantumfs"

func newDataStore(durableStore quantumfs.DataStore) *dataStore {
	return &dataStore{
		durableStore: durableStore,
	}
}

type dataStore struct {
	durableStore quantumfs.DataStore
}

func (store *dataStore) Get(c *ctx, key quantumfs.ObjectKey) *quantumfs.Buffer {
	buf, err := quantumfs.ConstantStore.Get(key)
	if err == nil {
		return &buf
	}

	buf, err = store.durableStore.Get(key)
	if err == nil {
		return &buf
	}
	c.elog("Couldn't get from any store: %v. Key %s", err, key)

	return nil
}

func (store *dataStore) Set(c *ctx, buffer *quantumfs.Buffer) error {

	return store.durableStore.Set(buffer.Key(), buffer)
}
