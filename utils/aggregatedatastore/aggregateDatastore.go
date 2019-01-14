// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package aggregateDataStore combines 2 types of DataStore
// 1. Constant DataStore
// 2. Durable DataStore
package aggregatedatastore

import "github.com/aristanetworks/quantumfs"

type aggregateDataStore struct {
	durableDS  quantumfs.DataStore
	constantDS quantumfs.DataStore
}

// New returns a new instance of aggregateDataStore
func New(ds quantumfs.DataStore) quantumfs.DataStore {

	return &aggregateDataStore{
		durableDS:  ds,
		constantDS: quantumfs.ConstantStore,
	}
}

// Get for aggregateDataStore will check the following dataStore
// starting from the top:
// 1. Constant DataStore
// 2. Durable DataStore
func (store *aggregateDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	var err error

	if err = store.constantDS.Get(c, key, buf); err != nil {
		if err = store.durableDS.Get(c, key, buf); err != nil {
			return err
		}
		return nil
	}
	return nil
}

// Set for aggredateDataStore will always put the data in Durable DataStore
func (store *aggregateDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.durableDS.Set(c, key, buf)
}

func (store *aggregateDataStore) Freshen(c *quantumfs.Ctx,
	key quantumfs.ObjectKey) error {

	return store.durableDS.Freshen(c, key)
}
