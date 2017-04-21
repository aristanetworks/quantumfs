// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package aggregatedatastore

import "github.com/aristanetworks/quantumfs"

// AggregateDataStore combines 2 types of DataStore
// 1. Constant DataStore
// 2. Durable DataStore
type AggregateDataStore struct {
	durableDS  quantumfs.DataStore
	constantDS quantumfs.DataStore
}

// New returns a new instance of AggregateDataStore
func New(ds quantumfs.DataStore) quantumfs.DataStore {

	return &AggregateDataStore{
		durableDS:  ds,
		constantDS: quantumfs.ConstantStore,
	}
}

// Get for aggregateDataStore will check the following dataStore
// starting from the top:
// 1. Constant DataStore
// 2. Durable DataStore
func (store *AggregateDataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
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
func (store *AggregateDataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return store.durableDS.Set(c, key, buf)
}
