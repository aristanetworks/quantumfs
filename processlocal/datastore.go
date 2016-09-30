// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

func NewDataStore(conf string) quantumfs.DataStore {
	store := &DataStore{
		data: make(map[string][]byte),
	}

	return store
}

type DataStore struct {
	mutex sync.RWMutex
	data  map[string][]byte
}

func (store *DataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	var err error

	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if data, exists := store.data[key.String()]; !exists {
		err = fmt.Errorf("Key does not exist")
	} else {
		buf.Set(data, key.Type())
	}
	return err
}

func (store *DataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buffer quantumfs.Buffer) error {

	if buffer.Size() > int(quantumfs.MaxBlockSize) {
		panic("Attempted to store overlarge block")
	}

	store.mutex.RLock()
	_, exists := store.data[key.String()]
	store.mutex.RUnlock()
	if exists {
		return nil
	}

	c.Vlog(qlog.LogDatastore, "Storing key %s len %d", key.String(),
		buffer.Size())
	store.mutex.Lock()
	store.data[key.String()] = buffer.Get()
	store.mutex.Unlock()

	return nil
}
