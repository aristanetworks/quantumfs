// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

func NewDataStore() quantumfs.DataStore {
	store := &DataStore{
		data: make(map[quantumfs.ObjectKey][]byte),
	}

	return store
}

type DataStore struct {
	mutex sync.RWMutex
	data  map[quantumfs.ObjectKey][]byte
}

func (store *DataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	var err error

	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if data, exists := store.data[key]; !exists {
		err = fmt.Errorf("Key does not exist")
	} else {
		buf.Set(data, key.Type())
	}
	return err
}

func (store *DataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buffer quantumfs.Buffer) error {

	if len(buffer.Get()) > quantumfs.MaxBlockSize {
		panic("Attempted to store overlarge block")
	}

	c.Vlog(qlog.LogDatastore, "Storing key %x len %d", key.Key[:], buffer.Size())
	store.mutex.Lock()
	store.data[key] = buffer.Get()
	store.mutex.Unlock()

	return nil
}

func (store *DataStore) Exists(c *quantumfs.Ctx, key quantumfs.ObjectKey) bool {
	store.mutex.RLock()
	_, exists := store.data[key]
	store.mutex.RUnlock()

	return exists
}
