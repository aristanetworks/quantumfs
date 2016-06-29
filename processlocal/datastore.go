// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"

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

func (store *DataStore) Get(key quantumfs.ObjectKey) (quantumfs.Buffer, error) {

	var err error
	var buf quantumfs.Buffer
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if data, exists := store.data[key]; !exists {
		err = fmt.Errorf("Key does not exist")
	} else {
		buf = quantumfs.NewBuffer(data, key.Type())
	}
	return buf, err
}

func (store *DataStore) Set(key quantumfs.ObjectKey,
	buffer *quantumfs.Buffer) error {

	if len(buffer.Get()) > quantumfs.MaxBlockSize {
		panic("Attempted to store overlarge block")
	}

	store.mutex.Lock()
	store.data[key] = buffer.Get()
	store.mutex.Unlock()

	return nil
}

func (store *DataStore) Exists(key quantumfs.ObjectKey) bool {
	store.mutex.RLock()
	_, exists := store.data[key]
	store.mutex.RUnlock()

	return exists
}
