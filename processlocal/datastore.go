// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "bytes"
import "crypto/md5"
import "encoding/hex"
import "fmt"
import "sync"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/hash"
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

	c.Vlog(qlog.LogDatastore, "---In processlocal::Get key %s", key.String())
	defer c.Vlog(qlog.LogDatastore, "Out-- processlocal::Get")

	var err error

	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if data, exists := store.data[key.String()]; !exists {
		err = fmt.Errorf("Key does not exist")
	} else {
		newData := make([]byte, len(data))
		copy(newData, data)
		buf.Set(newData, key.Type())
	}
	return err
}

func (store *DataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buffer quantumfs.Buffer) error {

	c.Vlog(qlog.LogDatastore, "---In processlocal::Set key %s", key.String())
	defer c.Vlog(qlog.LogDatastore, "Out-- processlocal::Set")

	if buffer.Size() > int(quantumfs.MaxBlockSize) {
		panic("Attempted to store overlarge block")
	}

	store.mutex.RLock()
	data, exists := store.data[key.String()]
	store.mutex.RUnlock()
	if exists {
		if !bytes.Equal(buffer.Get(), data) {
			c.Elog(qlog.LogDatastore, "ERROR: Key Collision! %s",
				key.String())
			digest := md5.Sum(data)
			qhash := hash.Hash(data)
			c.Elog(qlog.LogDatastore, "Original data: %s %s %s",
				hex.EncodeToString(digest[:]),
				hex.EncodeToString(qhash[:]),
				quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata,
					qhash).String())
			digest = md5.Sum(buffer.Get())
			qhash = hash.Hash(data)
			c.Elog(qlog.LogDatastore, "New data:      %s %s %s",
				hex.EncodeToString(digest[:]),
				hex.EncodeToString(qhash[:]),
				quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata,
					qhash).String())
		}
		return nil
	}

	c.Vlog(qlog.LogDatastore, "Storing key %s len %d", key.String(),
		buffer.Size())
	store.mutex.Lock()
	store.data[key.String()] = buffer.Get()
	store.mutex.Unlock()

	return nil
}
