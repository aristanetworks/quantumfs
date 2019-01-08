// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package processlocal

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/hash"
	"github.com/aristanetworks/quantumfs/qlog"
)

func NewDataStore(conf string) quantumfs.DataStore {
	store := &dataStore{
		data: make(map[string][]byte),
	}

	return store
}

type dataStore struct {
	mutex sync.RWMutex
	data  map[string][]byte
}

func (store *dataStore) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	defer c.FuncIn(qlog.LogDatastore, "processlocal::Get",
		"key %s", key.String()).Out()

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

func (store *dataStore) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buffer quantumfs.Buffer) error {

	defer c.FuncIn(qlog.LogDatastore, "processlocal::Set",
		"key %s", key.String()).Out()

	if buffer.Size() > int(quantumfs.MaxBlockSize) {
		panic("Attempted to store overlarge block")
	}

	store.mutex.RLock()
	data, exists := store.data[key.String()]
	store.mutex.RUnlock()
	if exists {
		if !bytes.Equal(buffer.Get(), data) {
			c.Elog(qlog.LogDatastore, "Key Collision! %s",
				key.String())
			digest := md5.Sum(data)
			qhash := hash.Hash(data)
			c.Elog(qlog.LogDatastore, "Original data: %s %s %s",
				hex.EncodeToString(digest[:]),
				hex.EncodeToString(qhash[:]),
				quantumfs.NewObjectKey(quantumfs.KeyTypeMetadata,
					qhash).String())
			digest = md5.Sum(buffer.Get())
			qhash = hash.Hash(buffer.Get())
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

func (store *dataStore) Freshen(c *quantumfs.Ctx, key quantumfs.ObjectKey) error {
	defer c.FuncIn(qlog.LogDatastore, "processlocal::Freshen", "key %s",
		key.String()).Out()

	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if _, exists := store.data[key.String()]; !exists {
		return fmt.Errorf("Key does not exist")
	}
	return nil
}
