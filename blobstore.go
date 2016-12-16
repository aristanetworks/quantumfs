// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/gocql/gocql"
)

type cqlBlobStore struct {
	store    *cqlStore
	keyspace string
}

// NewCqlBlobStore initializes a blobstore.BlobStore to be used with a cql cluster
// This function is traversed only in the non-mock code path. In the mock path
// initCqlStore is directly used.
func NewCqlBlobStore(confName string) (blobstore.BlobStore, error) {

	cfg, err := readCqlConfig(confName)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed,
			"error in reading cql config file %s", err.Error())
	}

	cluster := NewRealCluster(cfg.Cluster)
	var store cqlStore
	store, err = initCqlStore(cluster)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed,
			"error in initializing cql store %s", err.Error())
	}

	cbs := &cqlBlobStore{
		store:    &store,
		keyspace: cfg.Cluster.KeySpace,
	}
	return cbs, nil
}

// Insert method has an semaphore limiting the number of
// concurrent Inserts to 100. This limits the number of concurrent
// insert queries to scyllaDB which is currently causing timeouts.
// This workaround should be removed when get a fix from ScyllaDB.
// The number 100, has been emperically determined.
func (b *cqlBlobStore) Insert(key string, value []byte,
	metadata map[string]string) error {

	queryStr := fmt.Sprintf("INSERT into %s.blobStore (key, value) VALUES (?, ?)", b.keyspace)
	query := b.store.session.Query(queryStr, key, value)

	b.store.sem.P()
	defer b.store.sem.V()

	err := query.Exec()
	if err != nil {
		return blobstore.NewError(blobstore.ErrOperationFailed, "error in Insert %s", err.Error())
	}
	return nil
}

// Get is the cql implementation of blobstore.Get()
func (b *cqlBlobStore) Get(key string) ([]byte, map[string]string, error) {

	// Session.Query() does not return error
	var value []byte
	queryStr := fmt.Sprintf("SELECT value FROM %s.blobStore WHERE key = ?", b.keyspace)
	query := b.store.session.Query(queryStr, key)
	err := query.Scan(&value)

	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil, blobstore.NewError(blobstore.ErrKeyNotFound, "error Get %s",
				err.Error())
		}
		return nil, nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in Get %s",
			err.Error())
	}
	return value, nil, nil
}

// Delete is the cql implementation of blobstore.Delete()
func (b *cqlBlobStore) Delete(key string) error {
	panic("Delete not implemented")
}

// Metadata is the cql implementation of blobstore.Metadata()
func (b *cqlBlobStore) Metadata(key string) (map[string]string, error) {
	panic("Metadata not implemented")
}

// Update is the cql implementation of blobstore.Update()
func (b *cqlBlobStore) Update(key string, metadata map[string]string) error {
	panic("Update not implemented")
}
