// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"github.com/aristanetworks/ether/blobstore"
	"github.com/gocql/gocql"
)

type cqlBlobStore struct {
	store *cqlStore
}

// NewCqlBlobStore initializes a blobstore.BlobStore to be used with a cql cluster
// This function is traversed only in the non-mock code path. In the mock path
// initCqlStore is directly used.
func NewCqlBlobStore(confName string) (blobstore.BlobStore, error) {

	mocking := false

	cfg, err := readCqlConfig(confName)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in reading cql config file %s", err.Error())
	}

	cluster := NewRealCluster(cfg.Nodes...)
	var store cqlStore
	store, err = initCqlStore(cluster, mocking)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in initializing cql store %s", err.Error())
	}

	cbs := &cqlBlobStore{
		store: &store,
	}
	return cbs, nil
}

func (b *cqlBlobStore) Insert(key string, value []byte,
	metadata map[string]string) error {

	// Session.Query() does not return error
	query := b.store.session.Query(`INSERT into blobStore (key, value) VALUES (?, ?)`, key, value)
	err := query.Exec()

	if err != nil {
		return blobstore.NewError(blobstore.ErrOperationFailed, "error in Insert %s", err.Error())
	}
	return nil
}

func (b *cqlBlobStore) Get(key string) ([]byte, map[string]string, error) {

	// Session.Query() does not return error
	var value []byte
	query := b.store.session.Query(`SELECT value FROM blobStore WHERE key = ?`, key)
	err := query.Scan(&value)

	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil, blobstore.NewError(blobstore.ErrKeyNotFound, "error Get %s", err.Error())
		}
		return nil, nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in Get %s", err.Error())
	}
	return value, nil, nil
}

func (b *cqlBlobStore) Delete(key string) error {
	panic("Delete not implemented")
}

func (b *cqlBlobStore) Metadata(key string) (map[string]string, error) {
	panic("Metadata not implemented")
}

func (b *cqlBlobStore) Update(key string, metadata map[string]string) error {
	panic("Update not implemented")
}
