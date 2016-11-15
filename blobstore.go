// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"github.com/aristanetworks/ether"
	"github.com/gocql/gocql"
)

type cqlBlobStore struct {
	store *cqlStore
}

// NewCqlBlobStore initializes a ether.BlobStore to be used with a cql cluster
// This function is taversed only in the non-mock code path. In the mock path
// initCqlStore is directly used.
func NewCqlBlobStore(confName string) (ether.BlobStore, ether.ErrorResponse) {

	etherr := ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: "", Internal: nil}
	mocking := false

	cfg, err := readCqlConfig(confName)
	if err != nil {
		etherr.ErrorCode = ether.ErrBadArguments
		etherr.ErrorMessage = "Error reading cql config"
		etherr.Internal = err
		return nil, etherr
	}

	cluster := NewRealCluster(cfg.Nodes...)
	var store cqlStore
	store, err = initCqlStore(cluster, mocking)
	if err != nil {
		etherr.ErrorCode = ether.ErrBadArguments
		etherr.ErrorMessage = "Error in initCqlStore"
		etherr.Internal = err
		return nil, etherr
	}

	cbs := &cqlBlobStore{
		store: &store,
	}
	return cbs, etherr
}

func (b *cqlBlobStore) Insert(key string, value []byte,
	metadata map[string]string) (etherr ether.ErrorResponse) {

	etherr = ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: "", Internal: nil}

	// Session.Query() does not return error
	query := b.store.session.Query(`INSERT into blobStore (key, value) VALUES (?, ?)`, key, value)
	err := query.Exec()

	if err != nil {
		etherr.ErrorCode = ether.ErrOperationFailed
		etherr.ErrorMessage = "Insert operation failed"
		etherr.Internal = err
	}
	return etherr
}

func (b *cqlBlobStore) Get(key string) (value []byte,
	metadata map[string]string, etherr ether.ErrorResponse) {

	etherr = ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: "", Internal: nil}

	// Session.Query() does not return error
	query := b.store.session.Query(`SELECT value FROM blobStore WHERE key = ?`, key)
	err := query.Scan(&value)

	if err != nil {
		if err == gocql.ErrNotFound {
			etherr.ErrorCode = ether.ErrKeyNotFound
		} else {
			etherr.ErrorCode = ether.ErrOperationFailed
		}
		etherr.ErrorMessage = "Select operation failed"
		etherr.Internal = err
		value = nil
	}
	return value, nil, etherr
}

func (b *cqlBlobStore) Delete(key string) (etherr ether.ErrorResponse) {
	panic("Delete not implemented")
}

func (b *cqlBlobStore) Metadata(key string) (metadata map[string]string,
	etherr ether.ErrorResponse) {
	panic("Metadata not implemented")
}

func (b *cqlBlobStore) Update(key string,
	metadata map[string]string) (etherr ether.ErrorResponse) {
	panic("Update not implemented")
}
