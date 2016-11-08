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
func NewCqlBlobStore(confName string) (ether.BlobStore, ether.ErrorResponse) {

	initCqlStore(confName)

	cbs := &cqlBlobStore{
		store: &globalCqlStore,
	}
	err := ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: ""}
	return cbs, err
}

func formatError(e error) (err ether.ErrorResponse) {
	err.ErrorCode = ether.ErrOperationFailed
	err.ErrorMessage = e.Error()
	return err
}

func (b *cqlBlobStore) Get(key string) (value []byte,
	metadata map[string]string, err ether.ErrorResponse) {

	err = ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: ""}

	e := b.store.session.Query(
		`SELECT value FROM blobStore WHERE key = ?`, key).Scan(&value)

	if e != nil {
		if e == gocql.ErrNotFound {
			err.ErrorCode = ether.ErrKeyNotFound
		} else {
			err.ErrorCode = ether.ErrOperationFailed
		}
		err.ErrorMessage = "Select operation failed"
		err.Err = e
		return nil, nil, err
	}

	return value, nil, err
}

func (b *cqlBlobStore) Insert(key string, value []byte,
	metadata map[string]string) (err ether.ErrorResponse) {
	err = ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: ""}

	e := b.store.session.Query(
		`INSERT into blobStore (key, value) VALUES (?, ?)`, key, value).Exec()

	if e != nil {
		err.ErrorCode = ether.ErrOperationFailed
		err.ErrorMessage = "Insert operation failed"
		err.Err = e
		return err
	}
	return err
}

func (b *cqlBlobStore) Delete(key string) (err ether.ErrorResponse) {
	panic("Delete not implemented")
}

func (b *cqlBlobStore) Metadata(key string) (metadata map[string]string,
	err ether.ErrorResponse) {
	panic("Metadata not implemented")
}

func (b *cqlBlobStore) Update(key string,
	metadata map[string]string) (err ether.ErrorResponse) {
	err = ether.ErrorResponse{ErrorCode: ether.ErrOk, ErrorMessage: ""}
	panic("Update not implemented")
}
