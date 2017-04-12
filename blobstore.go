// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/utils/stats"
	"github.com/aristanetworks/ether/utils/stats/inmem"
	"github.com/gocql/gocql"
)

// Currently CQL blobstore ignores any non-CQL store specific
//  metadata.

// TimeToLive is a CQL store specific metadata.
// It represents the time after which the blob
// will be automatically cleaned up the store.
const TimeToLive = "cql.TTL"

type cqlBlobStore struct {
	store    *cqlStore
	keyspace string

	insertStats stats.OpStats
	getStats    stats.OpStats
}

// NewCqlBlobStore initializes a blobstore.BlobStore to be used with a CQL cluster.
func NewCqlBlobStore(confName string) (blobstore.BlobStore, error) {
	// This function is traversed only in the non-mock code path. In the mock path
	// initCqlStore is directly used. Also ensures that an active session exists with
	// the CQL cluster.

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
		store:       &store,
		keyspace:    cfg.Cluster.KeySpace,
		insertStats: inmem.NewOpStatsInMem("insertBlobStore"),
		getStats:    inmem.NewOpStatsInMem("getBlobStore"),
	}
	return cbs, nil
}

// Insert is the CQL implementation of blobstore.Insert()
func (b *cqlBlobStore) Insert(key string, value []byte,
	metadata map[string]string) error {

	if metadata == nil {
		return blobstore.NewError(blobstore.ErrBadArguments,
			"metadata is nil")
	}

	ttl, keyExists := metadata[TimeToLive]
	if !keyExists {
		return blobstore.NewError(blobstore.ErrBadArguments,
			"%s not found in metadata %v",
			TimeToLive, metadata)
	}

	queryStr := fmt.Sprintf(`INSERT
INTO %s.blobStore (key, value)
VALUES (?, ?)
USING TTL %s`, b.keyspace, ttl)
	query := b.store.session.Query(queryStr, key, value)

	b.store.sem.P()
	defer b.store.sem.V()

	start := time.Now()
	defer func() { b.insertStats.RecordOp(time.Since(start)) }()

	err := query.Exec()
	if err != nil {
		return blobstore.NewError(blobstore.ErrOperationFailed, "error in Insert %s", err.Error())
	}
	return nil
}

// Get is the CQL implementation of blobstore.Get()
func (b *cqlBlobStore) Get(key string) ([]byte, map[string]string, error) {

	// Session.Query() does not return error
	var value []byte
	var ttl int
	queryStr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, b.keyspace)
	query := b.store.session.Query(queryStr, key)

	start := time.Now()
	defer func() { b.getStats.RecordOp(time.Since(start)) }()

	err := query.Scan(&value, &ttl)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil, blobstore.NewError(blobstore.ErrKeyNotFound, "error Get %s",
				err.Error())
		}
		return nil, nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in Get %s",
			err.Error())
	}

	mdata := make(map[string]string)
	mdata[TimeToLive] = strconv.Itoa(ttl)
	return value, mdata, nil
}

// Delete is the CQL implementation of blobstore.Delete()
func (b *cqlBlobStore) Delete(key string) error {
	return blobstore.NewError(blobstore.ErrOperationFailed,
		"Delete operation is not implemented")
}

// Metadata is the CQL implementation of blobstore.Metadata()
func (b *cqlBlobStore) Metadata(key string) (map[string]string, error) {
	var ttl int
	queryStr := fmt.Sprintf(`SELECT ttl(value)
FROM %s.blobStore
WHERE key = ?`, b.keyspace)
	query := b.store.session.Query(queryStr, key)

	b.store.sem.P()
	defer b.store.sem.V()

	start := time.Now()
	// getStats includes both get and metadata API stats
	defer func() { b.getStats.RecordOp(time.Since(start)) }()

	err := query.Scan(&ttl)
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, blobstore.NewError(blobstore.ErrKeyNotFound, "error Metadata %s",
				err.Error())
		}
		return nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in Metadata %s",
			err.Error())
	}
	mdata := make(map[string]string)
	mdata[TimeToLive] = strconv.Itoa(ttl)
	return mdata, nil
}

// Update is the CQL implementation of blobstore.Update()
func (b *cqlBlobStore) Update(key string, metadata map[string]string) error {
	return blobstore.NewError(blobstore.ErrOperationFailed,
		"Update operation is not implemented")
}

func (b *cqlBlobStore) ReportAPIStats() {
	b.insertStats.(stats.OpStatReporter).ReportOpStats()
	b.getStats.(stats.OpStatReporter).ReportOpStats()
}
