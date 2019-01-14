// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package cql

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aristanetworks/quantumfs/backends/cql/stats"
	"github.com/aristanetworks/quantumfs/backends/cql/stats/inmem"
	"github.com/gocql/gocql"
)

// Currently CQL blobstore ignores any non-CQL store specific
//  metadata.

// TimeToLive is a CQL store specific metadata.
// It represents the time after which the blob
// will be automatically cleaned up the store.
const TimeToLive = "cql.TTL"

// cqlBlobStore implements both BlobStore interface
// CqlStore interface.
type cqlBlobStore struct {
	store    *cqlStore
	keyspace string
	cfName   string

	insertStats stats.OpStats
	getStats    stats.OpStats
}

func newCqlBS(cluster Cluster, cfg *Config) (BlobStore, error) {
	store, err := initCqlStore(cluster)
	if err != nil {
		return nil, NewError(ErrOperationFailed,
			"error in initializing cql store %s", err.Error())
	}

	bsName, _ := prefixToTblNames(os.Getenv("CFNAME_PREFIX"))
	if err := isTablePresent(&store, cfg, cfg.Cluster.KeySpace,
		bsName); err != nil {

		return nil, NewError(ErrOperationFailed, "%s", err.Error())
	}

	cbs := &cqlBlobStore{
		store:       &store,
		keyspace:    cfg.Cluster.KeySpace,
		cfName:      bsName,
		insertStats: inmem.NewOpStatsInMem("insertBlobStore"),
		getStats:    inmem.NewOpStatsInMem("getBlobStore"),
	}
	return cbs, nil
}

// NewCqlBlobStore initializes a BlobStore to be used with a CQL cluster.
func NewCqlBlobStore(confName string) (BlobStore, error) {
	cfg, err := readCqlConfig(confName)
	if err != nil {
		return nil, NewError(ErrOperationFailed,
			"error in reading cql config file %s", err.Error())
	}

	cluster := NewRealCluster(&cfg.Cluster)

	return newCqlBS(cluster, cfg)
}

// InsertLog can be used in external tool for log parsing
const InsertLog = "Cql::Insert"

// GoCqlInsertLog can be used in external tool for log parsing
const GoCqlInsertLog = "GoCql::Insert"

// KeyTTLLog can be used in external tool for log parsing
const KeyTTLLog = "Key: %s TTL: %s"

// Insert is the CQL implementation of Insert()
func (b *cqlBlobStore) Insert(c ctx, key []byte, value []byte,
	metadata map[string]string) error {
	keyHex := hex.EncodeToString(key)
	if metadata == nil {
		return NewError(ErrBadArguments,
			"metadata is nil")
	}

	ttl, keyExists := metadata[TimeToLive]
	if !keyExists {
		return NewError(ErrBadArguments,
			"%s not found in metadata %v",
			TimeToLive, metadata)
	}

	defer c.FuncIn(InsertLog, KeyTTLLog, keyHex, ttl).Out()
	queryStr := fmt.Sprintf(`INSERT
INTO %s.%s (key, value)
VALUES (?, ?)
USING TTL %s`, b.keyspace, b.cfName, ttl)
	query := b.store.session.Query(queryStr, key, value)

	b.store.sem.P()
	defer b.store.sem.V()

	start := time.Now()
	defer func() { b.insertStats.RecordOp(time.Since(start)) }()

	var err error
	func() {
		defer c.FuncIn(GoCqlInsertLog, KeyTTLLog, keyHex, ttl).Out()
		err = query.Exec()
	}()
	if err != nil {
		return NewError(ErrOperationFailed,
			"error in Insert[%s] %s", keyHex, err.Error())
	}
	return nil
}

// GetLog can be used in external tool for log parsing
const GetLog = "Cql::Get"

// GoCqlGetLog can be used in external tool for log parsing
const GoCqlGetLog = "GoCql::Get"

// Get is the CQL implementation of Get()
func (b *cqlBlobStore) Get(c ctx, key []byte) ([]byte, map[string]string, error) {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(GetLog, KeyLog, keyHex).Out()

	// Session.Query() does not return error
	var value []byte
	var ttl int
	queryStr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.%s
WHERE key = ?`, b.keyspace, b.cfName)
	query := b.store.session.Query(queryStr, key)

	start := time.Now()
	defer func() { b.getStats.RecordOp(time.Since(start)) }()

	var err error
	func() {
		defer c.FuncIn(GoCqlGetLog, KeyLog, keyHex).Out()
		err = query.Scan(&value, &ttl)

	}()
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, nil, NewError(ErrKeyNotFound, "error Get[%s] %s",
				keyHex, err.Error())
		}
		return nil, nil, NewError(ErrOperationFailed, "error in Get[%s] %s",
			keyHex, err.Error())
	}

	mdata := make(map[string]string)
	mdata[TimeToLive] = strconv.Itoa(ttl)
	c.Vlog(GetLog+" "+KeyTTLLog, keyHex, mdata[TimeToLive])
	return value, mdata, nil
}

// DeleteLog can be used in external tool for log parsing
const DeleteLog = "Cql::Delete"

// Delete is the CQL implementation of Delete()
func (b *cqlBlobStore) Delete(c ctx, key []byte) error {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(DeleteLog, KeyLog, keyHex).Out()
	return NewError(ErrOperationFailed,
		"Delete operation is not implemented")
}

// MetadataLog can be used in external tool for log parsing
const MetadataLog = "Cql::Metadata"

// GoCqlMetadataLog can be used in external tool for log parsing
const GoCqlMetadataLog = "GoCql::Metadata"

// Metadata is the CQL implementation of Metadata()
// Note: retreiving this information does not have performance overhead
func (b *cqlBlobStore) Metadata(c ctx, key []byte) (map[string]string, error) {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(MetadataLog, KeyLog, keyHex).Out()
	var ttl int
	queryStr := fmt.Sprintf(`SELECT ttl(value)
FROM %s.%s
WHERE key = ?`, b.keyspace, b.cfName)
	query := b.store.session.Query(queryStr, key)

	b.store.sem.P()
	defer b.store.sem.V()

	start := time.Now()
	// getStats includes both get and metadata API stats
	defer func() { b.getStats.RecordOp(time.Since(start)) }()

	var err error
	func() {
		defer c.FuncIn(GoCqlMetadataLog, KeyLog, keyHex).Out()
		err = query.Scan(&ttl)
	}()
	if err != nil {
		if err == gocql.ErrNotFound {
			return nil, NewError(ErrKeyNotFound,
				"error Metadata[%s] %s", keyHex, err.Error())
		}
		return nil, NewError(ErrOperationFailed,
			"error in Metadata[%s] %s", keyHex, err.Error())
	}
	mdata := make(map[string]string)
	mdata[TimeToLive] = strconv.Itoa(ttl)
	c.Vlog(MetadataLog+" "+KeyTTLLog, keyHex, mdata[TimeToLive])
	return mdata, nil
}

// UpdateLog can be used in external tool for log parsing
const UpdateLog = "Cql::Update"

// Update is the CQL implementation of Update()
func (b *cqlBlobStore) Update(c ctx, key []byte, metadata map[string]string) error {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(UpdateLog, KeyLog, keyHex).Out()
	return NewError(ErrOperationFailed,
		"Update operation is not implemented")
}

func (b *cqlBlobStore) ReportAPIStats() {
	b.insertStats.(stats.OpStatReporter).ReportOpStats()
	b.getStats.(stats.OpStatReporter).ReportOpStats()
}

// Keyspace returns the keyspace for this CQL blobstore
func (b *cqlBlobStore) Keyspace() string {
	// since we don't support changing keyspace after
	// the session has established, returning the configured
	// keyspace is fine
	return b.keyspace
}

// GetExtKeyInfoLog can be used in external tool for log parsing
const GetExtKeyInfoLog = "Cql::GetExtKeyInfoLog"
const GoGetExtKeyInfoLog = "GoCql::GetExtKeyInfoLog"

// ExtKeyinfo returns Extended CQL specific information for a key from
// CQL blobstore
// Note: This API is intended to be used for debugging purposes.
//       Since this API may be slow in collecting extended information
//       use it with caution.
func (b *cqlBlobStore) GetExtKeyInfo(c ctx,
	key []byte) (ExtKeyInfo, error) {

	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(GetExtKeyInfoLog, KeyLog, keyHex).Out()

	var info ExtKeyInfo
	queryStr := fmt.Sprintf(`SELECT ttl(value), writetime(value)
FROM %s.%s
WHERE key = ?`, b.keyspace, b.cfName)
	query := b.store.session.Query(queryStr, key)

	var err error
	var writeTime int64
	func() {
		defer c.FuncIn(GoGetExtKeyInfoLog, KeyLog, keyHex).Out()
		err = query.Scan(&info.TTL, &writeTime)
	}()
	if err != nil {
		if err == gocql.ErrNotFound {
			return ExtKeyInfo{},
				NewError(ErrKeyNotFound,
					"error ExtKeyInfo[%s] %s", keyHex,
					err.Error())
		}
		return ExtKeyInfo{},
			NewError(ErrOperationFailed,
				"error in ExtKeyInfo[%s] %s", keyHex, err.Error())
	}

	// CQL's write time is in micro-seconds from epoch.
	// convert it to golang's time.Time.
	info.WriteTime = time.Unix(
		writeTime/int64(time.Second/time.Microsecond), 0).UTC()

	return info, nil
}
