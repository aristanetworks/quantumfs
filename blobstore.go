// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aristanetworks/ether"
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

// cqlBlobStore implements both blobstore.BlobStore interface
// CqlStore interface.
type cqlBlobStore struct {
	store    *cqlStore
	keyspace string
	cfName   string

	insertStats stats.OpStats
	getStats    stats.OpStats
}

func newCqlBS(cluster Cluster, cfg *Config) (blobstore.BlobStore, error) {
	store, err := initCqlStore(cluster)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed,
			"error in initializing cql store %s", err.Error())
	}

	bsName, _ := prefixToTblNames(os.Getenv("CFNAME_PREFIX"))
	if err := isTablePresent(&store, cfg, bsName); err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed, "%s", err.Error())
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

// NewCqlBlobStore initializes a blobstore.BlobStore to be used with a CQL cluster.
func NewCqlBlobStore(confName string) (blobstore.BlobStore, error) {
	cfg, err := readCqlConfig(confName)
	if err != nil {
		return nil, blobstore.NewError(blobstore.ErrOperationFailed,
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

// Insert is the CQL implementation of blobstore.Insert()
func (b *cqlBlobStore) Insert(c ether.Ctx, key []byte, value []byte,
	metadata map[string]string) error {
	keyHex := hex.EncodeToString(key)
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
		return blobstore.NewError(blobstore.ErrOperationFailed,
			"error in Insert[%s] %s", keyHex, err.Error())
	}
	return nil
}

// GetLog can be used in external tool for log parsing
const GetLog = "Cql::Get"

// GoCqlGetLog can be used in external tool for log parsing
const GoCqlGetLog = "GoCql::Get"

// KeyLog can be used in external tool for log parsing
const KeyLog = "Key: %s"

// Get is the CQL implementation of blobstore.Get()
func (b *cqlBlobStore) Get(c ether.Ctx, key []byte) ([]byte, map[string]string, error) {
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
			return nil, nil, blobstore.NewError(blobstore.ErrKeyNotFound, "error Get[%s] %s",
				keyHex, err.Error())
		}
		return nil, nil, blobstore.NewError(blobstore.ErrOperationFailed, "error in Get[%s] %s",
			keyHex, err.Error())
	}

	mdata := make(map[string]string)
	mdata[TimeToLive] = strconv.Itoa(ttl)
	c.Vlog(GetLog+" "+KeyTTLLog, keyHex, mdata[TimeToLive])
	return value, mdata, nil
}

// DeleteLog can be used in external tool for log parsing
const DeleteLog = "Cql::Delete"

// Delete is the CQL implementation of blobstore.Delete()
func (b *cqlBlobStore) Delete(c ether.Ctx, key []byte) error {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(DeleteLog, KeyLog, keyHex).Out()
	return blobstore.NewError(blobstore.ErrOperationFailed,
		"Delete operation is not implemented")
}

// MetadataLog can be used in external tool for log parsing
const MetadataLog = "Cql::Metadata"

// GoCqlMetadataLog can be used in external tool for log parsing
const GoCqlMetadataLog = "GoCql::Metadata"

// Metadata is the CQL implementation of blobstore.Metadata()
// Note: retreiving this information does not have performance overhead
func (b *cqlBlobStore) Metadata(c ether.Ctx, key []byte) (map[string]string, error) {
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
			return nil, blobstore.NewError(blobstore.ErrKeyNotFound,
				"error Metadata[%s] %s", keyHex, err.Error())
		}
		return nil, blobstore.NewError(blobstore.ErrOperationFailed,
			"error in Metadata[%s] %s", keyHex, err.Error())
	}
	mdata := make(map[string]string)
	mdata[TimeToLive] = strconv.Itoa(ttl)
	c.Vlog(MetadataLog+" "+KeyTTLLog, keyHex, mdata[TimeToLive])
	return mdata, nil
}

// UpdateLog can be used in external tool for log parsing
const UpdateLog = "Cql::Update"

// Update is the CQL implementation of blobstore.Update()
func (b *cqlBlobStore) Update(c ether.Ctx, key []byte, metadata map[string]string) error {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn(UpdateLog, KeyLog, keyHex).Out()
	return blobstore.NewError(blobstore.ErrOperationFailed,
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
func (b *cqlBlobStore) GetExtKeyInfo(c ether.Ctx,
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
				blobstore.NewError(blobstore.ErrKeyNotFound,
					"error ExtKeyInfo[%s] %s", keyHex,
					err.Error())
		}
		return ExtKeyInfo{},
			blobstore.NewError(blobstore.ErrOperationFailed,
				"error in ExtKeyInfo[%s] %s", keyHex, err.Error())
	}

	// CQL's write time is in micro-seconds from epoch.
	// convert it to golang's time.Time.
	info.WriteTime = time.Unix(
		writeTime/int64(time.Second/time.Microsecond), 0).UTC()

	return info, nil
}
