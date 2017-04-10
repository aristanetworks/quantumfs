// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
)

// The JSON decoder, by default, doesn't unmarshal time.Duration from a
// string. The custom struct allows to setup an unmarshaller which uses
// time.ParseDuration
type ttlDuration struct {
	Duration time.Duration
}

func (d *ttlDuration) UnmarshalJSON(data []byte) error {
	var str string
	var dur time.Duration
	var err error
	if err = json.Unmarshal(data, &str); err != nil {
		return err
	}

	dur, err = time.ParseDuration(str)
	if err != nil {
		return err
	}

	d.Duration = dur
	return nil
}

type cqlAdapterConfig struct {
	// CqlTTLRefreshTime controls when a block's TTL is refreshed
	// A block's TTL is refreshed when its TTL is <= CqlTTLRefreshTime
	// ttlrefreshtime is a string accepted by
	// https://golang.org/pkg/time/#ParseDuration
	TTLRefreshTime ttlDuration `json:"ttlrefreshtime"`

	// CqlTTLRefreshValue is the time by which a block's TTL will
	// be advanced during TTL refresh.
	// When a block's TTL is refreshed, its new TTL is set as
	// CqlTTLRefreshValue
	// ttlrefreshvalue is a string accepted by
	// https://golang.org/pkg/time/#ParseDuration
	TTLRefreshValue ttlDuration `json:"ttlrefreshvalue"`

	// CqlTTLDefaultValue is the TTL value of a new block
	// When a block is written its TTL is set to
	// CqlTTLDefaultValue
	// ttldefaultvalue is a string accepted by
	// https://golang.org/pkg/time/#ParseDuration
	TTLDefaultValue ttlDuration `json:"ttldefaultvalue"`
}

var refreshTTLTimeSecs int64
var refreshTTLValueSecs int64
var defaultTTLValueSecs int64

func loadCqlAdapterConfig(path string) error {
	var c struct {
		A cqlAdapterConfig `json:"adapter"`
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error in opening cql config file: %v\n(%v)", path, err)
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&c)
	if err != nil {
		return fmt.Errorf("error in decoding cql config file: %v\n(%v)", path, err)
	}

	refreshTTLTimeSecs = int64(c.A.TTLRefreshTime.Duration.Seconds())
	refreshTTLValueSecs = int64(c.A.TTLRefreshValue.Duration.Seconds())
	defaultTTLValueSecs = int64(c.A.TTLDefaultValue.Duration.Seconds())

	if refreshTTLTimeSecs == 0 || refreshTTLValueSecs == 0 ||
		defaultTTLValueSecs == 0 {
		return fmt.Errorf("ttldefaultvalue, ttlrefreshvalue and " +
			"ttlrefreshtime must be non-zero")
	}

	// we can add more checks here later on eg: min of 1 day etc

	return nil
}

// asserts that metadata is !nil and it contains cql.TimeToLive
// once Metadata API is refactored, above assertions will be
// revisited

// TTL will be set using Insert when
//  key exists and current TTL < refreshTTLTimeSecs
func refreshTTL(b blobstore.BlobStore, key string,
	metadata map[string]string) error {

	setTTL := defaultTTLValueSecs

	if metadata == nil {
		return fmt.Errorf("Store must have metadata")
	}
	ttl, ok := metadata[cql.TimeToLive]
	if !ok {
		return fmt.Errorf("Store must return metadata with " +
			"TimeToLive")
	}
	ttlVal, err := strconv.ParseInt(ttl, 10, 64)
	if err != nil {
		return fmt.Errorf("Invalid TTL value in metadata %s ",
			ttl)
	}

	// if key exists and TTL doesn't need to be refreshed
	// then return
	if ttlVal >= refreshTTLTimeSecs {
		return nil
	}
	// if key exists but TTL needs to be refreshed then
	// calculate new TTL.
	setTTL = refreshTTLValueSecs

	// if key doesn't exist then use default TTL and Insert
	buf, _, err := b.Get(key)
	if err != nil {
		return fmt.Errorf("Err if blobstore.Get() for key %v",
			key)

	}
	newmetadata := make(map[string]string)
	newmetadata[cql.TimeToLive] = fmt.Sprintf("%d", setTTL)
	return b.Insert(key, buf, newmetadata)
}

// TODO(sid): Change this to push blobStoreTranslator
// as RawEtherBlobStoreTranslator into
// quantumfs/thirdparty_backends/ether.go
type blobStoreTranslator struct {
	blobstore blobstore.BlobStore
}

func getDataStore(db blobstore.BlobStore) quantumfs.DataStore {

	translator := blobStoreTranslator{blobstore: db}
	return &translator
}

func (ebt *blobStoreTranslator) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	data, _, err := ebt.blobstore.Get(key.String())
	if err != nil {
		return err
	}
	buf.Set(data, key.Type())
	return nil
}

func (ebt *blobStoreTranslator) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return ebt.blobstore.Insert(key.String(), buf.Get(), nil)
}

func humanizeBytes(size uint64) string {

	suffix := []string{"B", "KB", "MB", "GB"}

	f := float64(size)
	var i int
	for i = 0; f >= 1024 && i < len(suffix); i++ {
		f = f / 1024
	}

	if i == len(suffix) {
		i = i - 1
	}

	return fmt.Sprintf("%.1f %s", f, suffix[i])
}

func newCtx() *quantumfs.Ctx {
	// Create  Ctx with random RequestId
	Qlog := qlog.NewQlogTiny()
	requestID := uint64(1)
	ctx := &quantumfs.Ctx{
		Qlog:      Qlog,
		RequestId: requestID,
	}

	return ctx
}

func getWorkspaceRootID(c *quantumfs.Ctx, db quantumfs.WorkspaceDB,
	wsname string) (quantumfs.ObjectKey, error) {

	if wsname == "" {
		return quantumfs.ObjectKey{}, fmt.Errorf("Invalid workspace name")
	}

	parts := strings.Split(wsname, "/")
	if len(parts) != 3 {
		return quantumfs.ObjectKey{}, fmt.Errorf("Invalid workspace name")
	}

	return db.Workspace(c, parts[0], parts[1], parts[2])
}
