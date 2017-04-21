// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
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

type cqlWalkerConfig struct {
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

func loadCqlWalkerConfig(path string) error {
	var c struct {
		A cqlWalkerConfig `json:"walker"`
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

	if refreshTTLTimeSecs == 0 || refreshTTLValueSecs == 0 {
		return fmt.Errorf("ttlrefreshvalue and ttlrefreshtime must be non-zero")
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

	buf, _, err := b.Get(key)
	if err != nil {
		return fmt.Errorf("Err if blobstore.Get() for key %v",
			key)

	}
	newmetadata := make(map[string]string)
	newmetadata[cql.TimeToLive] = fmt.Sprintf("%d", refreshTTLValueSecs)
	return b.Insert(key, buf, newmetadata)
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

// Given 2 maps m1 and m2, return the (numKeys, numSize) unique to m1 wrt m2
func mapCompare(m1 map[string]uint64, m2 map[string]uint64) (uint64, uint64) {

	var uniqueKey, uniqueSize uint64
	for k, size := range m1 {
		if _, seen := m2[k]; !seen {
			uniqueKey++
			uniqueSize += size
		}
	}
	return uniqueKey, uniqueSize
}

// A simple histogram impl.
type histogram struct {
	mapLock   utils.DeferableMutex
	keysMap   map[int64]int
	totalKeys uint64
}

func newHistogram() *histogram {
	return &histogram{
		keysMap: make(map[int64]int),
	}
}

func (h *histogram) Increment(idx int64) {
	defer h.mapLock.Lock().Unlock()
	h.keysMap[idx]++
	h.totalKeys++
}

func (h *histogram) Print() {

	m := h.keysMap
	var keys []int
	for k := range m {
		keys = append(keys, int(k))
	}
	fmt.Println()
	sort.Ints(keys)
	for _, k := range keys {
		fmt.Printf("%20v days(s) : %v\n", k, m[int64(k)])
	}
	fmt.Printf("%20v : %v\n", "Total Keys", h.totalKeys)
}

func showProgress(progress bool, start time.Time, totalKeys uint64) {

	if progress {
		fmt.Printf("\r %10v %v %20v %v",
			"Time", time.Since(start), "Keys Walked", totalKeys)
	}
}
