// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aristanetworks/quantumfs"
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
	// All blocks in the datastore are refreshed every TTLRefreshTime
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

	// TTLRefreshCacheLen is the maximum number of keys to hold in the
	// skip map when walking. This restricts how effectively the walker can
	// prevent re-walking duplicate blocks, and controls memory usage.
	TTLRefreshCacheLen int

	// CqlTTLDefaultValue is the TTL value of a new block
	// When a block is written its TTL is set to
	// CqlTTLDefaultValue
	// ttldefaultvalue is a string accepted by
	// https://golang.org/pkg/time/#ParseDuration
	TTLDefaultValue ttlDuration `json:"ttldefaultvalue"`
}

// TTLConfig hold the boundary TTL values to be used in the walker.
type TTLConfig struct {
	SkipMapResetAfter_ms int64
	SkipMapMaxLen        int
	TTLNew               int64
}

// LoadTTLConfig loaks the TTL config values for the
// Walker from the config file
func LoadTTLConfig(path string) (*TTLConfig, error) {
	var c struct {
		A cqlWalkerConfig `json:"walker"`
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error in opening cql config file: %v\n(%v)",
			path, err)
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&c)
	if err != nil {
		return nil, fmt.Errorf("error in decoding cql config file: %v\n(%v)",
			path, err)
	}

	var tc TTLConfig
	tc.SkipMapResetAfter_ms = int64(c.A.TTLRefreshTime.Duration.Seconds() * 1000)
	tc.SkipMapMaxLen = int(c.A.TTLRefreshCacheLen)
	tc.TTLNew = int64(c.A.TTLRefreshValue.Duration.Seconds())

	if tc.SkipMapResetAfter_ms == 0 || tc.TTLNew == 0 {
		return nil, fmt.Errorf("ttlrefreshvalue and ttlrefreshtime must " +
			"be non-zero")
	}

	// we can add more checks here later on eg: min of 1 day etc

	return &tc, nil
}

// HumanizeBytes print the size with appropriate suffix.
func HumanizeBytes(size uint64) string {

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

// GetWorkspaceRootID return the quantumfs key for the given workspace name.
func GetWorkspaceRootID(c *quantumfs.Ctx, db quantumfs.WorkspaceDB,
	wsname string) (quantumfs.ObjectKey, quantumfs.WorkspaceNonce, error) {

	invalidNonce := quantumfs.WorkspaceNonce{
		Id:          0,
		PublishTime: 0}

	if wsname == "" {
		return quantumfs.ObjectKey{}, invalidNonce,
			fmt.Errorf("Invalid workspace name")
	}

	parts := strings.Split(wsname, "/")
	if len(parts) != 3 {
		return quantumfs.ObjectKey{},
			invalidNonce, fmt.Errorf("Invalid workspace name")
	}

	return db.Workspace(c, parts[0], parts[1], parts[2])
}

// Histogram is a simple Histogram impl.
type Histogram struct {
	mapLock     utils.DeferableMutex
	buckets     map[string]uint64
	totalValues uint64
}

// NewHistogram return a new Histogram object.
func NewHistogram() *Histogram {
	return &Histogram{
		buckets: make(map[string]uint64),
	}
}

// Increment the count in the given index.
func (h *Histogram) Increment(bucket string) {
	defer h.mapLock.Lock().Unlock()
	h.buckets[bucket]++
	h.totalValues++
}

// Add to the count in the given index.
func (h *Histogram) Add(bucket string, delta uint64) {
	defer h.mapLock.Lock().Unlock()
	h.buckets[bucket] += delta
	h.totalValues += delta
}

// Print the given Histogram
func (h *Histogram) Print() {

	defer h.mapLock.Lock().Unlock()
	m := h.buckets
	var bucketNames []string
	for name := range m {
		bucketNames = append(bucketNames, name)
	}
	sort.Strings(bucketNames)
	for _, name := range bucketNames {
		fmt.Printf("%s\t: %d (%.1f %%)\n", name, m[name],
			(float64(m[name])/float64(h.totalValues))*100.0)
	}
	fmt.Printf("Total Values : %d\n", h.totalValues)
}

// Bucket returns a refernece to the internal bucket
func (h *Histogram) Bucket() map[string]uint64 {
	return h.buckets
}
