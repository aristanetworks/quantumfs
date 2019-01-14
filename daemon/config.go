// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Central configuration object and handling

package daemon

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
)

// JSON encodable time.Duration wrapper
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	if data[0] == '"' {
		sd := string(data[1 : len(data)-1])
		var err error
		d.Duration, err = time.ParseDuration(sd)
		return err
	}

	id, err := json.Number(string(data)).Int64()
	if err != nil {
		return err
	}
	d.Duration = time.Duration(id)

	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", d.String())), nil
}

type QuantumFsConfig struct {
	CachePath string
	CacheSize uint64
	MountPath string

	ErrorExec string

	DataStoreName string
	DataStoreConf string

	WorkspaceDbName string
	WorkspaceDbConf string

	// How long the kernel is allowed to cache values
	CacheTimeSeconds uint64
	CacheTimeNsecs   uint32

	// How long before dirty data should be flushed
	DirtyFlushDelay Duration

	// How many bytes to allocate to the shared memory logs
	MemLogBytes uint64

	WorkspaceDB  quantumfs.WorkspaceDB
	DurableStore quantumfs.DataStore

	// Whether to do verbose tracing or not. This adds ~20% overhead, but makes
	// debugging _much_ easier.
	VerboseTracing bool

	// Should magical user ownership be used? Magical ownership causes all files
	// and directories owned by a non-system ID to appear as owned by the current
	// user. This adds some overhead to open heavy workloads.
	MagicOwnership bool

	DisableLockChecks bool
}
