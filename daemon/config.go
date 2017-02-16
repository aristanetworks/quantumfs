// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Central configuration object and handling

package daemon

import "time"

import "github.com/aristanetworks/quantumfs"

type QuantumFsConfig struct {
	CachePath string
	CacheSize uint64
	MountPath string

	DataStoreName string
	DataStoreConf string

	WorkspaceDbName string
	WorkspaceDbConf string

	// How long the kernel is allowed to cache values
	CacheTimeSeconds uint64
	CacheTimeNsecs   uint32

	// How long before dirty data should be flushed
	DirtyFlushDelay time.Duration

	// How many bytes to allocate to the shared memory logs
	MemLogBytes uint64

	WorkspaceDB  quantumfs.WorkspaceDB
	DurableStore quantumfs.DataStore
}
