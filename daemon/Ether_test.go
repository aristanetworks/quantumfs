// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Smoke tests for the Ether datastores

import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/thirdparty_backends"

func (th *testHelper) etherFilesystemConfig() QuantumFsConfig {
	mountPath := th.TempDir + "/mnt"

	datastorePath := th.TempDir + "/ether"
	datastore, err := thirdparty_backends.ConnectDatastore("ether.filesystem",
		datastorePath)
	th.AssertNoErr(err)

	config := QuantumFsConfig{
		CachePath:        th.TempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  30 * time.Second,
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(""),
		DurableStore:     datastore,
	}
	return config
}

func TestSmokeTestEtherFilesystem(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		test.startQuantumFs(test.etherFilesystemConfig())
		interDirectoryRename(test)
	})
}
