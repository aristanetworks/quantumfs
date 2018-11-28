// +build never

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Smoke tests for the systemlocal WorkspaceDB

import (
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/backends/processlocal"
	"github.com/aristanetworks/quantumfs/backends/systemlocal"
)

func (th *testHelper) systemlocalConfig() QuantumFsConfig {
	mountPath := th.tempDir + "/mnt"

	th.log("Starting initialization of workspaceDB")
	workspaceDbPath := th.tempDir + "/workspaceDB"
	workspaceDB := systemlocal.NewWorkspaceDB(workspaceDbPath)
	th.log("Finished initialization of workspaceDB")

	th.log("Starting initialization of datastore")
	datastorePath := th.tempDir + "/datastore"
	datastore := systemlocal.NewDatastore(datastorePath)
	th.log("Finished initialization of datastore")

	config := QuantumFsConfig{
		CachePath:        th.tempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  30 * time.Second,
		MountPath:        mountPath,
		WorkspaceDB:      workspaceDB,
		DurableStore:     datastore,
	}
	return config
}

func TestSmokeTestSystemlocal(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		test.startQuantumFs(test.systemlocalConfig())
		interDirectoryRename(test)
	})
}
