// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Smoke tests for the systemlocal WorkspaceDB

import (
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/backends/systemlocal"
)

func (th *testHelper) systemlocalConfig() QuantumFsConfig {
	mountPath := th.TempDir + "/mnt"

	th.Log("Starting initialization of workspaceDB")
	workspaceDbPath := th.TempDir + "/workspaceDB"
	workspaceDB := systemlocal.NewWorkspaceDB(workspaceDbPath)
	th.Log("Finished initialization of workspaceDB")

	th.Log("Starting initialization of datastore")
	datastorePath := th.TempDir + "/datastore"
	datastore := systemlocal.NewDataStore(datastorePath)
	th.Log("Finished initialization of datastore")

	config := QuantumFsConfig{
		CachePath:        th.TempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  Duration{Duration: 30 * time.Second},
		MountPath:        mountPath,
		WorkspaceDB:      workspaceDB,
		DurableStore:     datastore,
	}
	return config
}

func TestSmokeTestSystemlocal(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		test.startQuantumFs(test.systemlocalConfig(), nil, false)
		interDirectoryRename(test)
	})
}
