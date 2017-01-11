// +build never

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Smoke tests for the systemlocal WorkspaceDB

import "testing"

import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/systemlocal"

func (th *testHelper) systemlocalConfig() QuantumFsConfig {
	mountPath := th.tempDir + "/mnt"

	workspaceDbPath := th.tempDir + "/workspaceDB"

	th.log("Starting initialization of workspaceDB")
	workspaceDB := systemlocal.NewWorkspaceDB(workspaceDbPath)
	th.log("Finished initialization of workspaceDB")

	config := QuantumFsConfig{
		CachePath:        th.tempDir + "/ramfs",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  30 * time.Second,
		MountPath:        mountPath,
		WorkspaceDB:      workspaceDB,
		DurableStore:     processlocal.NewDataStore(""),
	}
	return config
}

func TestSmokeTestSystemlocalWorkspaceDB(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		test.startQuantumFs(test.systemlocalConfig())
		interDirectoryRename(test)
	})
}
