// +build linux

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Smoke tests for the Ether datastores

import "testing"

import "github.com/aristanetworks/quantumfs/processlocal"
import "github.com/aristanetworks/quantumfs/thirdparty_backends"

func (th *testHelper) etherFilesystemConfig() QuantumFsConfig {
	mountPath := th.createTestDirs()

	datastorePath := th.tempDir + "/ether"

	config := QuantumFsConfig{
		CachePath:        "",
		CacheSize:        1 * 1024 * 1024,
		CacheTimeSeconds: 1,
		CacheTimeNsecs:   0,
		MountPath:        mountPath,
		WorkspaceDB:      processlocal.NewWorkspaceDB(),
		DurableStore:     thirdparty_backends.NewEtherFilesystemStore(datastorePath),
	}
	return config
}

func TestSmokeTestEtherFilesystem(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startQuantumFs(test.etherFilesystemConfig())
		interDirectoryRename(test)
	})
}
