// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Smoke tests for the Ether datastores

import "testing"

func TestSmokeTestEtherFilesystem(t *testing.T) {
	t.Skip("ether.filesystem often take more than 200ms to write a block")
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		test.startQuantumFs(test.etherFilesystemConfig())
		interDirectoryRename(test)
	})
}
