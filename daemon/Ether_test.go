// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Smoke tests for the cql datastores

import (
	"testing"
)

func TestSmokeTestCqlFilesystem(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		test.startQuantumFs(test.cqlFilesystemConfig(), nil, false)
		interDirectoryRename(test)
	})
}
