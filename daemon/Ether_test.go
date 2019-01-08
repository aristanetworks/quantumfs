// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

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
