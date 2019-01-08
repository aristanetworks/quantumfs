// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package server

// Test the grpc server

import (
	"testing"

	"github.com/aristanetworks/quantumfs/testutils"
)

func TestBasicLifeCycle(t *testing.T) {
	runTestWithQfsDaemon(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		workspaceName := test.RelPath(workspace)
		name := "testFile"
		filename := workspace + "/" + name

		test.AssertNoErr(testutils.PrintToFile(filename, "content"))
		test.SyncWorkspace(workspaceName)
		test.SyncAllWorkspaces()
	})
}
