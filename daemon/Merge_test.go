// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test workspace merging

import "os"
import "testing"


func TestBasicMerge(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspaceA := test.NewWorkspace()
		workspaceB := test.NewWorkspace()

		fileA := workspaceA + "/fileA"
		fileB := workspaceB + "/fileB"
		fileC := workspaceA + "/subdir/fileC"
		fileC2 := workspaceB + "/subdir/fileC"

		err := os.MkdirAll(workspaceA + "/subdir", 0777)
		test.AssertNoErr(err)
		err = os.MkdirAll(workspaceB + "/subdir", 0777)
		test.AssertNoErr(err)

		dataA := test.MakeFile(fileA)
		dataB := test.MakeFile(fileB)
		test.MakeFile(fileC)
		dataC2 := test.MakeFile(fileC2)

		test.SyncAllWorkspaces()

		api := test.getApi()
		err = api.Merge(test.RelPath(workspaceB), test.RelPath(workspaceA))
		test.AssertNoErr(err)

		test.CheckData(fileA, dataA)
		test.CheckData(workspaceA + "/fileB", dataB)
		test.CheckData(fileC, dataC2)
	})
}
