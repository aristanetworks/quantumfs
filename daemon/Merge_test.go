// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test workspace merging

import "bytes"
import "io/ioutil"
import "os"
import "testing"

import "github.com/aristanetworks/quantumfs/testutils"

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

		data := string(GenData(1000))
		err = testutils.PrintToFile(fileA, data[:100])
		test.AssertNoErr(err)

		err = testutils.PrintToFile(fileB, data[:300])
		test.AssertNoErr(err)

		err = testutils.PrintToFile(fileC, data[:500])
		test.AssertNoErr(err)

		err = testutils.PrintToFile(fileC2, data[400:900])
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()

		api := test.getApi()
		err = api.Merge(test.RelPath(workspaceB), test.RelPath(workspaceA))
		test.AssertNoErr(err)

		readData, err := ioutil.ReadFile(fileA)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal([]byte(data[:100]), readData),
			"pre-existing file corrupted in merge")

		readData, err = ioutil.ReadFile(workspaceA + "/fileB")
		test.AssertNoErr(err)
		test.Assert(bytes.Equal([]byte(data[:300]), readData),
			"merged in file isn't correct")

		readData, err = ioutil.ReadFile(fileC)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal([]byte(data[400:900]), readData),
			"overwritten file during merge doesn't choose newer file")
	})
}
