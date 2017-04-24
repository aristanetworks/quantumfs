// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test parsing supplementary groups

import "os"
import "runtime"
import "syscall"
import "testing"

func TestGroupParsing(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer test.setGroups([]int{0, 30, 200})()

		pid := uint32(syscall.Gettid())

		// Sanity test that we fail to match when there is no match
		test.Assert(!hasMatchingGid(test.newCtx(), 99, pid, 1000),
			"No matching failed")

		// Match the passed in gid even if it isn't in the groups line
		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 99),
			"Matching failed")

		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 0),
			"Matching failed")

		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 30),
			"Matching failed")

		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 200),
			"Matching failed")
	})
}

func TestSupplementaryGroupFileAccess(t *testing.T) {
	// Test BUG197678
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		dirName := workspace + "/dir"
		fileName := dirName + "/file"

		err := os.MkdirAll(dirName, 0777)
		test.AssertNoErr(err)

		file, err := os.Create(fileName)
		test.AssertNoErr(err)
		err = file.Chown(0, 200)
		test.AssertNoErr(err)
		file.Close()

		defer test.setGroups([]int{10, 200, 300})()

		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

		test.AssertNoErr(syscall.Access(fileName, R_OK))
	})
}

func TestSupplementaryGroupFileRead(t *testing.T) {
	// Test BUG197678
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		dirName := workspace + "/dir"
		fileName := dirName + "/file"

		err := os.MkdirAll(dirName, 0777)
		test.AssertNoErr(err)

		file, err := os.Create(fileName)
		test.AssertNoErr(err)
		err = file.Chown(0, 200)
		test.AssertNoErr(err)
		file.Close()

		defer test.setGroups([]int{10, 200, 300})()

		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

		file, err = os.Open(fileName)
		test.AssertNoErr(err)
		file.Close()
	})
}
