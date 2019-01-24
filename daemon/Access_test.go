// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test that Access works in a variety of scenarios

import (
	"os"
	"runtime"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

func permTest(test *testHelper, filename string, modeCheck uint32,
	shouldPass bool) {

	err := syscall.Access(filename+"garbage", F_OK)
	test.Assert(err != nil, "Access on invalid file passes")

	err = syscall.Access(filename, modeCheck)
	if shouldPass {
		test.Assert(err == nil, "Access fails with permission")
	} else {
		test.Assert(err != nil,
			"Access doesn't fail when missing permission")
	}
}

func permTestSub(test *testHelper, filename string, modeCheck uint32,
	shouldPass bool, asRoot bool) {

	if !asRoot {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		err := syscall.Setreuid(99, -1)
		test.AssertNoErr(err)

		defer syscall.Setreuid(0, -1)
	}

	permTest(test, filename, modeCheck, shouldPass)
}

func accessTest(test *testHelper, filename string, shouldPass bool, asRoot bool) {

	syscall.Chmod(filename, 0000)
	// just tests if file exists, so should always pass
	permTestSub(test, filename, F_OK, true, asRoot)

	syscall.Chmod(filename, 0400)
	permTestSub(test, filename, R_OK, shouldPass, asRoot)

	syscall.Chmod(filename, 0200)
	permTestSub(test, filename, W_OK, shouldPass, asRoot)

	syscall.Chmod(filename, 0100)
	permTestSub(test, filename, X_OK, shouldPass, asRoot)
}

func accessTestBothUsers(test *testHelper, filename string) {

	err := os.Chown(filename, 100, 100)
	test.AssertNoErr(err)

	// first run the test as root
	accessTest(test, filename, true, true)

	// Now run it as a different user who may not have access
	accessTest(test, filename, false, false)

	// Now run it as the user who has access
	err = os.Chown(filename, 99, 0)
	test.AssertNoErr(err)
	accessTest(test, filename, true, false)
}

func TestAccessListFileInWsr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/testFile"
		err := testutils.PrintToFile(filename, string(GenData(1000)))
		test.AssertNoErr(err)

		accessTestBothUsers(test, filename)
	})
}

func TestAccessListFileSubdir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		filename := workspace + "/subdir/testFile"
		err := testutils.PrintToFile(filename, string(GenData(1000)))
		test.AssertNoErr(err)

		accessTestBothUsers(test, filename)
	})
}

func TestAccessListHardlinkInWsr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace + "/testFile"
		err := testutils.PrintToFile(filename, string(GenData(1000)))
		test.AssertNoErr(err)

		linkname := workspace + "/subdir/subsubdir/linkFile"
		err = syscall.Link(filename, linkname)

		accessTestBothUsers(test, linkname)
		accessTestBothUsers(test, filename)
	})
}

func TestAccessListHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace + "/subdir/testFile"
		err := testutils.PrintToFile(filename, string(GenData(1000)))
		test.AssertNoErr(err)

		linkname := workspace + "/subdir/subsubdir/linkFile"
		err = syscall.Link(filename, linkname)

		accessTestBothUsers(test, linkname)
		accessTestBothUsers(test, filename)
	})
}

func TestAccessSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace + "/subdir/testFile"
		err := testutils.PrintToFile(filename, string(GenData(1000)))
		test.AssertNoErr(err)

		linkname := workspace + "/subdir/subsubdir/linkFile"
		err = syscall.Symlink(filename, linkname)

		accessTestBothUsers(test, linkname)
		accessTestBothUsers(test, filename)
	})
}

func TestAccessDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		accessTestBothUsers(test, workspace+"/subdir")
	})
}

func TestRootAccessRegularFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		test.createFile(workspace, "testFile", 1000)
		filename := workspace + "/testFile"

		for _, perm := range []uint32{0000, 0666, 0402, 0642} {
			test.AssertNoErr(syscall.Chmod(filename, perm))
			permTest(test, filename, X_OK, false)
			permTest(test, filename, R_OK, true)
			permTest(test, filename, W_OK, true)
		}

		for _, perm := range []uint32{0001, 0100, 0777, 0700} {
			test.AssertNoErr(syscall.Chmod(filename, perm))
			permTest(test, filename, X_OK, true)
			permTest(test, filename, R_OK, true)
			permTest(test, filename, W_OK, true)
		}
	})
}

func TestRootAccessDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		dir := workspace + "/subdir"

		for _, perm := range []uint32{0000, 0666, 0402, 0642} {
			test.AssertNoErr(syscall.Chmod(dir, perm))
			permTest(test, dir, X_OK, true)
			permTest(test, dir, R_OK, true)
			permTest(test, dir, W_OK, true)
		}
	})
}
