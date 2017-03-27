// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that Access works in a variety of scenarios
import "os"
import "os/user"
import "syscall"
import "testing"

func permTest(test *testHelper, filename string, modeCheck uint32) {
	err := syscall.Access(filename + "garbage", F_OK)
	test.assert(err != nil, "Invalid file exists")

	userInfo, err := user.Current()
	test.assertNoErr(err)
	isRoot := (userInfo.Uid == "0")

	err = syscall.Access(filename, R_OK)
	if modeCheck & R_OK != 0 || isRoot {
		test.assertNoErr(err)
	} else {
		test.assert(err != nil,
			"Access doesn't fail when read isn't permitted")
	}

	err = syscall.Access(filename, W_OK)
	if modeCheck & W_OK != 0 || isRoot {
		test.assertNoErr(err)
	} else {
		test.assert(err != nil,
			"Access doesn't fail when write isn't permitted")
	}

	err = syscall.Access(filename, X_OK)
	if modeCheck & X_OK != 0 || isRoot {
		test.assertNoErr(err)
	} else {
		test.assert(err != nil,
			"Access doesn't fail when execute isn't permitted")
	}
}

func accessTest(test *testHelper, filename string) {
	// clear all permissions
	syscall.Chmod(filename, 0000)
	permTest(test, filename, 0000)

	syscall.Chmod(filename, 0444)
	permTest(test, filename, 0444)

	syscall.Chmod(filename, 0222)
	permTest(test, filename, 0222)

	syscall.Chmod(filename, 0111)
	permTest(test, filename, 0111)
}

func TestAccessListFileInWsr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		filename := workspace +"/testFile"
		err := printToFile(filename, string(genData(1000)))
		test.assertNoErr(err)

		accessTest(test, filename)
	})
}

func TestAccessListFileSubdir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir", 0777)

		filename := workspace +"/subdir/testFile"
		err := printToFile(filename, string(genData(1000)))
		test.assertNoErr(err)

		accessTest(test, filename)
	})
}

func TestAccessListHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace +"/subdir/testFile"
		err := printToFile(filename, string(genData(1000)))
		test.assertNoErr(err)

		linkname := workspace +"/subdir/subsubdir/linkFile"
		err = syscall.Link(filename, linkname)

		accessTest(test, linkname)
		accessTest(test, filename)
	})
}

func TestAccessSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace +"/subdir/testFile"
		err := printToFile(filename, string(genData(1000)))
		test.assertNoErr(err)

		linkname := workspace +"/subdir/subsubdir/linkFile"
		err = syscall.Symlink(filename, linkname)

		accessTest(test, linkname)
		accessTest(test, filename)
	})
}

func TestAccessDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir", 0777)

		accessTest(test, workspace+"/subdir")
	})
}
