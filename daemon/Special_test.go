// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on special files, primarily creation

import "syscall"
import "testing"

func specialCreate(test *testHelper, filetype uint32) {
	workspace := test.newWorkspace()
	testFilename := workspace + "/" + "test"
	err := syscall.Mknod(testFilename, filetype|syscall.S_IRWXU,
		0x12345678)
	test.Assert(err == nil, "Error creating node: %v", err)

	confirm := func(filepath string, expectedNlink uint64) {
		var stat syscall.Stat_t
		err = syscall.Stat(filepath, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)

		test.Assert(stat.Nlink == expectedNlink, "Incorrect Nlink: %d",
			stat.Nlink)

		if filetype == syscall.S_IFBLK || filetype == syscall.S_IFCHR {
			test.Assert(stat.Rdev == 0x12345678,
				"Node rdev incorrect %x", stat.Rdev)
		} else {
			test.Assert(stat.Rdev == 0, "Node rdev incorrectly set %x",
				stat.Rdev)
		}

		var expectedPermissions uint32
		expectedPermissions |= filetype
		expectedPermissions |= syscall.S_IRWXU
		test.Assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	}

	confirm(testFilename, 1)

	// Ensure hardlinks work too
	testLinkname := testFilename + "_link"
	err = syscall.Link(testFilename, testLinkname)
	test.AssertNoErr(err)
	confirm(testLinkname, 2)

	// Branch and confirm everything is still correct
	workspace = test.absPath(test.branchWorkspace(workspace))
	confirm(workspace+"/test", 2)
	confirm(workspace+"/test_link", 2)
}

func TestBlockDevCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFBLK)
	})
}

func TestCharDevCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFCHR)
	})
}

func TestSocketCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFSOCK)
	})
}

func TestFileMknodCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFREG)
	})
}

func specialCreateFail(test *testHelper, filetype uint32) {
	workspace := test.newWorkspace()
	testFilename := workspace + "/" + "test"
	err := syscall.Mknod(testFilename, filetype|syscall.S_IRWXU,
		0x12345678)
	test.Assert(err != nil, "Unexpected success creating node")
}

func TestDirectoryMknodCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreateFail(test, syscall.S_IFDIR)
	})
}

func TestSymlinkMknodCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreateFail(test, syscall.S_IFLNK)
	})
}
