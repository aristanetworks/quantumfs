// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on special files, primarily creation

import "syscall"
import "testing"

func specialCreate(test *testHelper, filetype uint32) {
	test.startDefaultQuantumFs()

	workspace := test.newWorkspace()
	testFilename := workspace + "/" + "test"
	err := syscall.Mknod(testFilename, filetype|syscall.S_IRWXU,
		0x12345678)
	test.assert(err == nil, "Error creating node: %v", err)

	confirm := func(workspace string) {
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)
		test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.assert(stat.Nlink == 1, "Incorrect Nlink: %d",
			stat.Nlink)

		if filetype == syscall.S_IFBLK || filetype == syscall.S_IFCHR {
			test.assert(stat.Rdev == 0x12345678,
				"Node rdev incorrect %x", stat.Rdev)
		} else {
			test.assert(stat.Rdev == 0, "Node rdev incorrectly set %x",
				stat.Rdev)
		}

		var expectedPermissions uint32
		expectedPermissions |= filetype
		expectedPermissions |= syscall.S_IRWXU
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	}

	confirm(workspace)

	// Branch and confirm everything is still correct
	workspace = test.absPath(test.branchWorkspace(workspace))
	confirm(workspace)
}

func TestBlockDevCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFBLK)
	})
}

func TestCharDevCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFCHR)
	})
}

func TestSocketCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFSOCK)
	})
}

func TestFileMknodCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreate(test, syscall.S_IFREG)
	})
}

func specialCreateFail(test *testHelper, filetype uint32) {
	test.startDefaultQuantumFs()

	workspace := test.newWorkspace()
	testFilename := workspace + "/" + "test"
	err := syscall.Mknod(testFilename, filetype|syscall.S_IRWXU,
		0x12345678)
	test.assert(err != nil, "Unexpected success creating node")
}

func TestDirectoryMknodCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreateFail(test, syscall.S_IFDIR)
	})
}

func TestSymlinkMknodCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		specialCreateFail(test, syscall.S_IFLNK)
	})
}
