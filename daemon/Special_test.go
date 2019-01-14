// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test the various operations on special files, primarily creation

import (
	"syscall"
	"testing"
)

func specialCreate(test *testHelper, filetype uint32) {
	workspace := test.NewWorkspace()
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

	test.AssertNoErr(syscall.Chmod(testFilename, syscall.S_IRWXG))

	var expectedPermissions uint32
	expectedPermissions |= filetype
	expectedPermissions |= syscall.S_IRWXG
	var stat syscall.Stat_t
	test.AssertNoErr(syscall.Stat(testFilename, &stat))
	test.Assert(stat.Mode == expectedPermissions,
		"File permissions incorrect. Expected %x got %x",
		expectedPermissions, stat.Mode)

	test.AssertNoErr(syscall.Chmod(testFilename, syscall.S_IRWXU))

	// Ensure hardlinks work too
	testLinkname := testFilename + "_link"
	err = syscall.Link(testFilename, testLinkname)
	test.AssertNoErr(err)
	confirm(testLinkname, 2)

	// Branch and confirm everything is still correct
	workspace = test.AbsPath(test.branchWorkspace(workspace))
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
	workspace := test.NewWorkspace()
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

func TestCharDevXattrs(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testfile := "test"
		attr := "user.data"

		workspace := test.NewWorkspace()
		fullname := workspace + "/" + "test"
		test.AssertNoErr(syscall.Mknod(fullname,
			syscall.S_IFCHR|syscall.S_IRWXU, 0x12345678))
		test.verifyNoXattr(workspace, testfile, attr)
		var data []byte
		sz, err := syscall.Listxattr(workspace+"/test", data)
		test.AssertNoErr(err)
		test.Assert(sz == 0, "size of xattrs is %d", sz)
	})
}
