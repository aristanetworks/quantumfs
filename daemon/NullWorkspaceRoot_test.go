// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on type NullWorkspaceRoot, whose only instance is
// _null/null, creating any type of file or directory under this path is enforced
// to be prohibited.

import "os"
import "syscall"
import "testing"

func TestNullWorkspaceDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"

		err := syscall.Mkdir(testFilename, 0124)
		test.assert(err == syscall.EPERM,
			"Unexpected success creating directory in null")
	})
}

func TestNullWorkspaceFileCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"

		fd, err := syscall.Creat(testFilename, 0124)
		test.assert(err == syscall.EPERM,
			"Unexpected success creating file in null")
		test.assert(fd == -1,
			"Unexpected valid filedescriptor in null")
	})
}

// This function is implemented to facilitate the creation of different types of
// special files
func NullWorkspaceSpecialFile(test *testHelper, filetype uint32) {
	workspace := test.nullWorkspace()
	testFilename := workspace + "/" + "test"
	err := syscall.Mknod(testFilename, filetype|syscall.S_IRWXU,
		0x12345678)
	test.assert(err == syscall.EPERM,
		"Unexpected success creating special file in null")
}

func TestNullWorkspaceBlockDevCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		NullWorkspaceSpecialFile(test, syscall.S_IFBLK)
	})
}

func TestNullWorkspaceCharDevCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		NullWorkspaceSpecialFile(test, syscall.S_IFCHR)
	})
}

func TestNullWorkspaceSocketCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		NullWorkspaceSpecialFile(test, syscall.S_IFSOCK)
	})
}

func TestNullWorkspaceFileMknodCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		NullWorkspaceSpecialFile(test, syscall.S_IFREG)
	})
}

func TestNullWorkspaceFileSymlinkCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.nullWorkspace()
		link := workspace + "/symlink"
		err := syscall.Symlink("/usr/bin/arch", link)
		test.assert(err == syscall.EPERM,
			"Unexpected success creating symlink in null")
	})
}

func TestNullWorkspaceHardlinkCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFileName := workspace + "/testfile"
		fd, err := os.Create(testFileName)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		nullworkspace := test.nullWorkspace()
		testLinkName := nullworkspace + "/testlink"
		err = syscall.Link(testFileName, testLinkName)
		test.assert(err == syscall.EPERM,
			"Unexpected success creating hardlink in null")
	})
}

func TestNullWorkspaceRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFileName := workspace + "/testfile"
		fd, err := os.Create(testFileName)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		testDirName := workspace + "/testdir"
		err = syscall.Mkdir(testDirName, 0124)
		test.assert(err == nil, "Error creating test directory: %v", err)

		nullworkspace := test.nullWorkspace()
		nullTestFileName := nullworkspace + "/testfile"
		err = syscall.Rename(testFileName, nullTestFileName)
		test.assert(err == syscall.EPERM,
			"Unexpected success moving file into null")

		nullTestDirName := nullworkspace + "/testdir"
		err = syscall.Rename(testDirName, nullTestDirName)
		test.assert(err == syscall.EPERM,
			"Unexpected success moving directory into null")
	})
}
