// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test the various operations on type NullWorkspaceRoot, whose only instance is
// _null/null, creating any type of file or directory under this path is enforced
// to be prohibited.

import (
	"os"
	"syscall"
	"testing"
)

func TestNullWorkspaceDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"

		err := syscall.Mkdir(testFilename, 0124)
		test.Assert(err == syscall.EROFS,
			"Unexpected success creating directory in null")
	})
}

func TestNullWorkspaceFileCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"

		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == syscall.EROFS,
			"Unexpected success creating file in null")
		test.Assert(fd == -1,
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
	test.Assert(err == syscall.EROFS,
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
		test.Assert(err == syscall.EROFS,
			"Unexpected success creating symlink in null")
	})
}

func TestNullWorkspaceHardlinkCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFileName := workspace + "/testfile"
		fd, err := os.Create(testFileName)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		nullworkspace := test.nullWorkspace()
		testLinkName := nullworkspace + "/testlink"
		err = syscall.Link(testFileName, testLinkName)
		test.Assert(err == syscall.EROFS,
			"Unexpected success creating hardlink in null")
	})
}

func TestNullWorkspaceRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFileName := workspace + "/testfile"
		fd, err := os.Create(testFileName)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		testDirName := workspace + "/testdir"
		err = syscall.Mkdir(testDirName, 0124)
		test.Assert(err == nil, "Error creating test directory: %v", err)

		nullworkspace := test.nullWorkspace()
		nullTestFileName := nullworkspace + "/testfile"
		err = syscall.Rename(testFileName, nullTestFileName)
		test.Assert(err == syscall.EROFS,
			"Unexpected success moving file into null")

		nullTestDirName := nullworkspace + "/testdir"
		err = syscall.Rename(testDirName, nullTestDirName)
		test.Assert(err == syscall.EROFS,
			"Unexpected success moving directory into null")
	})
}
