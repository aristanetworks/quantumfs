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
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"

		err := syscall.Mkdir(testFilename, 0124)
		test.assert(err == syscall.EPERM,
			"Unexpected success creating directory in null")

		err = os.Mkdir(testFilename, 0124)
		test.assert(err != nil,
			"Unexpected success creating directory in null")
	})
}

func TestNullWorkspaceRecursiveDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test/test"

		err := os.MkdirAll(testFilename, 0124)
		test.assert(err != nil,
			"Unexpected success creating recursive directory in null")
	})
}

func TestNullWorkspaceFileCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

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
	test.startDefaultQuantumFs()

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
