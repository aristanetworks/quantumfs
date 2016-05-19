// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on directories, such as creation and traversing

import "os"
import "syscall"
import "testing"

import "arista.com/quantumfs"

func TestDirectoryCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := quantumfs.NullNamespaceName + "/" +
			quantumfs.NullWorkspaceName
		testFilename := workspace + "/" + "test"
		err := syscall.Mkdir(test.relPath(testFilename), 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err == nil, "Error stat'ing test dir: %v", err)
		test.assert(stat.Size == qfsBlockSize, "Incorrect Size: %d",
			stat.Size)
		test.assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFDIR
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := quantumfs.NullNamespaceName + "/" +
			quantumfs.NullWorkspaceName
		dirName := workspace + "/test/a/b"
		err := os.MkdirAll(test.relPath(dirName), 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(dirName), &stat)
		test.assert(err == nil, "Error stat'ing test dir: %v", err)
		test.assert(stat.Size == qfsBlockSize, "Incorrect Size: %d",
			stat.Size)
		test.assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFDIR
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryFileCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := quantumfs.NullNamespaceName + "/" +
			quantumfs.NullWorkspaceName
		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/c"

		err := os.MkdirAll(test.relPath(dirName), 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(test.relPath(testFilename), 0124)
		test.assert(err == nil, "Error creating file: %v", err)

		err = syscall.Close(fd)
		test.assert(err == nil, "Error closing fd: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)
		test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG | syscall.S_IRWXO
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}
