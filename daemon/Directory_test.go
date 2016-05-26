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
		expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
			syscall.S_IRWXO
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryFileDescriptorDirtying_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		// Create a file and determine its inode numbers
		workspace := quantumfs.NullNamespaceName + "/" +
			quantumfs.NullWorkspaceName
		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/" + "test"

		err := os.MkdirAll(test.relPath(dirName), 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(test.relPath(testFilename), 0124)
		test.assert(err == nil, "Error creating file: %v", err)
		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err == nil, "Error stat'ind test file: %v", err)
		test.assert(stat.Ino >= quantumfs.InodeIdReservedEnd,
			"File had reserved inode number %d", stat.Ino)

		// Find the matching FileHandle
		descriptors := test.fileDescriptorFromInodeNum(stat.Ino)
		test.assert(len(descriptors) == 1,
			"Incorrect number of fds found 1 != %d", len(descriptors))
		fileDescriptor := descriptors[0]
		file := fileDescriptor.file

		// Save the workspace rootId, change the File key, simulating
		// changing the data, then mark the matching FileDescriptor dirty.
		// This should trigger a refresh up the hierarchy and, because we
		// currently do not support delayed syncing, change the workspace
		// rootId and mark the fileDescriptor clean.
		oldRootId := test.workspaceRootId(quantumfs.NullNamespaceName,
			quantumfs.NullWorkspaceName)

		file.key.Key[1]++
		fileDescriptor.dirty(test.newCtx())

		newRootId := test.workspaceRootId(quantumfs.NullNamespaceName,
			quantumfs.NullWorkspaceName)

		test.assert(oldRootId != newRootId, "Workspace rootId didn't change")
		test.assert(!file.dirty_, "FileDescriptor not cleaned after"+
			"change")

		syscall.Close(fd)
	})
}

// If we modify a Directory we do not ever reload the data from the datastore. In
// order to confirm the data has been written correctly we need to branch the
// workspace after modifying the directory and confirm the newly loaded directory
// contains the changes in the update.
func TestDirectoryUpdate_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		src := quantumfs.NullNamespaceName + "/" +
			quantumfs.NullWorkspaceName
		dst := "dirupdate/test"

		// First create a file
		testFilename := src + "/" + "test"
		fd, err := os.Create(test.relPath(testFilename))
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		// Then branch the workspace
		err = api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Ensure the new workspace has the correct file
		testFilename = dst + "/" + "test"
		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err == nil, "Workspace copy doesn't match")
	})
}
