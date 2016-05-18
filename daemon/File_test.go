// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on files such as creation, read and write

import "syscall"
import "testing"

import "arista.com/quantumfs"

func TestFileCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := quantumfs.NullNamespaceName + "/" + quantumfs.NullWorkspaceName
		testFilename := workspace + "/" + "test"
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

func TestFileDescriptorDirtying_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		// Create a file and determine its inode numbers
		workspace := quantumfs.NullNamespaceName + "/" + quantumfs.NullWorkspaceName
		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(test.relPath(testFilename), 0124)
		test.assert(err == nil, "Error creating file: %v", err)
		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err == nil, "Error stat'ind test file: %v", err)
		test.assert(stat.Ino >= quantumfs.InodeIdReservedEnd,
			"File had reserved inode number %d", stat.Ino)

		// Find the matching FileHandle
		descriptors := test.fileDescriptorFromInodeNum(stat.Ino)
		test.assert(len(descriptors) == 1, "Incorrect number of fds found 1 != %d",
			len(descriptors))
		fileDescriptor := descriptors[0]
		file := fileDescriptor.file

		// Save the workspace rootId, change the File key, simulating changing the
		// data, then mark the matching FileDescriptor dirty. This should trigger a
		// refresh up the hierarchy and, because we currently do not support delayed
		// syncing, change the workspace rootId and mark the fileDescriptor clean.
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
