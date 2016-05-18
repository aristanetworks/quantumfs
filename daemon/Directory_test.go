// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on directories, such as creation and traversing

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
