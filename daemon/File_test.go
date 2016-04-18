// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on files such as creation, read and write

import "syscall"
import "testing"

import "arista.com/quantumfs"

func TestFileCreation_test(t *testing.T) {
	test := startTest(t)
	test.startDefaultQuantumFs()

	workspace := quantumfs.NullNamespaceName + "/" + quantumfs.NullWorkspaceName
	testFilename := workspace + "/" + "test"
	fd, err := syscall.Creat(test.relPath(testFilename), 0124)
	test.assert(err != nil, "Error creating file: %v", err)

	err = syscall.Close(fd)
	test.assert(err != nil, "Error closing fd: %v", err)

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

	test.endTest()
}
