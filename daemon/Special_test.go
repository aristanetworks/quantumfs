// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on special files, primarily creation

import "syscall"
import "testing"

func TestBlockDevCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		err := syscall.Mknod(testFilename, syscall.S_IFBLK|syscall.S_IRWXU,
			0x12345678)
		test.assert(err == nil, "Error creating block device: %v", err)

		confirm := func(workspace string) {
			var stat syscall.Stat_t
			err = syscall.Stat(testFilename, &stat)
			test.assert(err == nil, "Error stat'ing test file: %v", err)
			test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
			test.assert(stat.Nlink == 1, "Incorrect Nlink: %d",
				stat.Nlink)
			test.assert(stat.Rdev == 0x12345678,
				"Block rdev incorrect %x", stat.Rdev)

			var expectedPermissions uint32
			expectedPermissions |= syscall.S_IFBLK
			expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
				syscall.S_IRWXO
			test.assert(stat.Mode == expectedPermissions,
				"File permissions incorrect. Expected %x got %x",
				expectedPermissions, stat.Mode)
		}

		confirm(workspace)

		// Branch and confirm everything is still correct
		workspace = test.absPath(test.branchWorkspace(workspace))
		confirm(workspace)
	})
}

func TestCharDevCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		err := syscall.Mknod(testFilename, syscall.S_IFCHR|syscall.S_IRWXU,
			0x12345678)
		test.assert(err == nil, "Error creating char device: %v", err)

		confirm := func(workspace string) {
			var stat syscall.Stat_t
			err = syscall.Stat(testFilename, &stat)
			test.assert(err == nil, "Error stat'ing test file: %v", err)
			test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
			test.assert(stat.Nlink == 1, "Incorrect Nlink: %d",
				stat.Nlink)
			test.assert(stat.Rdev == 0x12345678,
				"Block rdev incorrect %x", stat.Rdev)

			var expectedPermissions uint32
			expectedPermissions |= syscall.S_IFCHR
			expectedPermissions |= syscall.S_IRWXU | syscall.S_IRWXG |
				syscall.S_IRWXO
			test.assert(stat.Mode == expectedPermissions,
				"File permissions incorrect. Expected %x got %x",
				expectedPermissions, stat.Mode)
		}

		confirm(workspace)

		// Branch and confirm everything is still correct
		workspace = test.absPath(test.branchWorkspace(workspace))
		confirm(workspace)
	})
}
