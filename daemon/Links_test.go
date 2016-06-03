// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test operations on hardlinks and symlinks

import "os"
import "syscall"
import "testing"

func TestHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		file1 := workspace + "/orig_file"
		fd, err := os.Create(test.relPath(file1))
		test.assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		file2 := workspace + "/hardlink"
		err = syscall.Link(test.relPath(file1), test.relPath(file2))
		test.assert(err != nil, "Expected hardlink to fail")
		test.assert(err == syscall.EPERM, "Expected EPERM error: %v", err)
	})
}

func TestSymlinkCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		err := syscall.Symlink("/usr/bin/arch", test.relPath(link))
		test.assert(err == nil, "Error creating symlink: %v", err)
	})
}

func TestReadlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		orig := "/usr/bin/arch"
		err := syscall.Symlink(orig, test.relPath(link))
		test.assert(err == nil, "Error creating symlink: %v", err)

		path, err := os.Readlink(test.relPath(link))
		test.assert(err == nil, "Error reading symlink: %v", err)
		test.assert(path == orig, "Path does not match '%s' != '%s'",
			orig, path)
	})
}
