// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test operations on hardlinks and symlinks

import "bytes"
import "io/ioutil"
import "os"
import "syscall"
import "testing"

func TestHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		testData := []byte("arstarst")

		workspace := test.newWorkspace()
		file1 := workspace + "/orig_file"
		err := ioutil.WriteFile(file1, testData, 0777)
		test.assert(err == nil, "Error creating file: %v", err)

		file2 := workspace + "/hardlink"
		err = syscall.Link(file1, file2)
		test.assert(err == nil, "Creating hardlink failed: %v", err)

		// Open the file to ensure we linked successfully
		data, err := ioutil.ReadFile(file2)
		test.assert(err == nil, "Error reading linked file: %v", err)
		test.assert(bytes.Equal(data, testData), "Data corrupt!")

		// Branch and confirm the hardlink is still there
		workspace = test.absPath(test.branchWorkspace(workspace))
		file2 = workspace + "/hardlink"
		data, err = ioutil.ReadFile(file2)
		test.assert(err == nil, "Error reading linked file: %v", err)
		test.assert(bytes.Equal(data, testData), "Data corrupt!")
	})
}

func TestSymlinkCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		err := syscall.Symlink("/usr/bin/arch", link)
		test.assert(err == nil, "Error creating symlink: %v", err)
	})
}

func TestReadlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		orig := "/usr/bin/arch"
		err := syscall.Symlink(orig, link)
		test.assert(err == nil, "Error creating symlink: %v", err)

		path, err := os.Readlink(link)
		test.assert(err == nil, "Error reading symlink: %v", err)
		test.assert(path == orig, "Path does not match '%s' != '%s'",
			orig, path)
	})
}

func TestSymlinkAndReadlinkThroughBranch(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		orig := "/usr/bin/arch"
		err := syscall.Symlink(orig, link)
		test.assert(err == nil, "Error creating symlink: %v", err)

		workspace = test.branchWorkspace(workspace)
		link = test.absPath(workspace + "/symlink")

		path, err := os.Readlink(link)
		test.assert(err == nil, "Error reading symlink: %v", err)
		test.assert(path == orig, "Path does not match '%s' != '%s'",
			orig, path)
	})
}

func TestSymlinkSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		orig := "/usr/bin/arch"
		err := syscall.Symlink(orig, link)
		test.assert(err == nil, "Error creating symlink: %v", err)

		stat, err := os.Lstat(link)
		test.assert(err == nil,
			"Lstat symlink error%v,%v", err, stat)
		stat_t := stat.Sys().(*syscall.Stat_t)
		test.assert(stat_t.Size == int64(len(orig)),
			"Wrong size of symlink:%d, should be:%d",
			stat_t.Size, len(orig))

	})

}
