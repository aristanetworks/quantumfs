// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test operations on hardlinks and symlinks

import "bytes"
import "io/ioutil"
import "os"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs"

func TestHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
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

		// Take note of the nextHardlinkId
		nextHardlinkId := test.getWorkspaceRoot(workspace).nextHardlinkId

		// Branch and confirm the hardlink is still there
		workspace = test.absPath(test.branchWorkspace(workspace))
		file1 = workspace + "/orig_file"
		file2 = workspace + "/hardlink"
		data, err = ioutil.ReadFile(file2)
		test.assert(err == nil, "Error reading linked file: %v", err)
		test.assert(bytes.Equal(data, testData), "Data corrupt!")

		wsr := test.getWorkspaceRoot(workspace)
		test.assert(len(wsr.hardlinks) == 1, "Wsr hardlink link len is %d",
			len(wsr.hardlinks))

		nextHardlinkId_ := test.getWorkspaceRoot(workspace).nextHardlinkId
		test.assert(nextHardlinkId == nextHardlinkId_ && nextHardlinkId != 0,
			"nextHardlinkId unset or not saved/loaded")

		// Ensure that hardlinks are now in place
		file1InodeNum := test.getInodeNum(file1)
		file2InodeNum := test.getInodeNum(file2)

		parentInode := test.getInode(workspace)
		parentDir := parentInode.(*WorkspaceRoot).Directory
		defer parentDir.childRecordLock.Lock().Unlock()
		test.assert(parentDir.children.record(file1InodeNum).Type() ==
			quantumfs.ObjectTypeHardlink,
			"file1 not replaced with hardlink %d %v", file1InodeNum,
			parentDir.children.childrenRecords)
		test.assert(parentDir.children.record(file2InodeNum).Type() ==
			quantumfs.ObjectTypeHardlink,
			"file2 not created as hardlink")
	})
}

func TestSymlinkCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		link := workspace + "/symlink"
		err := syscall.Symlink("/usr/bin/arch", link)
		test.assert(err == nil, "Error creating symlink: %v", err)
	})
}

func TestReadlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
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

func TestSymlinkHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		err := os.MkdirAll(workspace+"/dir", 0777)
		test.assertNoErr(err)

		file := workspace + "/dir/file"
		softlink := workspace + "/dir/symlink"
		hardlink := workspace + "/dir/hardlink"

		data := genData(2000)
		err = printToFile(file, string(data))
		test.assertNoErr(err)

		err = syscall.Symlink(file, softlink)
		test.assertNoErr(err)

		err = syscall.Link(softlink, hardlink)
		test.assertNoErr(err)

		err = printToFile(softlink, string(data))
		test.assertNoErr(err)
		data = append(data, data...)

		readData, err := ioutil.ReadFile(softlink)
		test.assertNoErr(err)
		test.assert(bytes.Equal(readData, data), "data mismatch")

		readData, err = ioutil.ReadFile(hardlink)
		test.assertNoErr(err)
		test.assert(bytes.Equal(readData, data), "data mismatch")

		err = printToFile(softlink, string(data))
		test.assertNoErr(err)
	})
}
