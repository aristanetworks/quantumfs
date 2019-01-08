// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package walker

import (
	"io/ioutil"
	"os"
	"strconv"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
)

// This file contains test cases which do not
// generate any errors.

// The steps followed are the same in all the tests:
//  1. Mount a QFS instance a create files/links/dirs in it.
//  2. Restart QFS to flush the read cache.
//  3. Walk the tree using filepath.walk and read all the files.
//     While doing this intercept all the Get calls using a wrapper
//     around dataStore. Store all the intercepted keys.
//  4. Then walk the tree using the walker. Store all the walked keys.
//  5. Compare the set of keys intercepted in both the walks. They
//     should be the same.

// TestFileWalk is a basic test of walking one small file.
func TestFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.readWalkCompare(workspace, false)
	})
}

// TestSpecialFileWalk tests walk of a special file.
func TestSpecialFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		// Write Special File
		filename := workspace + "/file"
		err := syscall.Mknod(filename,
			syscall.S_IFCHR|syscall.S_IRWXU, 0x12345678)
		test.Assert(err == nil, "Error creating node: %v", err)

		test.readWalkCompare(workspace, false)
	})
}

// TestEmptyWSR tests the walk of an empty WSR.
func TestEmptyWSR(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Add nothing to workspace

		test.readWalkCompare(workspace, false)
	})
}

// TestDirWalk tests the walk of a directory
// without any files in it.
func TestDirWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Write Dir
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)
		test.readWalkCompare(workspace, false)
	})
}

// TestMaxDirRecordsWalk tests the walk of a
// directory with maximum number of records.
func TestMaxDirRecordsWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Write Dir
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		// Write File
		for i := 0; i < quantumfs.MaxDirectoryRecords(); i++ {
			filename := dirname + "/file_" + strconv.Itoa(i)
			fd, err := os.Create(filename)
			test.Assert(err == nil, "Create failed (%s): %s",
				filename, err)
			fd.Close()
		}
		test.readWalkCompare(workspace, false)
	})
}

// TestChainedDirEntriesWalk tests walk of
// chained directory entry objects. Chaining occurs
// since a directory has more than max directory
// records.
func TestChainedDirEntriesWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Write Dir
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		// Write Files
		for i := 0; i < quantumfs.MaxDirectoryRecords()+100; i++ {
			filename := dirname + "/file_" + strconv.Itoa(i)
			fd, err := os.Create(filename)
			test.Assert(err == nil, "Create failed (%s): %s",
				filename, err)
			fd.Close()
		}
		test.readWalkCompare(workspace, false)
	})
}

// TestDirFilesWalk tests the walk of directory
// with files in it.
func TestDirFilesWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write Dir
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		// Write File 1
		filename := dirname + "/file"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Write File 2, empty File
		filename = dirname + "/file2"
		f, err := os.Create(filename)
		test.Assert(err == nil, "File create failed (%s): %s",
			filename, err)
		test.Assert(f != nil, "File create failed (%s): %s",
			filename, err)
		err = f.Close()
		test.Assert(err == nil, "File close failed (%s): %s",
			filename, err)

		test.readWalkCompare(workspace, false)
	})
}

// TestSoftLink tests walk of a soft-link.
func TestSoftLink(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Mark Soft Link 1
		link := workspace + "/filelink"
		err = os.Symlink(filename, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		test.readWalkCompare(workspace, false)
	})
}

// TestHardLink tests walk of hard-link.
func TestHardLink(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Mark Hard Link 1
		fname := "/filelink"
		link := workspace + fname
		hardlinks := make(map[string]struct{})
		hardlinks[fname] = struct{}{}
		err = os.Link(filename, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		test.readWalkCompare(workspace, false)
		test.checkSmallFileHardlinkKey(workspace, hardlinks)
	})
}

// TestHardLinkHardLink tests wlk of a hard-link
// pointing to a hard-link.
func TestHardLinkHardLink(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		hardlinks := make(map[string]struct{})

		// Mark Hard Link 1
		fname := "/filelink1"
		link1 := workspace + fname
		hardlinks[fname] = struct{}{}
		err = os.Link(filename, link1)
		test.Assert(err == nil, "Link failed (%s): %s",
			link1, err)

		// Mark Hard Link 2 to hard link 1
		fname = "/filelink2"
		link2 := workspace + fname
		hardlinks[fname] = struct{}{}
		err = os.Link(link1, link2)
		test.Assert(err == nil, "Link failed (%s): %s",
			link2, err)

		test.readWalkCompare(workspace, false)
		test.checkSmallFileHardlinkKey(workspace, hardlinks)
	})
}

// TestChainedHardLinkEntries tests walk of chained
// hard-link entry objects. Chaining occurs when there are
// more than max directory records count of hard-links in
// a workspace.
func TestChainedHardLinkEntries(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		hardlinks := make(map[string]struct{})
		// Mark Hard Link 1
		for i := 0; i < quantumfs.MaxDirectoryRecords()+100; i++ {
			fname := "/filelink_" + strconv.Itoa(i)
			hardlinks[fname] = struct{}{}
			link := workspace + fname
			err = os.Link(filename, link)
			test.Assert(err == nil, "Link failed (%s): %s",
				link, err)
		}
		test.readWalkCompare(workspace, false)
		test.checkSmallFileHardlinkKey(workspace, hardlinks)
	})
}

// TestLargeFileWalk tests walk of a large file.
func TestLargeFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(1024 * 1024 * 33)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.readWalkCompare(workspace, false)
	})
}

// TestLargeFileLinkWalk tests walk of a hard-link to
// large file.
func TestLargeFileLinkWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(1024 * 1024 * 33)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Mark Hard Link 1
		link := workspace + "/filelink"
		err = os.Link(filename, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)
		test.readWalkCompare(workspace, false)
		// can't use test.checkSmallFileHardlinkKey
		// TODO(kthommandra): add support for checking key in
		// directoryRecord of an object and its other keys.
	})
}

// TestMiscWalk tests walk of workspace with a directory
// containing files and hard-links.
func TestMiscWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write Dir1
		dirname := workspace + "/dir1"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		// Write File 1
		filename := workspace + "/file1"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Write File in dir
		filename = dirname + "/file1"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Write file 2 in dir
		filename2 := dirname + "/file2"
		err = ioutil.WriteFile(filename2, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename2, err)

		// Mark Hard Link 1
		link := workspace + "/filelink"
		err = os.Link(filename, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		// Mark Hard Link 2
		link = workspace + "/filelink2"
		err = os.Link(filename2, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		test.readWalkCompare(workspace, false)
	})
}

// TestMiscWalkWithSkipDir tests walk of a
// workspace containing directory with files and
// hard-links while using the skip option.
func TestMiscWalkWithSkipDir(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write Dir1
		dirname := workspace + "/dir1"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		// Write File 1
		filename := workspace + "/file1"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Write File in dir
		filename = dirname + "/file1"
		err = ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Write file 2 in dir
		filename2 := dirname + "/file2"
		err = ioutil.WriteFile(filename2, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename2, err)

		// Mark Hard Link 1
		link := workspace + "/filelink"
		err = os.Link(filename, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		// Mark Hard Link 2
		link = workspace + "/filelink2"
		err = os.Link(filename2, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		test.readWalkCompare(workspace, true)
	})
}

// TestExtendedAttributesWalk tests walk of extended
// attributes.
func TestExtendedAttributesWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Set attr for the file
		err = syscall.Setxattr(filename, xattrName, xattrData, 0)
		test.Assert(err == nil, "Error setting data XAttr: %v", err)

		test.readWalkCompare(workspace, false)
	})
}

// TestExtendedAttributesiAddRemoveWalk tests if the
// walk is ok after adding and removing extended attributes.
func TestExtendedAttributesiAddRemoveWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Set attr for the file
		err = syscall.Setxattr(filename, xattrName, xattrData, 0)
		test.Assert(err == nil, "Error setting data XAttr: %v", err)

		// Remove attr for the file
		err = syscall.Removexattr(filename, xattrName)
		test.Assert(err == nil, "Error removing data XAttr: %v", err)

		test.readWalkCompare(workspace, false)
	})
}
