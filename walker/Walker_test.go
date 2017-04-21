// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "io/ioutil"
import "os"
import "testing"

import "github.com/aristanetworks/quantumfs/daemon"

// The steps followed are the same in all the tests:
//  1. Mount a QFS instance a create files/links/dirs in it.
//  2. Restart QFS to flush the read cache.
//  3. Walk the tree using filepath.walk and read all the files.
//     While doing this intercept all the Get calls using a wrapper
//     around dataStore. Store all the intercepted keys.
//  4. Then walk the tree using the walker. Store all the walked keys.
//  5. Compare the set of keys intercepted in both the walks. They
//     should be the same.
func TestFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.readWalkCompare(workspace)
	})
}

func TestEmptyWSR(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Add nothing to workspace

		test.readWalkCompare(workspace)
	})
}

func TestDirWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Write Dir
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)
		test.readWalkCompare(workspace)
	})
}

func TestNestedDirWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// Write Dir
		dirname := workspace + "/dir"
		err := os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		// Empty Dir
		dirname = dirname + "/dir2"
		err = os.MkdirAll(dirname, 0777)
		test.Assert(err == nil, "Mkdir failed (%s): %s",
			dirname, err)

		test.readWalkCompare(workspace)
	})
}

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
		f, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0666)
		test.Assert(err == nil, "File create failed (%s): %s",
			filename, err)
		test.Assert(f != nil, "File create failed (%s): %s",
			filename, err)
		err = f.Close()
		test.Assert(err == nil, "File close failed (%s): %s",
			filename, err)

		test.readWalkCompare(workspace)
	})
}

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

		test.readWalkCompare(workspace)
	})
}

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
		link := workspace + "/filelink"
		err = os.Link(filename, link)
		test.Assert(err == nil, "Link failed (%s): %s",
			link, err)

		test.readWalkCompare(workspace)
	})
}

func TestLargeFileWalk(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(1024 * 1024 * 33)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.readWalkCompare(workspace)
	})
}

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
		test.readWalkCompare(workspace)
	})
}

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

		test.readWalkCompare(workspace)
	})
}

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

		test.readWalkCompareSkip(workspace)
	})
}
