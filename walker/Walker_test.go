// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "fmt"
import "io/ioutil"
import "os"
import "strings"
import "strconv"
import "testing"

import "github.com/aristanetworks/quantumfs"
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
		test.readWalkCompare(workspace)
	})
}

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
		f, err := os.Create(filename)
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

func TestChainedHardLinkEntries(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(50)
		workspace := test.NewWorkspace()

		// Write File 1
		filename := workspace + "/file"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		// Mark Hard Link 1
		for i := 0; i < quantumfs.MaxDirectoryRecords()+100; i++ {
			link := workspace + "/filelink_" + strconv.Itoa(i)
			err = os.Link(filename, link)
			test.Assert(err == nil, "Link failed (%s): %s",
				link, err)
		}
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

func TestWalkPanicString(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedString := "raised panic"
		expectedErr := fmt.Errorf(expectedString)

		// Write File 1
		filename := workspace + "/panicFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
			size uint64, isDir bool) error {

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedString)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.Assert(err.Error() == expectedErr.Error(),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
	})
}

func TestWalkPanicErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedErr := fmt.Errorf("raised panic")

		// Write File 1
		filename := workspace + "/panicFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
			size uint64, isDir bool) error {

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedErr)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.Assert(err == expectedErr,
			"Walk did not get the expectedErr value, instead got %v",
			err)
	})
}

func TestWalkErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedErr := fmt.Errorf("send error")

		// Write File 1
		filename := workspace + "/errorFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey,
			size uint64, isDir bool) error {

			if strings.HasSuffix(path, "/errorFile") {
				return expectedErr
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.Assert(err.Error() == expectedErr.Error(),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
	})
}
