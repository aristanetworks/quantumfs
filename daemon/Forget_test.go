// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that inodes can be Forgotten and re-accessed

import "bytes"
import "io/ioutil"
import "os"
import "strconv"
import "syscall"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/testutils"

func TestForgetOnDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/dir", 0777)

		numFiles := 10
		data := genData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := testutils.PrintToFile(
				workspace+"/dir/file"+strconv.Itoa(i), string(data))
			test.Assert(err == nil, "Error creating small file")
		}

		// Now force the kernel to drop all cached inodes
		test.remountFilesystem()

		test.AssertLogContains("Forget called",
			"No inode forget triggered during dentry drop.")

		// Now read all the files back to make sure we still can
		for i := 0; i < numFiles; i++ {
			var readBack []byte
			readBack, err := ioutil.ReadFile(workspace + "/dir/file" +
				strconv.Itoa(i))
			test.Assert(bytes.Equal(readBack, data),
				"File contents not preserved after Forget")
			test.Assert(err == nil, "Unable to read file after Forget")
		}
	})
}

func TestForgetOnWorkspaceRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		numFiles := 10
		data := genData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := testutils.PrintToFile(
				workspace+"/file"+strconv.Itoa(i), string(data))
			test.Assert(err == nil, "Error creating small file")
		}

		// Now force the kernel to drop all cached inodes
		test.remountFilesystem()

		test.AssertLogContains("Forget called",
			"No inode forget triggered during dentry drop.")

		// Now read all the files back to make sure we still can
		for i := 0; i < numFiles; i++ {
			var readBack []byte
			readBack, err := ioutil.ReadFile(workspace + "/file" +
				strconv.Itoa(i))
			test.Assert(bytes.Equal(readBack, data),
				"File contents not preserved after Forget")
			test.Assert(err == nil, "Unable to read file after Forget")
		}
	})
}

func TestConfirmWorkspaceMutabilityAfterUninstantiation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		fileName := workspace + "/file"
		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating a small file: %v", err)
		file.Close()

		// Get inodeId of workspace and namespace
		wsrId := test.getInodeNum(workspace)
		fileId := test.getInodeNum(fileName)

		// Now force the kernel to drop all cached inodes
		test.remountFilesystem()
		test.AssertLogContains("Forget called",
			"No inode forget triggered during dentry drop.")
		test.syncAllWorkspaces()

		// Make sure that the workspace has already been uninstantiated
		fileInode := test.qfs.inodeNoInstantiate(&test.qfs.c, fileId)
		test.Assert(fileInode == nil,
			"Failed to forgot file inode")

		wsrInode := test.qfs.inodeNoInstantiate(&test.qfs.c, wsrId)
		test.Assert(wsrInode == nil,
			"Failed to forgot workspace inode")

		// Verify the mutability is preserved
		fd, err := syscall.Creat(workspace+"/file1", 0124)
		defer syscall.Close(fd)
		test.Assert(err == nil, "Error opening the file: %v", err)
	})
}

func TestForgetUninstantiatedChildren(t *testing.T) {
	// This test is disabled until we can think of a good way to fix it. Also,
	// its not 100% necessary.
	t.Skip()
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		dirName := workspace + "/dir"

		err := os.Mkdir(dirName, 0777)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		// Generate a bunch of files
		numFiles := 10
		data := genData(255)
		for i := 0; i < numFiles; i++ {
			err := testutils.PrintToFile(
				workspace+"/dir/file"+strconv.Itoa(i), string(data))
			test.Assert(err == nil, "Error creating small file")
		}

		// Now branch this workspace so we have a workspace full of
		// uninstantiated Inodes
		workspace = test.branchWorkspace(workspace)
		dirName = test.absPath(workspace + "/dir")

		// Get the listing from the directory to instantiate that directory
		// and add its children to the uninstantiated inode list.
		dirInodeNum := test.getInodeNum(dirName)
		dir, err := os.Open(dirName)
		test.Assert(err == nil, "Error opening directory: %v", err)
		children, err := dir.Readdirnames(-1)
		test.Assert(err == nil, "Error reading directory children: %v", err)
		test.Assert(len(children) == numFiles,
			"Wrong number of children: %d != %d", len(children),
			numFiles)
		dir.Close()

		test.syncAllWorkspaces()

		// we need to lock to do this without racing
		test.qfs.mapMutex.Lock()
		numUninstantiatedOld := len(test.qfs.parentOfUninstantiated)
		test.qfs.mapMutex.Unlock()

		// Forgetting should now forget the Directory and thus remove all the
		// uninstantiated children from the parentOfUninstantiated list.
		test.remountFilesystem()

		test.AssertLogContains("Forget called",
			"No inode forget triggered during dentry drop.")

		test.qfs.mapMutex.Lock()
		//BUG: Between remountFilesystem and here, the kernel can and does
		// lookup these files, thereby populating the map we're checking!
		numUninstantiatedNew := len(test.qfs.parentOfUninstantiated)
		test.qfs.mapMutex.Unlock()

		test.Assert(numUninstantiatedOld > numUninstantiatedNew,
			"No uninstantiated inodes were removed %d <= %d",
			numUninstantiatedOld, numUninstantiatedNew)

		for _, parent := range test.qfs.parentOfUninstantiated {
			test.Assert(parent != dirInodeNum, "Uninstantiated inodes "+
				"use forgotten directory as parent")
		}
	})
}

func TestMultipleLookupCount(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating file: %v", err)

		time.Sleep(200 * time.Millisecond)

		file2, err := os.Open(testFilename)
		test.Assert(err == nil, "Error opening file readonly")

		file.Close()
		file2.Close()
		// Wait for the closes to bubble up to QuantumFS
		time.Sleep(100 * time.Millisecond)

		// Forget Inodes
		test.remountFilesystem()

		test.AssertTestLog([]testutils.TLA{
			testutils.TLA{true, "Looked up 2 Times",
				"Failed to cause a second lookup"},
			testutils.TLA{true, "Forgetting inode with lookupCount of 2",
				"Inode with second lookup not forgotten"},
		})
	})
}

func TestLookupCountAfterCommand(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		workspace := test.newWorkspace()
		filename := workspace + "/test"
		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating a small file: %v", err)
		file.Close()

		relpath := test.relPath(workspace)

		// Get inodeId of workspace and namespace
		wsrId := test.getInodeNum(workspace)
		fileId := test.getInodeNum(fileName)

		api := test.getApi()
		err = api.GetAccessed(relpath)

		// Now force the kernel to drop all cached inodes
		test.remountFilesystem()
		test.AssertLogContains("Forget called",
			"No inode forget triggered during dentry drop.")
		test.syncAllWorkspaces()

		// Make sure that the workspace has already been uninstantiated
		fileInode := test.qfs.inodeNoInstantiate(&test.qfs.c, fileId)
		test.Assert(fileInode == nil,
			"Failed to forgot file inode")

		wsrInode := test.qfs.inodeNoInstantiate(&test.qfs.c, wsrId)
		test.Assert(wsrInode == nil,
			"Failed to forgot workspace inode")
	})
}

func TestLookupCountHardlinks(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"
		linkFilename := workspace + "/link"

		// First lookup
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating file: %v", err)
		file.Close()

		// Second lookup
		err = os.Link(testFilename, linkFilename)
		test.Assert(err == nil, "Error creating hardlink")

		// Forget Inodes
		test.remountFilesystem()

		test.AssertLogContains("Looked up 2 Times",
			"Failed to cause a second lookup")
	})
}

func TestForgetMarking(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		// Make a simple one directory two children structure
		test.AssertNoErr(os.MkdirAll(workspace+"/testdir", 0777))

		data := genData(1000)
		test.AssertNoErr(ioutil.WriteFile(workspace+"/testdir/a", data,
			0777))
		test.AssertNoErr(ioutil.WriteFile(workspace+"/testdir/b", data,
			0777))

		// get the inode numbers
		parentId := test.getInodeNum(workspace + "/testdir")
		childIdA := test.getInodeNum(workspace + "/testdir/a")
		childIdB := test.getInodeNum(workspace + "/testdir/b")

		test.syncAllWorkspaces()

		// We need to trigger, ourselves, the kind of Forget sequence where
		// markings are necessary: parent, childA, then childB
		parent := test.qfs.inodeNoInstantiate(&test.qfs.c, parentId)
		test.Assert(parent != nil,
			"Parent not loaded when expected")

		childA := test.qfs.inodeNoInstantiate(&test.qfs.c, childIdA)
		test.Assert(childA != nil,
			"ChildA not loaded when expected")

		childB := test.qfs.inodeNoInstantiate(&test.qfs.c, childIdB)
		test.Assert(childB != nil,
			"ChildB not loaded when expected")

		// Now start Forgetting
		test.qfs.Forget(uint64(parentId), 1)
		test.syncAllWorkspaces()

		// Parent should still be loaded
		parent = test.qfs.inodeNoInstantiate(&test.qfs.c, parentId)
		test.Assert(parent != nil,
			"Parent forgotten while children are loaded")

		// Forget one child, not enough to forget the parent
		test.qfs.Forget(uint64(childIdA), 1)
		test.syncAllWorkspaces()

		parent = test.qfs.inodeNoInstantiate(&test.qfs.c, parentId)
		test.Assert(parent != nil,
			"Parent forgotten when only 1/2 children unloaded")

		childA = test.qfs.inodeNoInstantiate(&test.qfs.c, childIdA)
		test.Assert(childA == nil, "ChildA not forgotten when requested")

		// Now forget the last child, which should unload the parent also
		test.qfs.Forget(uint64(childIdB), 1)
		test.syncAllWorkspaces()

		childA = test.qfs.inodeNoInstantiate(&test.qfs.c, childIdA)
		test.Assert(childA == nil, "ChildA not forgotten when requested")

		childB = test.qfs.inodeNoInstantiate(&test.qfs.c, childIdB)
		test.Assert(childB == nil, "ChildB not forgotten when requested")

		parent = test.qfs.inodeNoInstantiate(&test.qfs.c, parentId)
		test.Assert(parent == nil,
			"Parent %d not forgotten when all children unloaded",
			parentId)
	})
}

func TestForgetLookupRace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		data := genData(2000)

		testFile := workspace + "/testFile"
		err := testutils.PrintToFile(testFile, string(data[:1000]))
		test.AssertNoErr(err)

		test.syncAllWorkspaces()

		test.remountFilesystem()

		_, err = os.Stat(testFile)
		test.AssertNoErr(err)

		test.syncAllWorkspaces()

		// This test will fail here with the error "Unknown inodeId %d" or
		// "lookupCount less than zero" if the race happened.
	})
}
