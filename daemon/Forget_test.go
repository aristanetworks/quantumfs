// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that inodes can be Forgotten and re-accessed

import (
	"bytes"
	"io/ioutil"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func (th *testHelper) ForceForget() {
	// Now force the kernel to drop all cached inodes
	th.remountFilesystem()
	th.SyncAllWorkspaces()

	th.WaitForLogString("Forget called",
		"No inode forget triggered during dentry drop.")
}

func TestForgetOnDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/dir", 0777)

		numFiles := 10
		data := GenData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := testutils.PrintToFile(
				workspace+"/dir/file"+strconv.Itoa(i), string(data))
			test.Assert(err == nil, "Error creating small file")
		}
		test.ForceForget()
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
		workspace := test.NewWorkspace()

		numFiles := 10
		data := GenData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := testutils.PrintToFile(
				workspace+"/file"+strconv.Itoa(i), string(data))
			test.Assert(err == nil, "Error creating small file")
		}
		test.ForceForget()

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
		workspace := test.NewWorkspace()

		fileName := workspace + "/file"
		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating a small file: %v", err)
		file.Close()

		// Get inodeId of workspace and namespace
		wsrId := test.getInodeNum(workspace)
		fileId := test.getInodeNum(fileName)

		test.ForceForget()

		// Make sure that the workspace has already been uninstantiated
		fileInode := test.qfs.inodeNoInstantiate(&test.qfs.c, fileId)
		test.Assert(fileInode == nil,
			"Failed to forget file inode")

		wsrInode := test.qfs.inodeNoInstantiate(&test.qfs.c, wsrId)
		test.Assert(wsrInode == nil,
			"Failed to forget workspace inode")

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
		workspace := test.NewWorkspace()
		dirName := workspace + "/dir"

		err := syscall.Mkdir(dirName, 0777)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		// Generate a bunch of files
		numFiles := 10
		data := GenData(255)
		for i := 0; i < numFiles; i++ {
			err := testutils.PrintToFile(
				workspace+"/dir/file"+strconv.Itoa(i), string(data))
			test.Assert(err == nil, "Error creating small file")
		}

		// Now branch this workspace so we have a workspace full of
		// uninstantiated Inodes
		workspace = test.branchWorkspace(workspace)
		dirName = test.AbsPath(workspace + "/dir")

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

		test.SyncAllWorkspaces()

		// we need to lock to do this without racing
		test.qfs.mapMutex.Lock()
		numUninstantiatedOld := len(test.qfs.parentOfUninstantiated)
		test.qfs.mapMutex.Unlock()

		// Forgetting should now forget the Directory and thus remove all the
		// uninstantiated children from the parentOfUninstantiated list.
		test.ForceForget()

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
		workspace := test.NewWorkspace()
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
		test.SyncAllWorkspaces()

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
		workspace := test.NewWorkspace()
		fileName := workspace + "/test"
		file, err := os.Create(fileName)
		test.Assert(err == nil, "Error creating a small file: %v", err)
		file.Close()

		relpath := test.RelPath(workspace)

		// Get inodeId of workspace and namespace
		wsrId := test.getInodeNum(workspace)
		fileId := test.getInodeNum(fileName)

		// Only need to call one function running MUX::getWorkspaceRoot() to
		// verify whether the additional lookupCount has been subtracted
		api := test.getApi()
		_, err = api.GetAccessed(relpath)
		test.Assert(err == nil, "Failed call the command")

		test.ForceForget()
		// Make sure that the workspace has already been uninstantiated
		fileInode := test.qfs.inodeNoInstantiate(&test.qfs.c, fileId)
		test.Assert(fileInode == nil,
			"Failed to forget file inode")

		wsrInode := test.qfs.inodeNoInstantiate(&test.qfs.c, wsrId)
		test.Assert(wsrInode == nil,
			"Failed to forget workspace inode")
	})
}

func TestLookupCountAfterInsertInode(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		srcWorkspace := test.NewWorkspace()
		dir1 := srcWorkspace + "/dir1"

		err := os.MkdirAll(srcWorkspace+"/dir1", 0777)
		test.AssertNoErr(err)

		dstWorkspaceName := test.branchWorkspace(srcWorkspace)
		dstWorkspace := test.AbsPath(dstWorkspaceName)
		// Create one marker file in srcWorkspace and dstWorkspace
		err = testutils.PrintToFile(dir1+"/srcMarker", "testSomething")
		test.AssertNoErr(err)
		test.SyncAllWorkspaces()

		wsrId := test.getInodeNum(dstWorkspace)
		fileId := test.getInodeNum(dstWorkspace + "/dir1")

		// Uninstatiate the inodes instantiated by kernel, and then restore
		// them back at the end of the test
		test.qfs.Forget(uint64(fileId), 1)
		test.qfs.Forget(uint64(wsrId), 1)
		defer test.qfs.increaseLookupCount(test.newCtx(), fileId)
		defer test.qfs.increaseLookupCount(test.newCtx(), wsrId)
		test.SyncAllWorkspaces()

		api := test.getApi()
		key := getExtendedKeyHelper(test, dir1+"/srcMarker", "file")
		err = api.InsertInode(dstWorkspaceName+"/dir1/dstMarker",
			key, 0777, 0, 0)
		test.Assert(err == nil, "Failed inserting inode: %v", err)
		test.SyncAllWorkspaces()

		// Make sure that the workspace has already been uninstantiated
		fileInode := test.qfs.inodeNoInstantiate(&test.qfs.c, fileId)
		test.Assert(fileInode == nil,
			"Failed to forget directory inode")

		wsrInode := test.qfs.inodeNoInstantiate(&test.qfs.c, wsrId)
		test.Assert(wsrInode == nil,
			"Failed to forget workspace inode")

	})
}

// This function is intended to test that hardlink dereferencing and lookup counting
// works. So, to eliminate any kernel variation, we'll just call Mux::Lookup directly
func TestLookupCountHardlinks(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFilename := workspace + "/test"

		// First lookup
		file, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating file: %v", err)
		file.Close()

		inodeNum := test.getInodeNum(testFilename)
		wsrNum := test.getInodeNum(workspace)
		var header fuse.InHeader
		header.NodeId = uint64(wsrNum)
		var out fuse.EntryOut

		for i := 0; i < 100; i++ {
			test.qfs.Lookup(&header, "test", &out)
		}

		count, exists := test.qfs.lookupCount(inodeNum)
		test.Assert(exists, "Lookup count missing for file")

		// Since we can't guarantee the kernel hasn't looked it up extra by
		// this point, and we don't want racy tests, only check we're greater
		test.Assert(count > 100, "Lookup count missing lookups")
	})
}

func TestForgetMarking(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		// Make a simple one directory two children structure
		test.AssertNoErr(utils.MkdirAll(workspace+"/testdir", 0777))

		data := GenData(1000)
		test.AssertNoErr(ioutil.WriteFile(workspace+"/testdir/a", data,
			0777))
		test.AssertNoErr(ioutil.WriteFile(workspace+"/testdir/b", data,
			0777))

		// get the inode numbers
		parentId := test.getInodeNum(workspace + "/testdir")
		childIdA := test.getInodeNum(workspace + "/testdir/a")
		childIdB := test.getInodeNum(workspace + "/testdir/b")

		test.SyncAllWorkspaces()

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
		test.SyncAllWorkspaces()

		// Parent should still be loaded
		parent = test.qfs.inodeNoInstantiate(&test.qfs.c, parentId)
		test.Assert(parent != nil,
			"Parent forgotten while children are loaded")

		// Forget one child, not enough to forget the parent
		test.qfs.Forget(uint64(childIdA), 1)
		test.SyncAllWorkspaces()

		parent = test.qfs.inodeNoInstantiate(&test.qfs.c, parentId)
		test.Assert(parent != nil,
			"Parent forgotten when only 1/2 children unloaded")

		childA = test.qfs.inodeNoInstantiate(&test.qfs.c, childIdA)
		test.Assert(childA == nil, "ChildA not forgotten when requested")

		// Now forget the last child, which should unload the parent also
		test.qfs.Forget(uint64(childIdB), 1)
		test.SyncAllWorkspaces()

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
		workspace := test.NewWorkspace()

		data := GenData(2000)

		testFile := workspace + "/testFile"
		err := testutils.PrintToFile(testFile, string(data[:1000]))
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()

		test.remountFilesystem()

		_, err = os.Stat(testFile)
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()

		// This test will fail here with the error "Unknown inodeId %d" or
		// "lookupCount less than zero" if the race happened.
	})
}
