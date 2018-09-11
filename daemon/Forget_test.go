// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that inodes can be Forgotten and re-accessed

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func (th *testHelper) ForceForget(inodeId InodeId) {
	// Now force the kernel to drop all cached inodes
	th.remountFilesystem()
	th.SyncAllWorkspaces()
	forgetMsg := "Forget called"
	if inodeId != quantumfs.InodeIdInvalid {
		forgetMsg = fmt.Sprintf("Forget called on inode %d", inodeId)
	}
	failMsg := fmt.Sprintf("Waiting for inode %d to get uninstantiated",
		inodeId)
	th.WaitForLogString(forgetMsg, failMsg)
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
		test.ForceForget(quantumfs.InodeIdInvalid)
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
		test.ForceForget(quantumfs.InodeIdInvalid)

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

		test.ForceForget(fileId)
		test.Assert(!test.inodeIsInstantiated(&test.qfs.c, fileId),
			"Failed to forget file inode")

		test.WaitFor("wsr inode to be uninstantiated", func() bool {
			test.SyncWorkspace(test.RelPath(workspace))
			return !test.inodeIsInstantiated(&test.qfs.c, wsrId)
		})

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
		test.qfs.mapMutex.Lock(&test.qfs.c)
		numUninstantiatedOld := len(test.qfs.parentOfUninstantiated)
		test.qfs.mapMutex.Unlock(&test.qfs.c)

		// Forgetting should now forget the Directory and thus remove all the
		// uninstantiated children from the parentOfUninstantiated list.
		test.ForceForget(quantumfs.InodeIdInvalid)

		test.qfs.mapMutex.Lock(&test.qfs.c)
		//BUG: Between remountFilesystem and here, the kernel can and does
		// lookup these files, thereby populating the map we're checking!
		numUninstantiatedNew := len(test.qfs.parentOfUninstantiated)
		test.qfs.mapMutex.Unlock(&test.qfs.c)

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

		tla1 := testutils.TLA{
			MustContain: true,
			Text:        "Looked up 2 Times",
			FailMsg:     "Failed to cause a second lookup"}
		tla2 := testutils.TLA{
			MustContain: true,
			Text:        "Forgetting inode with lookupCount of 2",
			FailMsg:     "Inode with second lookup not forgotten"}
		test.AssertTestLog([]testutils.TLA{tla1, tla2})
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

		test.ForceForget(fileId)
		test.Assert(!test.inodeIsInstantiated(&test.qfs.c, fileId),
			"Failed to forget file inode")

		test.WaitFor("wsr inode to be uninstantiated", func() bool {
			test.SyncWorkspace(test.RelPath(workspace))
			return !test.inodeIsInstantiated(&test.qfs.c, wsrId)
		})
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
		defer test.qfs.incrementLookupCount(test.newCtx(), fileId)
		defer test.qfs.incrementLookupCount(test.newCtx(), wsrId)
		test.SyncAllWorkspaces()

		api := test.getApi()
		key := getExtendedKeyHelper(test, dir1+"/srcMarker", "file")
		err = api.InsertInode(dstWorkspaceName+"/dir1/dstMarker",
			key, 0777, 0, 0)
		test.Assert(err == nil, "Failed inserting inode: %v", err)
		test.SyncAllWorkspaces()

		// Make sure that the workspace has already been uninstantiated
		test.Assert(!test.inodeIsInstantiated(&test.qfs.c, fileId),
			"Failed to forget directory inode")

		test.WaitFor("wsr inode to be uninstantiated", func() bool {
			test.SyncWorkspace(test.RelPath(dstWorkspace))
			return !test.inodeIsInstantiated(&test.qfs.c, wsrId)
		})
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

		count, exists := test.qfs.lookupCount(&test.qfs.c, inodeNum)
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
		test.Assert(test.inodeIsInstantiated(&test.qfs.c, parentId),
			"Parent not loaded when expected")

		test.Assert(test.inodeIsInstantiated(&test.qfs.c, childIdA),
			"ChildA not loaded when expected")

		test.Assert(test.inodeIsInstantiated(&test.qfs.c, childIdB),
			"ChildB not loaded when expected")

		// Now start Forgetting
		test.qfs.Forget(uint64(parentId), 1)
		test.SyncAllWorkspaces()

		// Parent should still be loaded
		test.Assert(test.inodeIsInstantiated(&test.qfs.c, parentId),
			"Parent forgotten while children are loaded")

		// Forget one child, not enough to forget the parent
		test.qfs.Forget(uint64(childIdA), 1)
		test.SyncAllWorkspaces()

		// Wait for uninstantiation
		uninstMsg := fmt.Sprintf("Uninstantiating inode %d",
			childIdA)
		test.WaitForLogString(uninstMsg, "childA uninstantiation")

		test.Assert(test.inodeIsInstantiated(&test.qfs.c, parentId),
			"Parent forgotten when only 1/2 children unloaded")

		test.Assert(!test.inodeIsInstantiated(&test.qfs.c, childIdA),
			"ChildA not forgotten when requested")

		// Now forget the last child, which should unload the parent also
		test.qfs.Forget(uint64(childIdB), 1)
		test.SyncAllWorkspaces()

		uninstMsg = fmt.Sprintf("Uninstantiating inode %d",
			childIdB)
		test.WaitForLogString(uninstMsg, "childB uninstantiation")

		test.Assert(!test.inodeIsInstantiated(&test.qfs.c, childIdA),
			"ChildA not forgotten when requested")

		test.Assert(!test.inodeIsInstantiated(&test.qfs.c, childIdB),
			"ChildB not forgotten when requested")

		test.WaitFor("parent uninstantiation", func() bool {
			return !test.inodeIsInstantiated(&test.qfs.c, parentId)
		})
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

func TestForgetUnlinkedInstantiated(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "fileA"
		fullname := workspace + "/" + name

		test.createFile(workspace, name, 1000)
		var inodeId InodeId
		func() {
			file, err := os.OpenFile(fullname, os.O_RDONLY, 0777)
			test.AssertNoErr(err)
			defer file.Close()

			inodeId = test.getInodeNum(fullname)
			test.AssertNoErr(syscall.Unlink(fullname))
		}()
		test.SyncAllWorkspaces()
		_, exists := test.qfs.lookupCount(&test.qfs.c, inodeId)

		utils.Assert(!exists, "file %s still exists in the lookup map", name)
	})
}

func TestForgetUnlinkedUninstantiated(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "fileA"
		fullname := workspace + "/" + name

		test.createFile(workspace, name, 1000)
		inodeId := test.getInodeNum(fullname)
		test.remountFilesystem()
		test.AssertNoErr(syscall.Unlink(fullname))
		test.SyncAllWorkspaces()

		test.WaitFor("dropping fileA's inode", func() bool {
			_, exists := test.qfs.lookupCount(&test.qfs.c, inodeId)
			return !exists
		})
	})
}
