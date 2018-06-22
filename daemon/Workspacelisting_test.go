// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test some special properties of workspacelisting type

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
)

func verifyWorkspacelistingInodeStatus(c *ctx, test *testHelper,
	name string, space string, mustBeInstantiated bool,
	inodeMap *map[string]InodeId) InodeId {

	id, exists := (*inodeMap)[name]
	test.Assert(exists, "Fail to get the inodeId of %s", space)

	inode := test.qfs.inodeNoInstantiate(c, id)
	if mustBeInstantiated {
		test.Assert(inode != nil,
			"The %s should be instantiated", space)
	} else {
		test.Assert(inode == nil,
			"The %s should be uninstantiated", space)
	}

	return id
}

func TestWorkspacelistingInstantiateOnDemand(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.newCtx()
		tslInode := test.qfs.inodeNoInstantiate(c, quantumfs.InodeIdRoot)
		tsl := tslInode.(*TypespaceList)

		workspace := test.NewWorkspace()
		type_, name_, work_ := test.getWorkspaceComponents(workspace)
		_, exists := tsl.typespacesByName[type_]
		test.Assert(!exists,
			"Error getting a non-existing inodeId of typespace")

		// Creating a file in the new workspace can trigger updateChildren
		// in workspacelisting. Map within will be updated, so inodes in the
		// proceeding workspace will be valid right now.
		workspace1 := test.NewWorkspace()
		testFilename := workspace1 + "/" + "test"
		err := syscall.Mkdir(testFilename, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		// Verify that the typespace has been assigned an ID to but not
		// instantiated yet. If the inode is not created, there is no
		// need to verify its descendents: namespace and workspace.
		verifyWorkspacelistingInodeStatus(c, test, type_, "typespace",
			false, &tsl.typespacesByName)

		// Instantiate the three inodes and verify the existence
		testFilename = workspace + "/" + "test"
		err = syscall.Mkdir(testFilename, 0124)
		test.Assert(err == nil, "Failed creating directories: %v", err)

		nslId := verifyWorkspacelistingInodeStatus(c, test, type_,
			"typespace", true, &tsl.typespacesByName)
		nslInode := test.qfs.inodeNoInstantiate(c, nslId)
		nsl := nslInode.(*NamespaceList)

		wslId := verifyWorkspacelistingInodeStatus(c, test, name_,
			"namespace", true, &nsl.namespacesByName)
		wslInode := test.qfs.inodeNoInstantiate(c, wslId)
		wsl := wslInode.(*WorkspaceList)

		namesAndIds := make(map[string]InodeId, len(wsl.workspacesByName))
		for name, info := range wsl.workspacesByName {
			namesAndIds[name] = info.id
		}

		verifyWorkspacelistingInodeStatus(c, test, work_, "workspace",
			true, &namesAndIds)
	})
}

func TestWorkspaceReplacement(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		workspaceName := "test/test/test"
		workspacePath := test.AbsPath(workspaceName)
		fileName := workspacePath + "/file"

		api := test.getApi()

		// Create the workspace, put something in it, delete it and then
		// create the same name again.
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))
		test.AssertNoErr(api.EnableRootWrite(workspaceName))

		origInodeNum := test.getInodeNum(workspacePath)

		test.AssertNoErr(testutils.PrintToFile(fileName, "data"))

		test.AssertNoErr(api.DeleteWorkspace(workspaceName))

		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))

		// Wait for the kernel cache to expire natually
		time.Sleep(150 * time.Millisecond)

		// Confirm the inode number has changed and that the file no longer
		// exists.
		newInodeNum := test.getInodeNum(workspacePath)

		test.Assert(newInodeNum != origInodeNum, "Inode number unchanged")
		var stat syscall.Stat_t
		err := syscall.Stat(fileName, &stat)
		test.AssertErr(err)
	})
}

func TestWorkspaceDeletionManualForget(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		// Create a workspace and stick a file in there
		workspaceName := "testA/testB/testC"
		// We need a sibling to ensure the namespace stays around
		workspaceSibling := "testA/testB/testC2"
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(),
			workspaceSibling))

		test.AssertNoErr(api.EnableRootWrite(workspaceName))
		fileName := test.AbsPath(workspaceName + "/testFile")
		test.AssertNoErr(testutils.PrintToFile(fileName, "test data"))
		fileHandle, err := os.Open(fileName)
		test.AssertNoErr(err)
		defer fileHandle.Close()

		workspaceInodeId := test.getInodeNum(test.AbsPath(workspaceName))
		err = test.qfs.c.workspaceDB.DeleteWorkspace(&test.qfs.c.Ctx,
			"testA", "testB", "testC")
		test.AssertNoErr(err)

		// Check to ensure that we can't access the workspace inode anymore
		test.assertNoFile(test.AbsPath(fileName))

		// Make sure we cause updateChildren on the namespace
		namespaceInode := test.getInode(test.AbsPath("testA/testB"))
		test.Assert(namespaceInode != nil, "cannot fetch namespace inode")
		ManualLookup(&test.qfs.c, namespaceInode, "testC2")

		_, err = fileHandle.Stat()
		// We should still be able to stat our orphaned file
		test.AssertNoErr(err)

		// Check to ensure that the kernel is still able to Forget it
		test.qfs.Forget(uint64(workspaceInodeId), 1)
		test.TestLogDoesNotContain(fmt.Sprintf(alreadyUninstantiatedLog,
			workspaceInodeId))
		defer test.qfs.incrementLookupCount(test.newCtx(), workspaceInodeId)
	})
}

func TestRemoteNamespaceDeletion(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		workspaceName := "testA/testB/testC"
		// We need a sibling to ensure the typespace stays around
		workspaceSibling := "testA/testB2/testC2"
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(),
			workspaceSibling))

		fileHandle, err := os.Open(test.AbsPath(workspaceName))
		test.AssertNoErr(err)
		defer fileHandle.Close()

		// Now simulate the namespace being remotely removed
		err = test.qfs.c.workspaceDB.DeleteWorkspace(&test.qfs.c.Ctx,
			"testA", "testB", "testC")
		test.AssertNoErr(err)

		test.WaitForLogString("Out-- Mux::handleDeletedWorkspace",
			"handleDeletedWorkspace not finished")

		// Check to ensure that we can't access the namespace inode anymore
		var stat syscall.Stat_t
		err = syscall.Stat(test.AbsPath("testA/testB"), &stat)
		test.Assert(err != nil, "Still able to stat deleted namespace")

		// Make sure we cause updateChildren on the typespace
		typespaceInode := test.getInode(test.AbsPath("testA"))
		ManualLookup(&test.qfs.c, typespaceInode, "testB2")

		// Check to ensure that we can't access the workspace's child
		_, err = fileHandle.Stat()
		// We should still be able to stat our orphaned file
		test.AssertNoErr(err)
	})
}

func TestMetaDeleteNoResidual(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()
		workspaceName := "a/b/c"
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))

		test.AssertNoErr(api.EnableRootWrite(workspaceName))
		fileName := test.AbsPath(workspaceName + "/testFile")
		test.AssertNoErr(testutils.PrintToFile(fileName, "test data"))
		fileHandle, err := os.Open(fileName)
		test.AssertNoErr(err)
		fileHandle.Close()

		wsInodeId1 := test.getInodeNum(test.AbsPath(workspaceName))
		nsInodeId1 := test.getInodeNum(test.AbsPath("a/b"))
		tsInodeId1 := test.getInodeNum(test.AbsPath("a"))
		rootInodeId := test.getInodeNum(test.AbsPath(""))

		test.Log("inodes of %s are %d/%d/%d/%d", workspaceName,
			rootInodeId, tsInodeId1, nsInodeId1, wsInodeId1)
		f, err := os.Open(test.AbsPath(""))
		test.AssertNoErr(err)
		test.AssertNoErr(api.DeleteWorkspace(workspaceName))

		test.WaitForLogString("Out-- Mux::handleDeletedWorkspace",
			"handleDeletedWorkspace not finished")

		_, err = syscall.Openat(int(f.Fd()), "a",
			syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_DIRECTORY, 0)
		test.AssertErr(err)
		test.Assert(err == syscall.ENOENT, "Expected ENOENT, got %d", err)
		test.AssertNoErr(f.Close())

		test.SyncAllWorkspaces()

		workspaceName = "a/b/c"
		api = test.getApi()
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))

		wsInodeId2 := test.getInodeNum(test.AbsPath(workspaceName))
		nsInodeId2 := test.getInodeNum(test.AbsPath("a/b"))
		tsInodeId2 := test.getInodeNum(test.AbsPath("a"))

		test.Log("inodes of %s are %d/%d/%d", workspaceName,
			tsInodeId2, nsInodeId2, wsInodeId2)
		test.Assert(wsInodeId1 != wsInodeId2,
			"workspace inode (%d) did not change", wsInodeId1)
		test.Assert(nsInodeId1 != nsInodeId2,
			"namespace inode (%d) did not change", wsInodeId1)
		test.Assert(tsInodeId1 != tsInodeId2,
			"typespace inode (%d) did not change", wsInodeId1)
	})
}
