// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test some special properties of workspacelisting type

import (
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

func TestRemoteWorkspaceDeletion(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		// Create a workspace and stick a file in there
		workspaceName := "test/test/test"
		// We need a sibling to ensure the namespace stays around
		workspaceSibling := "test/test/testB"
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(),
			workspaceSibling))

		fileName := "testFile"
		test.AssertNoErr(testutils.PrintToFile(fileName, "test data"))
		fileHandle, err := os.Open(fileName)
		test.AssertNoErr(err)
		defer fileHandle.Close()

		// Now simulate the workspace being remotely removed
		err = test.qfs.c.workspaceDB.DeleteWorkspace(&test.qfs.c.Ctx, "test",
			"test", "test")
		test.AssertNoErr(err)

		// Check to ensure that we can't access the workspace inode anymore
		var stat syscall.Stat_t
		err = syscall.Stat(test.AbsPath(fileName), &stat)
		test.Assert(err != nil, "Still able to stat deleted workspace")

		// Make sure we cause updateChildren on the namespace
		namespaceInode := test.getInode(test.AbsPath("test/test"))
		ManualLookup(&test.qfs.c, namespaceInode, "testB")

		// Check to ensure that we can't access the workspace's child
		_, err = fileHandle.Stat()
		// We should still be able to stat our orphaned file
		test.AssertNoErr(err)
	})
}

func TestRemoteNamespaceDeletion(t *testing.T) {
	// BUG210390
	t.Skip()
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		workspaceName := "test/test/test"
		// We need a sibling to ensure the typespace stays around
		workspaceSibling := "test/testB/testB2"
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(), workspaceName))
		test.AssertNoErr(api.Branch(test.nullWorkspaceRel(),
			workspaceSibling))

		fileHandle, err := os.Open(test.AbsPath(workspaceName))
		test.AssertNoErr(err)
		defer fileHandle.Close()

		// Now simulate the namespace being remotely removed
		err = test.qfs.c.workspaceDB.DeleteWorkspace(&test.qfs.c.Ctx, "test",
			"test", "test")
		test.AssertNoErr(err)

		// Check to ensure that we can't access the namespace inode anymore
		var stat syscall.Stat_t
		err = syscall.Stat(test.AbsPath("test/test"), &stat)
		test.Assert(err != nil, "Still able to stat deleted namespace")

		// Make sure we cause updateChildren on the typespace
		typespaceInode := test.getInode(test.AbsPath("test"))
		ManualLookup(&test.qfs.c, typespaceInode, "testB")

		// Check to ensure that we can't access the workspace's child
		_, err = fileHandle.Stat()
		// We should still be able to stat our orphaned file
		test.AssertNoErr(err)
	})
}
