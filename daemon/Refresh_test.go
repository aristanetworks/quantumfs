// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "testing"
import "syscall"
import "os"
import "fmt"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/testutils"

func getRootId(test *testHelper, workspace string) quantumfs.ObjectKey {
	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	return test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)
}

func advanceWorkspace(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	wsdb := test.GetWorkspaceDB()

	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	_, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
		wsNamespaceName, wsWorkspaceName, src, dst)
	test.AssertNoErr(err)
}

func createTestFile(c *ctx, test *testHelper,
	workspace string, name string, size int) quantumfs.ObjectKey {

	oldRootId := getRootId(test, workspace)
	filename := workspace + "/" + name
	err := testutils.PrintToFile(filename, string(GenData(size)))
	test.AssertNoErr(err)

	test.SyncAllWorkspaces()

	newRootId := getRootId(test, workspace)

	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")

	c.vlog("Created file %s and new rootID is %s", name, newRootId.String())

	return newRootId
}

func removeTestFile(c *ctx, test *testHelper,
	workspace string, name string) quantumfs.ObjectKey {

	oldRootId := getRootId(test, workspace)

	filename := workspace + "/" + name

	err := os.Remove(filename)
	test.AssertNoErr(err)

	test.SyncAllWorkspaces()

	newRootId := getRootId(test, workspace)

	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")

	c.vlog("Removed file %s and new rootID is %s", name, newRootId.String())

	return newRootId
}

func linkTestFile(c *ctx, test *testHelper,
	workspace string, src string, dst string) quantumfs.ObjectKey {

	oldRootId := getRootId(test, workspace)
	srcfilename := workspace + "/" + src
	dstfilename := workspace + "/" + dst

	c.vlog("Before link %s -> %s", src, dst)
	err := syscall.Link(srcfilename, dstfilename)
	test.AssertNoErr(err)

	test.SyncAllWorkspaces()

	newRootId := getRootId(test, workspace)

	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")

	c.vlog("Created link %s -> %s and new rootID is %s", src, dst,
		newRootId.String())

	return newRootId
}

func markImmutable(ctx *ctx, workspace string) {
	defer ctx.qfs.mutabilityLock.Lock().Unlock()
	ctx.qfs.workspaceMutability[workspace] = workspaceImmutable
}

func markMutable(ctx *ctx, workspace string) {
	defer ctx.qfs.mutabilityLock.Lock().Unlock()
	ctx.qfs.workspaceMutability[workspace] = workspaceMutable
}

func refreshTo(c *ctx, test *testHelper, workspace string, dst quantumfs.ObjectKey) {
	wsr, cleanup := test.getWorkspaceRoot(workspace)
	defer cleanup()
	test.Assert(wsr != nil, "workspace root does not exist")
	wsr.refresh(c, dst)
}

func refreshTest(ctx *ctx, test *testHelper, workspace string,
	src quantumfs.ObjectKey, dst quantumfs.ObjectKey) {

	markImmutable(ctx, workspace)
	test.remountFilesystem()
	advanceWorkspace(ctx, test, workspace, src, dst)
	refreshTo(ctx, test, workspace, dst)
	markMutable(ctx, workspace)
}

func TestRefreshFileAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := removeTestFile(ctx, test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
	})
}

func TestRefreshUnchanged(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)

		markImmutable(ctx, workspace)
		test.remountFilesystem()
		refreshTo(ctx, test, workspace, newRootId1)
		markMutable(ctx, workspace)

		newRootId2 := getRootId(test, workspace)
		test.Assert(newRootId2.IsEqualTo(newRootId1),
			"Refresh to current rootId must be a noop")
	})
}

func TestRefreshFileRewrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"

		ctx := test.TestCtx()
		createTestFile(ctx, test, workspace, "otherfile", 1000)
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := createTestFile(ctx, test, workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := getRootId(test, workspace)
		test.Assert(newRootId3.IsEqualTo(newRootId1), "Unexpected rootid")

		removeTestFile(ctx, test, workspace, name)
	})
}

func TestRefreshFileRemove(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)
		name := "subdir/testFile"

		ctx := test.TestCtx()
		createTestFile(ctx, test, workspace, "otherfile", 1000)
		createTestFile(ctx, test, workspace, name, 1000)
		newRootId1 := removeTestFile(ctx, test, workspace, name)
		newRootId2 := createTestFile(ctx, test, workspace, name, 2000)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		filename := workspace + "/" + name
		err := os.Remove(filename)
		test.Assert(err != nil, "The file must not exist after refresh")
	})
}

func TestRefreshHardlinkAddition(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()
		oldRootId := createTestFile(ctx, test, workspace, "otherfile", 1000)
		createTestFile(ctx, test, workspace, name, 1000)
		newRootId1 := linkTestFile(ctx, test, workspace, name, linkfile)
		newRootId2 := removeTestFile(ctx, test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
		newRootId3 := removeTestFile(ctx, test, workspace, linkfile)

		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshHardlinkRemoval(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"
		linkfile := "linkFile"

		ctx := test.TestCtx()

		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		newRootId2 := linkTestFile(ctx, test, workspace, name, linkfile)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		removeTestFile(ctx, test, workspace, name)
		linkname := workspace + "/" + linkfile
		err := os.Remove(linkname)
		test.Assert(err != nil, "The linkfile must not exist after refresh")
	})
}

func TestRefreshNlinkDrop(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		oldRootId := createTestFile(ctx, test, workspace, "otherfile", 1000)
		newRootId1 := createTestFile(ctx, test, workspace, name, 1000)
		var newRootId2 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId2 = linkTestFile(ctx, test, workspace, name,
				linkfile)
		}

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := removeTestFile(ctx, test, workspace, name)
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}

func TestRefreshNlinkBump(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testFile"

		ctx := test.TestCtx()

		oldRootId := createTestFile(ctx, test, workspace, "otherfile", 1000)
		createTestFile(ctx, test, workspace, name, 1000)
		var newRootId1 quantumfs.ObjectKey

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId1 = linkTestFile(ctx, test, workspace, name,
				linkfile)
		}

		newRootId2 := removeTestFile(ctx, test, workspace, name)

		refreshTest(ctx, test, workspace, newRootId2, newRootId1)

		newRootId3 := removeTestFile(ctx, test, workspace, name)

		for i := 0; i < 10; i++ {
			linkfile := fmt.Sprintf("linkFile_%d", i)
			newRootId3 = removeTestFile(ctx, test, workspace, linkfile)
		}
		test.Assert(newRootId3.IsEqualTo(oldRootId), "Unexpected rootid")
	})
}
