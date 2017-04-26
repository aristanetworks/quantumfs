// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

func makeWorkspaceRootIdAdvance(test *testHelper,
	workspace string) quantumfs.ObjectKey {

	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	oldRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)
	filename := workspace + "/subdir/testFile"
	err := testutils.PrintToFile(filename, string(GenData(1000)))
	test.AssertNoErr(err)

	test.SyncAllWorkspaces()

	newRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)

	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")

	return newRootId
}

func TestAdvanceFail(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		oldRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)
		newRootId := makeWorkspaceRootIdAdvance(test, workspace)

		wsdb := test.GetWorkspaceDB()
		ctx := test.TestCtx()
		_, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, oldRootId, newRootId)
		test.Assert(err != nil, "Advance must not succeed")
	})
}

func TestAdvanceSucceed(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		newRootId1 := makeWorkspaceRootIdAdvance(test, workspace)
		newRootId2 := makeWorkspaceRootIdAdvance(test, workspace)

		wsdb := test.GetWorkspaceDB()
		ctx := test.TestCtx()

		// This workspace advance will succeed as it is using the correct
		// currentRootId, however, it will cause all future changes to the
		// workspace to fail.
		_, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, newRootId2, newRootId1)
		test.AssertNoErr(err)
	})
}

func TestAdvanceGoBackToOld(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		oldRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)
		newRootId1 := makeWorkspaceRootIdAdvance(test, workspace)

		wsdb := test.GetWorkspaceDB()
		ctx := test.TestCtx()

		// This workspace advance will succeed as it is using the correct
		// currentRootId, however, it will cause all future changes to the
		// workspace to fail.
		_, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, newRootId1, oldRootId)
		test.AssertNoErr(err)
	})
}
