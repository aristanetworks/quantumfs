// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

func makeWorkspaceRootIdAdvance(test *testHelper,
	workspace string) (quantumfs.ObjectKey, quantumfs.WorkspaceNonce) {

	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	oldRootId, _ := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)
	filename := workspace + "/subdir/testFile"
	err := testutils.PrintToFile(filename, string(GenData(1000)))
	test.AssertNoErr(err)

	test.SyncAllWorkspaces()

	newRootId, nonce := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)

	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")

	return newRootId, nonce
}

func TestAdvanceFail(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		oldRootId, nonce := test.workspaceRootId(wsTypespaceName,
			wsNamespaceName, wsWorkspaceName)
		newRootId, _ := makeWorkspaceRootIdAdvance(test, workspace)

		wsdb := test.GetWorkspaceDB()
		ctx := test.TestCtx()
		_, _, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, nonce, oldRootId,
			newRootId)
		test.Assert(err != nil, "Advance must not succeed")
	})
}

func TestAdvanceSucceed(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		newRootId1, _ := makeWorkspaceRootIdAdvance(test, workspace)
		newRootId2, nonce := makeWorkspaceRootIdAdvance(test, workspace)

		wsdb := test.GetWorkspaceDB()
		ctx := test.TestCtx()

		// This workspace advance will succeed as it is using the correct
		// currentRootId, however, it will cause all future changes to the
		// workspace to fail.
		_, _, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, nonce, newRootId2,
			newRootId1)
		test.AssertNoErr(err)
	})
}

func TestAdvanceGoBackToOld(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		oldRootId, nonce := test.workspaceRootId(wsTypespaceName,
			wsNamespaceName, wsWorkspaceName)
		newRootId1, _ := makeWorkspaceRootIdAdvance(test, workspace)

		wsdb := test.GetWorkspaceDB()
		ctx := test.TestCtx()

		// This workspace advance will succeed as it is using the correct
		// currentRootId, however, it will cause all future changes to the
		// workspace to fail.
		_, _, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, nonce, newRootId1,
			oldRootId)
		test.AssertNoErr(err)
	})
}
