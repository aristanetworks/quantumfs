// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "testing"

import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

func TestAdvance(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		utils.MkdirAll(workspace+"/subdir", 0777)

		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		oldRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)

		ctx := test.TestCtx()

		filename := workspace + "/subdir/testFile"
		err := testutils.PrintToFile(filename, string(GenData(1000)))
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()

		newRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)

		test.Assert(newRootId != oldRootId, "no changes to the rootId")

		wsdb := test.GetWorkspaceDB()
		_, err = wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
			wsNamespaceName, wsWorkspaceName, oldRootId, newRootId)
		test.Assert(err != nil, "Advance must not succeed")
	})
}
