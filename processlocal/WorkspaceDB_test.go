// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

// Unit test to ensure when you delete a workspace that the empty typespace and
// namespace are also deleted.

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

func createWorkspaces(ctx *quantumfs.Ctx, wsdb quantumfs.WorkspaceDB) {
	workspaces := [...][3]string{
		[3]string{"type1", "name1", "work1"},
		[3]string{"type1", "name2", "work2"},
		[3]string{"type1", "name2", "work3"},
		[3]string{"type2", "name3", "work4"},
	}

	nullType := quantumfs.NullSpaceName
	nullName := quantumfs.NullSpaceName
	nullWork := quantumfs.NullSpaceName

	for _, workspace := range workspaces {
		err := wsdb.BranchWorkspace(ctx, nullType, nullName, nullWork,
			workspace[0], workspace[1], workspace[2])
		utils.Assert(err == nil, "Failed to branch workspace: %v", err)
	}
}

func TestDeleteWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		_, _, err := test.wsdb.Workspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Expected workspace doesn't exist: %v", err)

		err = test.wsdb.DeleteWorkspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Error deleting workspace: %v", err)

		_, _, err = test.wsdb.Workspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err != nil, "Workspace still exists!")

		_, _, err = test.wsdb.Workspace(test.ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Sibling workspace removed")
	})
}

func TestDeleteNamespaceSingleWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.DeleteWorkspace(test.ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		list, err := test.wsdb.WorkspaceList(test.ctx, "type2", "name3")
		utils.Assert(err.(*quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Namespace still exists")
		utils.Assert(len(list) == 0, "Namespace not deleted")
	})
}

func TestDeleteNamespaceMultipleWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.DeleteWorkspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		list, err := test.wsdb.WorkspaceList(test.ctx, "type1", "name2")
		utils.Assert(err == nil, "Namespace removed early")
		utils.Assert(len(list) == 1, "Namespace deleted early")

		err = test.wsdb.DeleteWorkspace(test.ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		_, err = test.wsdb.WorkspaceList(test.ctx, "type1", "name2")
		utils.Assert(err.(*quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Namespace found")
	})
}

func TestDeleteTypespace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.DeleteWorkspace(test.ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		_, err = test.wsdb.WorkspaceList(test.ctx, "type2", "name3")
		utils.Assert(err.(*quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Namespace not deleted")

		_, err = test.wsdb.NamespaceList(test.ctx, "type2")
		utils.Assert(err.(*quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Typespace not deleted")
	})
}

func TestSetWorkspaceImmutable(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		immutable, err := test.wsdb.WorkspaceIsImmutable(test.ctx, "type1",
			"name1", "work1")
		utils.Assert(err == nil && !immutable,
			"Workspace is either immutable or with error: %v", err)

		err = test.wsdb.SetWorkspaceImmutable(test.ctx, "type1", "name1",
			"work1")
		utils.Assert(err == nil, "Failed setting workspace immutable")

		immutable, err = test.wsdb.WorkspaceIsImmutable(test.ctx, "type1",
			"name1", "work1")
		utils.Assert(err == nil && immutable,
			"Workspace is either mutable or with error: %v", err)
	})
}

func TestSetNonExistingWorkspaceImmutable(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		immutable, err := test.wsdb.WorkspaceIsImmutable(test.ctx, "type1",
			"name1", "work10")
		utils.Assert(err == nil && !immutable,
			"Workspace is either immutable or with error: %v", err)
	})
}

func TestWorkspaceImmutabilityAfterDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.SetWorkspaceImmutable(test.ctx, "type1", "name1",
			"work1")
		utils.Assert(err == nil, "Failed setting workspace immutable")

		immutable, err := test.wsdb.WorkspaceIsImmutable(test.ctx, "type1",
			"name1", "work1")
		utils.Assert(err == nil && immutable,
			"Workspace is either mutable or with error: %v", err)

		err = test.wsdb.DeleteWorkspace(test.ctx, "type1", "name1", "work1")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		immutable, err = test.wsdb.WorkspaceIsImmutable(test.ctx, "type1",
			"name1", "work1")
		utils.Assert(err == nil && !immutable,
			"Workspace is either immutable or with error: %v", err)
	})
}

func TestPubSubBranch(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.wsdb

		called := false
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			test.Assert(len(updates) == 1, "Wrong number of updates: %d",
				len(updates))
			called = true
		}
		wsdb.SetCallback(callback)
		err := wsdb.SubscribeTo("test/test/test")
		test.AssertNoErr(err)

		test.AssertNoErr(wsdb.BranchWorkspace(test.ctx,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "test", "test", "test"))

		test.WaitFor("Callback to be invoked", func() bool { return called })
	})
}

func TestPubSubAdvanceAndUnsubscribe(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.wsdb

		called := false
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			test.Assert(len(updates) == 1, "Wrong number of updates: %d",
				len(updates))
			called = true
		}
		wsdb.SetCallback(callback)
		err := wsdb.SubscribeTo("test/test/test")
		test.AssertNoErr(err)
		wsdb.UnsubscribeFrom("test/test/test")

		test.AssertNoErr(wsdb.BranchWorkspace(test.ctx,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "test", "test", "test"))

		err = wsdb.SubscribeTo("test/test/test")
		test.AssertNoErr(err)
		test.Assert(!called, "Notification when not subscribed!")

		key, nonce, err := wsdb.Workspace(test.ctx, "test", "test", "test")
		test.AssertNoErr(err)

		_, err = wsdb.AdvanceWorkspace(test.ctx, "test", "test", "test",
			nonce, key, key)
		test.AssertNoErr(err)

		test.WaitFor("Callback to be invoked", func() bool { return called })
	})
}

func TestPubSubDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.wsdb

		called := false
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			test.Assert(len(updates) == 1, "Wrong number of updates: %d",
				len(updates))
			called = true
		}
		wsdb.SetCallback(callback)

		test.AssertNoErr(wsdb.BranchWorkspace(test.ctx,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "test", "test", "test"))

		err := wsdb.SubscribeTo("test/test/test")
		test.AssertNoErr(err)

		err = wsdb.DeleteWorkspace(test.ctx, "test", "test", "test")
		test.AssertNoErr(err)

		test.WaitFor("Callback to be invoked", func() bool { return called })
		test.Assert(false, "Intentional")
	})
}
