// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

// Test the systemlocal workspaceDB

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

func TestDBInit(t *testing.T) {
	runTest(t, func(test *testHelper) {
		utils.Assert(test.db != nil, "Failed to init")
	})
}

func TestEmptyDB(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		var num int
		var list []string
		var err error

		num, err = db.NumTypespaces(ctx)
		utils.Assert(err == nil, "Error in counting typespaces: %v", err)
		utils.Assert(num == 1, "Too many typespaces")

		list, err = db.NamespaceList(ctx, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error checking existence of typespace: %v",
			err)
		utils.Assert(len(list) == 1, "Expected typespace not there")

		list, err = db.NamespaceList(ctx, "test")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Typespace shouldn't exist: %v", err)
		utils.Assert(len(list) == 0, "Unexpected namespace")

		num, err = db.NumNamespaces(ctx, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error in counting namespaces: %v", err)
		utils.Assert(num == 1, "Too many namespaces")

		wsList, err := db.WorkspaceList(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error checking existence of namespace: %v",
			err)
		utils.Assert(len(wsList) == 1, "Expected namespace not there")

		wsList, err = db.WorkspaceList(ctx, quantumfs.NullSpaceName,
			"test")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Namespace shouldn't exist: %v", err)
		utils.Assert(len(wsList) == 0, "Unexpected namespace")

		num, err = db.NumWorkspaces(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error in counting workspaces: %v", err)
		utils.Assert(num == 1, "Too many workspaces")

		key, _, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error checking existence workspace: %v",
			err)
		utils.Assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"null workspace isn't empty")

		key, _, err = db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "other")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Workspace shouldn't exist: %v", err)
	})
}

func TestBranching(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		err := db.BranchWorkspace(ctx, "typeA", "notthere", "a", "branch",
			"somewhere", "else")
		utils.Assert(err != nil, "Succeeded branching invalid typespace")

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName, "notthere",
			"a", "branch", "somewhere", "else")
		utils.Assert(err != nil, "Succeeded branching invalid namespace")

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "notthere", "branch", "somewhere",
			"else")
		utils.Assert(err != nil, "Succeeded branching invalid workspace")

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching null workspace: %v", err)

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err != nil, "Succeeded branching to existing workspace")

		err = db.BranchWorkspace(ctx, "branch", "test", "a", "duplicate",
			"test", "b")
		utils.Assert(err == nil, "Error rebranching workspace: %v", err)

		key, nonce1, err := db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error fetching key: %v", err)
		utils.Assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"Branched rootid isn't the empty workspace")

		key, nonce2, err := db.Workspace(ctx, "duplicate", "test", "b")
		utils.Assert(err == nil, "Error fetching key: %v", err)

		utils.Assert(!nonce1.Equals(&nonce2), "Duplicate nonce for "+
			"branched workspace")
	})
}

func TestNamespaceList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Failed branching workspace: %v", err)

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "b")
		utils.Assert(err == nil, "Failed branching workspace: %v", err)

		_, _, err = db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error checking if workspace exists: %v",
			err)
		_, _, err = db.Workspace(ctx, "branch", "test", "b")
		utils.Assert(err == nil, "Error checking if workspace exists: %v",
			err)

		namespaces, err := db.NamespaceList(ctx, "branch")
		utils.Assert(err == nil, "Error getting list of namespaces: %v", err)
		utils.Assert(len(namespaces) == 1, "Incorrect number of workspaces")

		workspaces, err := db.WorkspaceList(ctx, "branch", "test")
		utils.Assert(err == nil, "Error getting list of workspaces: %v", err)
		utils.Assert(len(workspaces) == 2, "Incorrect number of workspaces")

		a := false
		b := false

		for workspace, _ := range workspaces {
			if workspace == "a" {
				a = true
			}

			if workspace == "b" {
				b = true
			}
		}

		utils.Assert(a && b, "Expected workspaces not there")
	})
}

func TestWorkspaceList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching  workspace: %v", err)

		list, err := db.NamespaceList(ctx, "branch")
		utils.Assert(err == nil, "Error checking typespace exists: %v", err)
		utils.Assert(len(list) > 0, "Typespace not really created")

		typespaces, err := db.TypespaceList(ctx)
		utils.Assert(err == nil, "Error getting typespace list: %v", err)
		utils.Assert(len(typespaces) == 2, "Incorrect number of typespaces")

		wsList, err := db.WorkspaceList(ctx, "branch", "test")
		utils.Assert(err == nil, "Error checking namespace exists: %v", err)
		utils.Assert(len(wsList) > 0, "Namespace not really created")

		namespaces, err := db.NamespaceList(ctx, "branch")
		utils.Assert(err == nil, "Error getting namespace list: %v", err)
		utils.Assert(len(namespaces) == 1, "Incorrect number of namespaces")

		_, _, err = db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error checking workspace exists: %v", err)

		workspaces, err := db.WorkspaceList(ctx, "branch", "test")
		utils.Assert(err == nil, "Error getting workspace list: %v", err)
		utils.Assert(len(workspaces) == 1, "Incorrect number of workspaces")

		_null_ := false
		branch := false

		for _, typespace := range typespaces {
			if typespace == quantumfs.NullSpaceName {
				_null_ = true
			}

			if typespace == "branch" {
				branch = true
			}
		}
		utils.Assert(_null_ && branch, "Expected typespaces not there")

		err = db.BranchWorkspace(ctx, "branch", "test", "a", "branch",
			"test2", "b")
		utils.Assert(err == nil, "Error branching the workspace: %v", err)
		namespaces, err = db.NamespaceList(ctx, "branch")
		utils.Assert(err == nil, "Error getting namespace list: %v", err)
		utils.Assert(len(namespaces) == 2, "Incorrect number of namespaces")
		test1 := false
		test2 := false

		for _, namespace := range namespaces {
			if namespace == "test2" {
				test2 = true
			}

			if namespace == "test" {
				test1 = true
			}
		}

		utils.Assert(test1 && test2, "Expected namespaces not there")
	})
}

func TestAdvanceOk(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching workspace: %v", err)

		oldRootId, _, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error getting key: %v", err)

		_, nonce, err := db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error getting key: %v", err)

		newRootId, err := db.AdvanceWorkspace(ctx, "branch", "test", "a",
			nonce, oldRootId, quantumfs.EmptyDirKey)
		utils.Assert(err == nil, "Error when advancing root: %v", err)
		utils.Assert(newRootId.IsEqualTo(quantumfs.EmptyDirKey),
			"New root doesn't match")
	})
}

func TestAdvanceNotExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		oldRootId, nonce, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error getting key: %v", err)

		_, err = db.AdvanceWorkspace(ctx, "branch", "test", "a", nonce,
			oldRootId, quantumfs.EmptyDirKey)
		utils.Assert(err != nil,
			"Succeeded advancing non-existant workspace")
	})
}

func TestAdvanceOldRootId(t *testing.T) {
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching workspace: %v", err)

		oldRootId, _, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error getting key: %v", err)

		_, nonce, err := db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error getting key: %v", err)

		newRootId, err := db.AdvanceWorkspace(ctx, "branch", "test", "a",
			nonce, quantumfs.EmptyBlockKey, quantumfs.EmptyDirKey)
		utils.Assert(err != nil, "Succeeded advancing with old rootid")
		utils.Assert(!newRootId.IsEqualTo(quantumfs.EmptyDirKey),
			"New root matches what was set")
		utils.Assert(newRootId.IsEqualTo(oldRootId),
			"New root doesn't match old root id")
	})
}

func TestDbRestart(t *testing.T) {
	// Confirm that workspaces created persist across database restarts
	runTest(t, func(test *testHelper) {
		db := test.db
		ctx := test.ctx

		num, err := db.NumTypespaces(ctx)
		utils.Assert(err == nil, "Error counting typespaces: %v", err)
		utils.Assert(num == 1, "Too many typespaces")

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching workspace: %v", err)

		systemdb := db.(*workspaceDB)
		err = systemdb.db.Close()
		utils.Assert(err == nil, "Error closing database: %v", err)

		db = NewWorkspaceDB(test.path + "/db")

		_, _, err = db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error checking if workspace exists: %v",
			err)
	})
}

func createWorkspaces(wsdb quantumfs.WorkspaceDB, ctx *quantumfs.Ctx) {
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
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		_, _, err := wsdb.Workspace(ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Expected workspace doesn't exist: %v", err)

		err = wsdb.DeleteWorkspace(ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Error deleting workspace: %v", err)

		_, _, err = wsdb.Workspace(ctx, "type1", "name2", "work2")
		utils.Assert(err != nil, "Workspace still exists!")

		_, _, err = wsdb.Workspace(ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Sibling workspace removed")
	})
}

func TestDeleteNamespaceSingleWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		err := wsdb.DeleteWorkspace(ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		list, _ := wsdb.WorkspaceList(ctx, "type2", "name3")
		utils.Assert(len(list) == 0, "Namespace not deleted")
	})
}

func TestDeleteNamespaceMultipleWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		err := wsdb.DeleteWorkspace(ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		_, err = wsdb.WorkspaceList(ctx, "type1", "name2")
		utils.Assert(err == nil, "Namespace deleted early")

		err = wsdb.DeleteWorkspace(ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		_, err = wsdb.WorkspaceList(ctx, "type1", "name2")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Namespace not deleted")
	})
}

func TestDeleteTypespace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		err := wsdb.DeleteWorkspace(ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		_, err = wsdb.WorkspaceList(ctx, "type2", "name3")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Namespace not deleted")

		_, err = wsdb.NamespaceList(ctx, "type2")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Typespace not deleted")
	})
}

func TestSetWorkspaceImmutable(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		immutable, err := wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work1")
		utils.Assert(err == nil && !immutable,
			"Workspace is either immutable or with error: %v", err)

		err = wsdb.SetWorkspaceImmutable(ctx, "type1", "name1", "work1")
		utils.Assert(err == nil, "Failed setting workspace immutable")

		immutable, err = wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work1")
		utils.Assert(err == nil && immutable,
			"Workspace is either mutable or with error: %v", err)
	})
}

func TestSetNonExistingWorkspaceImmutable(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		_, err := wsdb.WorkspaceIsImmutable(ctx, "type1", "name1", "work10")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND, "Workspace found: %v",
			err)
	})
}

func TestWorkspaceImmutabilityAfterDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		wsdb := test.db
		ctx := test.ctx
		createWorkspaces(wsdb, ctx)

		err := wsdb.SetWorkspaceImmutable(ctx, "type1", "name1", "work1")
		utils.Assert(err == nil, "Failed setting workspace immutable")

		immutable, err := wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work1")
		utils.Assert(err == nil && immutable,
			"Workspace is either mutable or with error: %v", err)

		err = wsdb.DeleteWorkspace(ctx, "type1", "name1", "work1")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		_, err = wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work1")
		utils.Assert(err.(quantumfs.WorkspaceDbErr).Code ==
			quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Workspace immutable flag exists after delete", err)
	})
}
