// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

// Unit test to ensure when you delete a workspace that the empty typespace and
// namespace are also deleted.

import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"

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

		_, err := test.wsdb.Workspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Expected workspace doesn't exist: %v", err)

		err = test.wsdb.DeleteWorkspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Error deleting workspace: %v", err)

		_, err = test.wsdb.Workspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err != nil, "Workspace still exists!")

		_, err = test.wsdb.Workspace(test.ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Sibling workspace removed")
	})
}

func TestDeleteNamespaceSingleWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.DeleteWorkspace(test.ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ := test.wsdb.NamespaceExists(test.ctx, "type2", "name3")
		utils.Assert(!exists, "Namespace not deleted")
	})
}

func TestDeleteNamespaceMultipleWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.DeleteWorkspace(test.ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ := test.wsdb.NamespaceExists(test.ctx, "type1", "name2")
		utils.Assert(exists, "Namespace deleted early")

		err = test.wsdb.DeleteWorkspace(test.ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ = test.wsdb.NamespaceExists(test.ctx, "type1", "name2")
		utils.Assert(!exists, "Namespace not deleted")
	})
}

func TestDeleteTypespace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		createWorkspaces(test.ctx, test.wsdb)

		err := test.wsdb.DeleteWorkspace(test.ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ := test.wsdb.NamespaceExists(test.ctx, "type2", "name3")
		utils.Assert(!exists, "Namespace not deleted")

		exists, _ = test.wsdb.TypespaceExists(test.ctx, "type2")
		utils.Assert(!exists, "Typespace not deleted")
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
