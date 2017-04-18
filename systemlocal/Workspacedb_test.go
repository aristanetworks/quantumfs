// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

// Test the systemlocal WorkspaceDB

import "fmt"
import "os"
import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/aristanetworks/quantumfs/qlog"

type systemlocalTest func(path string)

func newCtx() *quantumfs.Ctx {
	// Create  Ctx with random RequestId
	Qlog := qlog.NewQlogTiny()
	requestId := qlog.TestReqId
	ctx := &quantumfs.Ctx{
		Qlog:      Qlog,
		RequestId: requestId,
	}

	return ctx
}

func runTest(t *testing.T, test systemlocalTest) {
	t.Parallel()

	// Create a temporary directory to contain the database
	testDir, err := utils.SetupTestspace(1, "systemlocalTest")
	if err != nil {
		panic(fmt.Sprintf("Unable to create test directory: %v", err))
	}

	test(testDir)

	os.RemoveAll(testDir)
}

func TestDBInit(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		utils.Assert(db != nil, "Failed to init")
	})
}

func TestEmptyDB(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		var num int
		var exists bool
		var err error

		num, err = db.NumTypespaces(ctx)
		utils.Assert(err == nil, "Error in counting typespaces: %v", err)
		utils.Assert(num == 1, "Too many typespaces")

		exists, err = db.TypespaceExists(ctx, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error checking existence of typespace: %v",
			err)
		utils.Assert(exists, "Expected typespace not there")

		exists, err = db.TypespaceExists(ctx, "test")
		utils.Assert(err == nil, "Error checking existence of namespace: %v",
			err)
		utils.Assert(!exists, "Unexpected namespace")

		num, err = db.NumNamespaces(ctx, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error in counting namespaces: %v", err)
		utils.Assert(num == 1, "Too many namespaces")

		exists, err = db.NamespaceExists(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error checking existence of namespace: %v",
			err)
		utils.Assert(exists, "Expected namespace not there")

		exists, err = db.NamespaceExists(ctx, quantumfs.NullSpaceName,
			"test")
		utils.Assert(err == nil, "Error checking existence of namespace: %v",
			err)
		utils.Assert(!exists, "Unexpected namespace")

		num, err = db.NumWorkspaces(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error in counting workspaces: %v", err)
		utils.Assert(num == 1, "Too many workspaces")

		exists, err = db.WorkspaceExists(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error checking existence workspace: %v",
			err)
		utils.Assert(exists, "Expected workspace not there")

		exists, err = db.WorkspaceExists(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "other")
		utils.Assert(err == nil, "Error checking existence of workspace: %v",
			err)
		utils.Assert(!exists, "Unexpected workspace")

		key, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error fetching key: %v",
			err)
		utils.Assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"null workspace isn't empty")
	})
}

func TestBranching(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

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

		key, err := db.Workspace(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error fetching key: %v", err)
		utils.Assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"Branched rootid isn't the empty workspace")
	})
}

func TestNamespaceList(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Failed branching workspace: %v", err)

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "b")
		utils.Assert(err == nil, "Failed branching workspace: %v", err)

		exists, err := db.WorkspaceExists(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error checking if workspace exists: %v",
			err)
		utils.Assert(exists, "Workspace not really created")
		exists, err = db.WorkspaceExists(ctx, "branch", "test", "b")
		utils.Assert(err == nil, "Error checking if workspace exists: %v",
			err)
		utils.Assert(exists, "Workspace not really created")

		namespaces, err := db.NamespaceList(ctx, "branch")
		utils.Assert(err == nil, "Error getting list of namespaces: %v", err)
		utils.Assert(len(namespaces) == 1, "Incorrect number of workspaces")

		workspaces, err := db.WorkspaceList(ctx, "branch", "test")
		utils.Assert(err == nil, "Error getting list of workspaces: %v", err)
		utils.Assert(len(workspaces) == 2, "Incorrect number of workspaces")

		a := false
		b := false

		for _, workspace := range workspaces {
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
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching  workspace: %v", err)

		exists, err := db.TypespaceExists(ctx, "branch")
		utils.Assert(err == nil, "Error checking typespace exists: %v", err)
		utils.Assert(exists, "Typespace not really created")

		typespaces, err := db.TypespaceList(ctx)
		utils.Assert(err == nil, "Error getting typespace list: %v", err)
		utils.Assert(len(typespaces) == 2, "Incorrect number of typespaces")

		exists, err = db.NamespaceExists(ctx, "branch", "test")
		utils.Assert(err == nil, "Error checking namespace exists: %v", err)
		utils.Assert(exists, "Namespace not really created")

		namespaces, err := db.NamespaceList(ctx, "branch")
		utils.Assert(err == nil, "Error getting namespace list: %v", err)
		utils.Assert(len(namespaces) == 1, "Incorrect number of namespaces")

		exists, err = db.WorkspaceExists(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error checking workspace exists: %v", err)
		utils.Assert(exists, "Workspace not really created")

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
		test := false
		test2 := false

		for _, namespace := range namespaces {
			if namespace == "test2" {
				test2 = true
			}

			if namespace == "test" {
				test = true
			}
		}

		utils.Assert(test && test2, "Expected namespaces not there")
	})
}

func TestAdvanceOk(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching workspace: %v", err)

		oldRootId, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error getting key: %v", err)

		newRootId, err := db.AdvanceWorkspace(ctx, "branch", "test", "a",
			oldRootId, quantumfs.EmptyDirKey)
		utils.Assert(err == nil, "Error when advancing root: %v", err)
		utils.Assert(newRootId.IsEqualTo(quantumfs.EmptyDirKey),
			"New root doesn't match")
	})
}

func TestAdvanceNotExist(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		oldRootId, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error getting key: %v", err)

		_, err = db.AdvanceWorkspace(ctx, "branch", "test", "a", oldRootId,
			quantumfs.EmptyDirKey)
		utils.Assert(err != nil,
			"Succeeded advancing non-existant workspace")
	})
}

func TestAdvanceOldRootId(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		err := db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching workspace: %v", err)

		oldRootId, err := db.Workspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName)
		utils.Assert(err == nil, "Error getting key: %v", err)

		newRootId, err := db.AdvanceWorkspace(ctx, "branch", "test", "a",
			quantumfs.EmptyBlockKey, quantumfs.EmptyDirKey)
		utils.Assert(err != nil, "Succeeded advancing with old rootid")
		utils.Assert(!newRootId.IsEqualTo(quantumfs.EmptyDirKey),
			"New root matches what was set")
		utils.Assert(newRootId.IsEqualTo(oldRootId),
			"New root doesn't match old root id")
	})
}

func TestDbRestart(t *testing.T) {
	// Confirm that workspaces created persist across database restarts
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		ctx := newCtx()

		num, err := db.NumTypespaces(ctx)
		utils.Assert(err == nil, "Error counting typespaces: %v", err)
		utils.Assert(num == 1, "Too many typespaces")

		err = db.BranchWorkspace(ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName, "branch",
			"test", "a")
		utils.Assert(err == nil, "Error branching workspace: %v", err)

		systemdb := db.(*WorkspaceDB)
		err = systemdb.db.Close()
		utils.Assert(err == nil, "Error closing database: %v", err)

		db = NewWorkspaceDB(path + "/db")

		exists, err := db.WorkspaceExists(ctx, "branch", "test", "a")
		utils.Assert(err == nil, "Error checking if workspace exists: %v",
			err)
		utils.Assert(exists, "Workspace didn't persist across restart")
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
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
		createWorkspaces(wsdb, ctx)

		_, err := wsdb.Workspace(ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Expected workspace doesn't exist: %v", err)

		err = wsdb.DeleteWorkspace(ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Error deleting workspace: %v", err)

		_, err = wsdb.Workspace(ctx, "type1", "name2", "work2")
		utils.Assert(err != nil, "Workspace still exists!")

		_, err = wsdb.Workspace(ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Sibling workspace removed")
	})
}

func TestDeleteNamespaceSingleWorkspace(t *testing.T) {
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
		createWorkspaces(wsdb, ctx)

		err := wsdb.DeleteWorkspace(ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ := wsdb.NamespaceExists(ctx, "type2", "name3")
		utils.Assert(!exists, "Namespace not deleted")
	})
}

func TestDeleteNamespaceMultipleWorkspace(t *testing.T) {
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
		createWorkspaces(wsdb, ctx)

		err := wsdb.DeleteWorkspace(ctx, "type1", "name2", "work2")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ := wsdb.NamespaceExists(ctx, "type1", "name2")
		utils.Assert(exists, "Namespace deleted early")

		err = wsdb.DeleteWorkspace(ctx, "type1", "name2", "work3")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ = wsdb.NamespaceExists(ctx, "type1", "name2")
		utils.Assert(!exists, "Namespace not deleted")
	})
}

func TestDeleteTypespace(t *testing.T) {
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
		createWorkspaces(wsdb, ctx)

		err := wsdb.DeleteWorkspace(ctx, "type2", "name3", "work4")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		exists, _ := wsdb.NamespaceExists(ctx, "type2", "name3")
		utils.Assert(!exists, "Namespace not deleted")

		exists, _ = wsdb.TypespaceExists(ctx, "type2")
		utils.Assert(!exists, "Typespace not deleted")
	})
}

func TestSetWorkspaceImmutable(t *testing.T) {
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
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
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
		createWorkspaces(wsdb, ctx)

		immutable, err := wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work10")
		utils.Assert(err == nil && !immutable,
			"Workspace is either immutable or with error: %v", err)
	})
}

func TestWorkspaceImmutabilityAfterDelete(t *testing.T) {
	runTest(t, func(path string) {
		wsdb := NewWorkspaceDB(path + "db")
		ctx := newCtx()
		createWorkspaces(wsdb, ctx)

		err := wsdb.SetWorkspaceImmutable(ctx, "type1", "name1", "work1")
		utils.Assert(err == nil, "Failed setting workspace immutable")

		immutable, err := wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work1")
		utils.Assert(err == nil && immutable,
			"Workspace is either mutable or with error: %v", err)

		err = wsdb.DeleteWorkspace(ctx, "type1", "name1", "work1")
		utils.Assert(err == nil, "Failed deleting workspace: %v", err)

		immutable, err = wsdb.WorkspaceIsImmutable(ctx,
			"type1", "name1", "work1")
		utils.Assert(err == nil && !immutable,
			"Workspace is either immutable or with error: %v", err)
	})
}
