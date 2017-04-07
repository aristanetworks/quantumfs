// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

// Unit test to ensure when you delete a workspace that the empty typespace and
// namespace are also deleted.

import "testing"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/utils"

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
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	_, err := wsdb.Workspace(ctx, "type1", "name2", "work2")
	utils.Assert(err == nil, "Expected workspace doesn't exist: %v", err)

	err = wsdb.DeleteWorkspace(ctx, "type1", "name2", "work2")
	utils.Assert(err == nil, "Error deleting workspace: %v", err)

	_, err = wsdb.Workspace(ctx, "type1", "name2", "work2")
	utils.Assert(err != nil, "Workspace still exists!")

	_, err = wsdb.Workspace(ctx, "type1", "name2", "work3")
	utils.Assert(err == nil, "Sibling workspace removed")
}

func TestDeleteNamespaceSingleWorkspace(t *testing.T) {
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	err := wsdb.DeleteWorkspace(ctx, "type2", "name3", "work4")
	utils.Assert(err == nil, "Failed deleting workspace: %v", err)

	exists, _ := wsdb.NamespaceExists(ctx, "type2", "name3")
	utils.Assert(!exists, "Namespace not deleted")
}

func TestDeleteNamespaceMultipleWorkspace(t *testing.T) {
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	err := wsdb.DeleteWorkspace(ctx, "type1", "name2", "work2")
	utils.Assert(err == nil, "Failed deleting workspace: %v", err)

	exists, _ := wsdb.NamespaceExists(ctx, "type1", "name2")
	utils.Assert(exists, "Namespace deleted early")

	err = wsdb.DeleteWorkspace(ctx, "type1", "name2", "work3")
	utils.Assert(err == nil, "Failed deleting workspace: %v", err)

	exists, _ = wsdb.NamespaceExists(ctx, "type1", "name2")
	utils.Assert(!exists, "Namespace not deleted")
}

func TestDeleteTypespace(t *testing.T) {
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	err := wsdb.DeleteWorkspace(ctx, "type2", "name3", "work4")
	utils.Assert(err == nil, "Failed deleting workspace: %v", err)

	exists, _ := wsdb.NamespaceExists(ctx, "type2", "name3")
	utils.Assert(!exists, "Namespace not deleted")

	exists, _ = wsdb.TypespaceExists(ctx, "type2")
	utils.Assert(!exists, "Typespace not deleted")
}

func TestSetWorkspaceImmutable(t *testing.T) {
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	immutable, err := wsdb.WorkspaceIsImmutable(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil && !immutable,
		"Workspace is either immutable or with error: %v", err)

	err = wsdb.SetWorkspaceImmutable(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil, "Failed setting workspace immutable")

	immutable, err = wsdb.WorkspaceIsImmutable(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil && immutable,
		"Workspace is either mutable or with error: %v", err)
}

func TestSetNonExistingWorkspaceImmutable(t *testing.T) {
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	immutable, err := wsdb.WorkspaceIsImmutable(ctx, "type1", "name1", "work10")
	utils.Assert(err == nil && !immutable,
		"Workspace is either immutable or with error: %v", err)
}

func TestWorkspaceImmutabilityAfterDelete(t *testing.T) {
	wsdb := NewWorkspaceDB("")
	ctx := newCtx()
	createWorkspaces(ctx, wsdb)

	err := wsdb.SetWorkspaceImmutable(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil, "Failed setting workspace immutable")

	immutable, err := wsdb.WorkspaceIsImmutable(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil && immutable,
		"Workspace is either mutable or with error: %v", err)

	err = wsdb.DeleteWorkspace(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil, "Failed deleting workspace: %v", err)

	immutable, err = wsdb.WorkspaceIsImmutable(ctx, "type1", "name1", "work1")
	utils.Assert(err == nil && !immutable,
		"Workspace is either immutable or with error: %v", err)
}
