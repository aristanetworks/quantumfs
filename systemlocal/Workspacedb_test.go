// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

// Test the systemlocal WorkspaceDB

import "fmt"
import "io/ioutil"
import "os"
import "testing"

import "github.com/aristanetworks/quantumfs"

type systemlocalTest func(path string)

func runTest(t *testing.T, test systemlocalTest) {
	t.Parallel()

	// Create a temporary directory to contain the database
	testDir, err := ioutil.TempDir("", "systemlocalTest")
	if err != nil {
		panic(fmt.Sprintf("Unable to create test directory: %v", err))
	}

	test(testDir)

	os.RemoveAll(testDir)
}

func assert(condition bool, format string, args ...interface{}) {
	if !condition {
		msg := fmt.Sprintf(format, args...)
		panic(msg)
	}
}

func TestDBInit(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")
		assert(db != nil, "Failed to init")
	})
}

func TestEmptyDB(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		var num int
		var exists bool
		var err error

		num, err = db.NumTypespaces(nil)
		assert(err == nil, "Error in counting typespaces: %v", err)
		assert(num == 1, "Too many typespaces")

		exists, err = db.TypespaceExists(nil, "_null_")
		assert(err == nil, "Error checking existence of typespace: %v", err)
		assert(exists, "Expected typespace not there")

		exists, err = db.TypespaceExists(nil, "test")
		assert(err == nil, "Error checking existence of namespace: %v",
			err)
		assert(!exists, "Unexpected namespace")

		num, err = db.NumNamespaces(nil, "_null_")
		assert(err == nil, "Error in counting namespaces: %v", err)
		assert(num == 1, "Too many namespaces")

		exists, err = db.NamespaceExists(nil, "_null_", "_null")
		assert(err == nil, "Error checking existence of namespace: %v",
			err)
		assert(exists, "Expected namespace not there")

		exists, err = db.NamespaceExists(nil, "_null_", "test")
		assert(err == nil, "Error checking existence of namespace: %v",
			err)
		assert(!exists, "Unexpected namespace")

		num, err = db.NumWorkspaces(nil, "_null_", "_null")
		assert(err == nil, "Error in counting workspaces: %v", err)
		assert(num == 1, "Too many workspaces")

		exists, err = db.WorkspaceExists(nil, "_null_", "_null", "null")
		assert(err == nil, "Error checking existence workspace: %v",
			err)
		assert(exists, "Expected workspace not there")

		exists, err = db.WorkspaceExists(nil, "_null_", "_null", "other")
		assert(err == nil, "Error checking existence of workspace: %v",
			err)
		assert(!exists, "Unexpected workspace")

		key, err := db.Workspace(nil, "_null_", "_null", "null")
		assert(err == nil, "Error fetching key: %v",
			err)
		assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"null workspace isn't empty")
	})
}

func TestBranching(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "typeA", "notthere", "a", "branch",
			"somewhere", "else")
		assert(err != nil, "Succeeded branching invalid typespace")

		err = db.BranchWorkspace(nil, "_null_", "notthere", "a", "branch",
			"somewhere", "else")
		assert(err != nil, "Succeeded branching invalid namespace")

		err = db.BranchWorkspace(nil, "_null_", "_null", "notthere",
			"branch", "somewhere", "else")
		assert(err != nil, "Succeeded branching invalid workspace")

		err = db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err == nil, "Error branching null workspace: %v", err)

		err = db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err != nil, "Succeeded branching to existing workspace")

		err = db.BranchWorkspace(nil, "branch", "test", "a", "duplicate",
			"test", "b")
		assert(err == nil, "Error rebranching workspace: %v", err)

		key, err := db.Workspace(nil, "branch", "test", "a")
		assert(err == nil, "Error fetching key: %v", err)
		assert(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"Branched rootid isn't the empty workspace")
	})
}

func TestNamespaceList(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err == nil, "Failed branching workspace: %v", err)

		err = db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "b")
		assert(err == nil, "Failed branching workspace: %v", err)

		exists, err := db.WorkspaceExists(nil, "branch", "test", "a")
		assert(err == nil, "Error checking if workspace exists: %v", err)
		assert(exists, "Workspace not really created")
		exists, err = db.WorkspaceExists(nil, "branch", "test", "b")
		assert(err == nil, "Error checking if workspace exists: %v", err)
		assert(exists, "Workspace not really created")

		namespaces, err := db.NamespaceList(nil, "branch")
		assert(err == nil, "Error getting list of namespaces: %v", err)
		assert(len(namespaces) == 1, "Incorrect number of workspaces")

		workspaces, err := db.WorkspaceList(nil, "branch", "test")
		assert(err == nil, "Error getting list of workspaces: %v", err)
		assert(len(workspaces) == 2, "Incorrect number of workspaces")

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

		assert(a && b, "Expected workspaces not there")
	})
}

func TestWorkspaceList(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err == nil, "Error branching  workspace: %v", err)

		exists, err := db.TypespaceExists(nil, "branch")
		assert(err == nil, "Error checking typespace exists: %v", err)
		assert(exists, "Typespace not really created")

		typespaces, err := db.TypespaceList(nil)
		assert(err == nil, "Error getting typespace list: %v", err)
		assert(len(typespaces) == 2, "Incorrect number of typespaces")

		exists, err = db.NamespaceExists(nil, "branch", "test")
		assert(err == nil, "Error checking namespace exists: %v", err)
		assert(exists, "Namespace not really created")

		namespaces, err := db.NamespaceList(nil, "branch")
		assert(err == nil, "Error getting namespace list: %v", err)
		assert(len(namespaces) == 1, "Incorrect number of namespaces")

		exists, err = db.WorkspaceExists(nil, "branch", "test", "a")
		assert(err == nil, "Error checking workspace exists: %v", err)
		assert(exists, "Workspace not really created")

		workspaces, err := db.WorkspaceList(nil, "branch", "test")
		assert(err == nil, "Error getting workspace list: %v", err)
		assert(len(workspaces) == 1, "Incorrect number of workspaces")

		_null_ := false
		branch := false

		for _, typespace := range typespaces {
			if typespace == "_null_" {
				_null_ = true
			}

			if typespace == "branch" {
				branch = true
			}
		}

		assert(_null_ && branch, "Expected namespaces not there")
	})
}

func TestAdvanceOk(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err == nil, "Error branching workspace: %v", err)

		oldRootId, err := db.Workspace(nil, "_null_", "_null", "null")
		assert(err == nil, "Error getting key: %v", err)

		newRootId, err := db.AdvanceWorkspace(nil, "branch", "test", "a",
			oldRootId, quantumfs.EmptyDirKey)
		assert(err == nil, "Error when advancing root: %v", err)
		assert(newRootId.IsEqualTo(quantumfs.EmptyDirKey),
			"New root doesn't match")
	})
}

func TestAdvanceNotExist(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		oldRootId, err := db.Workspace(nil, "_null_", "_null", "null")
		assert(err == nil, "Error getting key: %v", err)

		_, err = db.AdvanceWorkspace(nil, "branch", "test", "a", oldRootId,
			quantumfs.EmptyDirKey)
		assert(err != nil, "Succeeded advancing non-existant workspace")
	})
}

func TestAdvanceOldRootId(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err == nil, "Error branching workspace: %v", err)

		oldRootId, err := db.Workspace(nil, "_null_", "_null", "null")
		assert(err == nil, "Error getting key: %v", err)

		newRootId, err := db.AdvanceWorkspace(nil, "branch", "test", "a",
			quantumfs.EmptyBlockKey, quantumfs.EmptyDirKey)
		assert(err != nil, "Succeeded advancing with old rootid")
		assert(!newRootId.IsEqualTo(quantumfs.EmptyDirKey),
			"New root matches what was set")
		assert(newRootId.IsEqualTo(oldRootId),
			"New root doesn't match old root id")
	})
}

func TestDbRestart(t *testing.T) {
	// Confirm that workspaces created persist across database restarts
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		num, err := db.NumTypespaces(nil)
		assert(err == nil, "Error counting typespaces: %v", err)
		assert(num == 1, "Too many typespaces")

		err = db.BranchWorkspace(nil, "_null_", "_null", "null", "branch",
			"test", "a")
		assert(err == nil, "Error branching workspace: %v", err)

		systemdb := db.(*WorkspaceDB)
		err = systemdb.db.Close()
		assert(err == nil, "Error closing database: %v", err)

		db = NewWorkspaceDB(path + "/db")

		exists, err := db.WorkspaceExists(nil, "branch", "test", "a")
		assert(err == nil, "Error checking if workspace exists: %v", err)
		assert(exists, "Workspace didn't persist across restart")
	})
}
