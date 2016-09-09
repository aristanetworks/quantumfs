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

		assert(db.NumNamespaces(nil) == 1, "Too many namespaces")
		assert(db.NamespaceExists(nil, "_null"), "Expected namespace not there")
		assert(!db.NamespaceExists(nil, "test"), "Unexpected namespace")

		assert(db.NumWorkspaces(nil, "_null") == 1, "Too many workspaces")
		assert(db.WorkspaceExists(nil, "_null", "null"),
			"Expected workspace not there")
		assert(!db.WorkspaceExists(nil, "_null", "other"), "Unexpected workspace")

		key := db.Workspace(nil, "_null", "null")
		assert(key.IsEqualTo(quantumfs.EmptyDirKey), "null workspace isn't empty")
	})
}

func TestBranching(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "notthere", "a", "somewhere", "else")
		assert(err != nil, "Succeeded branching invalid namespace")

		err = db.BranchWorkspace(nil, "_null", "notthere", "somewhere",
			"else")
		assert(err != nil, "Succeeded branching invalid workspace")

		err = db.BranchWorkspace(nil, "_null", "null", "test", "a")
		assert(err == nil, "Failed branching null workspace")

		err = db.BranchWorkspace(nil, "_null", "null", "test", "a")
		assert(err != nil, "Succeeded branching to existing workspace")

		err = db.BranchWorkspace(nil, "test", "a", "test", "b")
		assert(err == nil, "Failed rebranching workspace")
	})
}

func TestNamespaceList(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null", "null", "test", "a")
		assert(err == nil, "Failed creating workspace")

		err = db.BranchWorkspace(nil, "_null", "null", "test", "b")
		assert(err == nil, "Failed creating workspace")

		exists := db.WorkspaceExists(nil, "test", "a")
		assert(exists, "Workspace not really created")
		exists = db.WorkspaceExists(nil, "test", "b")
		assert(exists, "Workspace not really created")

		workspaces := db.WorkspaceList(nil, "test")
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

		err := db.BranchWorkspace(nil, "_null", "null", "test", "a")
		assert(err == nil, "Failed creating workspace")

		exists := db.NamespaceExists(nil, "test")
		assert(exists, "Namespace not really created")

		namespaces := db.NamespaceList(nil)
		assert(len(namespaces) == 2, "Incorrect number of namespaces")

		_null := false
		test := false

		for _, namespace := range namespaces {
			if namespace == "_null" {
				_null = true
			}

			if namespace == "test" {
				test = true
			}
		}

		assert(_null && test, "Expected namespaces not there")
	})
}

func TestAdvanceOk(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null", "null", "test", "a")
		assert(err == nil, "Failed branching workspace")

		oldRootId := db.Workspace(nil, "_null", "null")

		newRootId, err := db.AdvanceWorkspace(nil, "test", "a", oldRootId,
			quantumfs.EmptyWorkspaceKey)
		assert(err == nil, "Error when advancing root: %v", err)
		assert(newRootId.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"New root doesn't match")
	})
}

func TestAdvanceNotExist(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		oldRootId := db.Workspace(nil, "_null", "null")

		_, err := db.AdvanceWorkspace(nil, "test", "a", oldRootId,
			quantumfs.EmptyWorkspaceKey)
		assert(err != nil, "Succeeded advancing non-existant workspace")
	})
}

func TestAdvanceOldRootId(t *testing.T) {
	runTest(t, func(path string) {
		db := NewWorkspaceDB(path + "/db")

		err := db.BranchWorkspace(nil, "_null", "null", "test", "a")
		assert(err == nil, "Failed branching workspace")

		oldRootId := db.Workspace(nil, "_null", "null")

		newRootId, err := db.AdvanceWorkspace(nil, "test", "a",
			quantumfs.EmptyBlockKey, quantumfs.EmptyWorkspaceKey)
		assert(err != nil, "Succeeded advancing with old rootid")
		assert(!newRootId.IsEqualTo(quantumfs.EmptyWorkspaceKey),
			"New root matches what was set")
		assert(newRootId.IsEqualTo(oldRootId),
			"New root doesn't match old root id")
	})
}
