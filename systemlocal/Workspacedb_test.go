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
	// Create a temporary directory to contain the database
	testDir, err := ioutil.TempDir("", "systemlocalTest")
	if err != nil {
		panic(fmt.Sprintf("Unable to create test directory: %v", err))
	}

	test(testDir)

	os.RemoveAll(testDir)
}

func assert(condition bool, message string) {
	if !condition {
		panic(message)
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
