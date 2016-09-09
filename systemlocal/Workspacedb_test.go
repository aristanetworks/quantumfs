// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

// Test the systemlocal WorkspaceDB

import "fmt"
import "io/ioutil"
import "os"
import "testing"

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
