// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various Api calls

import "os"
import "syscall"
import "testing"

import "arista.com/quantumfs"

func TestWorkspaceBranching_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		// First branch the null workspace
		src := quantumfs.NullNamespaceName + "/" +
			quantumfs.NullWorkspaceName
		dst := "apitest/a"
		err := api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Branch the branch to have a writeable workspace
		src = dst
		dst = "apitest/b"
		err = api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Then create a file
		testFilename := dst + "/" + "test"
		fd, _ := os.Create(test.relPath(testFilename))
		fd.Close()
		var stat syscall.Stat_t
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)

		// Ensure the first branched workspace wasn't modified
		testFilename = src + "/" + "test"
		err = syscall.Stat(test.relPath(testFilename), &stat)
		test.assert(err != nil, "Original workspace was modified")
	})
}
