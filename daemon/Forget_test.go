// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that inodes can be Forgotten and re-accessed

import "bytes"
import "io/ioutil"
import "os/exec"
import "strconv"
import "strings"
import "testing"

import "github.com/aristanetworks/quantumfs/qlog"

func TestForget_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()

		numFiles := 10
		data := genData(255)
		// Generate a bunch of files
		for i := 0; i < numFiles; i++ {
			err := printToFile(workspace+"/file"+strconv.Itoa(i),
				string(data))
			test.assert(err == nil, "Error creating small file")
		}

		// Now force the kernel to drop all cached inodes
		cmd := exec.Command("mount", "-i", "-oremount", test.tempDir+
			"/mnt")
		errorStr, err := cmd.CombinedOutput()
		test.assert(err == nil, "Unable to force vfs to drop dentry cache")
		test.assert(len(errorStr) == 0, "Error during remount: %s", errorStr)

		logFile := test.tempDir + "/ramfs/qlog"
		logOutput := qlog.ParseLogs(logFile)
		test.assert(!strings.Contains(logOutput, "Forgetting"),
			"No inode forget triggered during dentry drop. Try again")

		// Now read all the files back to make sure we still can
		for i := 0; i < numFiles; i++ {
			var readBack []byte
			readBack, err := ioutil.ReadFile(workspace + "/file" +
				strconv.Itoa(i))
			test.assert(bytes.Equal(readBack, data),
				"File contents not preserved after Forget")
			test.assert(err == nil, "Unable to read file after Forget")
		}
	})
}
