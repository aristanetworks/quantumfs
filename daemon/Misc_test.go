// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test cases which do not belong in other test files

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/testutils"
)

func TestUnknownInodeId(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		// We need to create enough files that we can read
		// some from the directory without reading the entire
		// directory. Then we can cause a directory snapshot
		// to be taken, delete the file filename and then
		// continue reading. This will result in the inodeId
		// for the filename being returned to the kernel after
		// that ID is no longer valid. This entry will be
		// cached in the kernel and the subsequent open call
		// will cause an inode number entirely unknown to
		// QuantumFS to be used in QuantumFs.Open().
		for i := 0; i < 300; i++ {
			file := fmt.Sprintf("%s/filler-%d", workspace, i)
			test.AssertNoErr(testutils.PrintToFile(file, "contents"))
		}
		test.AssertNoErr(testutils.PrintToFile(filename, "contents"))
		inodeNum := test.getInodeNum(filename)

		dir, err := os.Open(workspace)
		test.AssertNoErr(err)
		defer dir.Close()
		_, err = dir.Readdir(10)
		test.AssertNoErr(err)

		test.AssertNoErr(syscall.Unlink(filename))

		test.SyncAllWorkspaces()
		test.qfs.Forget(uint64(inodeNum), 1)

		_, err = dir.Readdir(0)
		test.AssertNoErr(err)

		_, err = os.Open(filename)
		test.AssertErr(err)
		test.Assert(os.IsNotExist(err),
			"Expected ENOENT, got %s", err.Error())
	})
}

func TestDualInstances(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		expectedData := test.MakeFile(filename)
		test.SyncAllWorkspaces()

		path := test.qfsInstances[1].config.MountPath + "/" +
			test.RelPath(filename)

		test.CheckData(path, expectedData)
	})
}

func TestWorkspacePubSubCallback(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		test.MakeFile(filename)
		test.SyncAllWorkspaces()

		test.WaitForLogString("Mux::handleWorkspaceChanges",
			"Workspace pubsub callback to be called")
	})
}

func TestInodeIdsIncrementing(t *testing.T) {
	runTest(t, func(test *testHelper) {
		ids := newInodeIds(100 * time.Millisecond)
		test.Assert(ids.newInodeId() == 4, "Wrong 1st inodeId given")
		test.Assert(ids.newInodeId() == 5, "Wrong 2nd inodeId given")
		test.Assert(ids.newInodeId() == 6, "Wrong 3rd inodeId given")

		ids.releaseInodeId(4)
		time.Sleep(50 * time.Millisecond)
		test.Assert(ids.newInodeId() == 7, "Wrong next id during delay")
		time.Sleep(60 * time.Millisecond)

		test.Assert(ids.newInodeId() == 4, "Didn't get to reuse 1st id")
		test.Assert(ids.newInodeId() == 8, "Wrong next id")
	})
}
