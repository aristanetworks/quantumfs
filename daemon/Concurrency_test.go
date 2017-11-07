// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test concurrent workspaces

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
)

func TestConcurrentReadWrite(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		mnt1 := test.qfsInstances[1].config.MountPath
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + "/" + workspaceName

		api0 := test.getApi()
		api1, err := quantumfs.NewApiWithPath(mnt1 + "/api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		dataA := []byte("abc")
		dataB := []byte("def")
		fileA := "/fileA"
		fileB := "/fileB"
		test.AssertNoErr(testutils.PrintToFile(workspace0+fileA,
			string(dataA)))

		test.AssertNoErr(api0.SyncAll())

		test.WaitFor("fileA to propagate", func() bool {
			readData, err := ioutil.ReadFile(workspace1 + fileA)
			if err != nil {
				return false
			}

			return bytes.Equal(readData, dataA)
		})

		test.AssertNoErr(testutils.PrintToFile(workspace0+fileB,
			string(dataA)))
		test.AssertNoErr(testutils.PrintToFile(workspace1+fileB,
			string(dataB)))
		test.AssertNoErr(api1.SyncAll())

		test.WaitFor("fileB to propagate", func() bool {
			readData, err := ioutil.ReadFile(workspace0 + fileB)
			if err != nil {
				return false
			}

			return bytes.Equal(readData, dataB)
		})
	})
}
