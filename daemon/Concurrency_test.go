// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test concurrent workspaces

import (
	"bytes"
	"testing"
	"io/ioutil"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
)

func TestConcurrentReadWrite(t *testing.T) {
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0 := test.NewWorkspace()
		mnt1 := test.qfsInstances[1].config.MountPath
		workspaceName := test.RelPath(workspace0)
		workspace1 := mnt1 + "/" +  workspaceName

		api1, err := quantumfs.NewApiWithPath(mnt1 + "/api")
		test.AssertNoErr(err)
		defer api1.Close()
		test.AssertNoErr(api1.EnableRootWrite(workspaceName))

		data := []byte("abc")
		testFile := "/file"
		test.AssertNoErr(testutils.PrintToFile(workspace0 + testFile,
			string(data)))

		readData, err := ioutil.ReadFile(workspace1 + testFile)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(readData, data),
			"concurrent read/write broken")
	})
}
