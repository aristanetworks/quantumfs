// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/testutils"
)

func TestGetBlock(t *testing.T) {
	t.Skip() // BUG209056
	runTest(t, func(test *testHelper) {
		api := test.getApi()
		defer test.putApi(api)

		for i := 0; i < 10; i++ {
			test.Log("iteration %d", i)
			testKey := fmt.Sprintf("%v_%d",
				string(daemon.GenData(18)), i)
			testData := daemon.GenData(2000)

			test.AssertNoErr(api.SetBlock(testKey, testData))

			readBack, err := api.GetBlock(testKey)
			test.AssertNoErr(err)
			test.Assert(bytes.Equal(testData, readBack),
				"Data changed between SetBlock and GetBlock")
		}
	})
}

func TestBasicInterface(t *testing.T) {
	runTest(t, func(test *testHelper) {
		apiNoPath, err := GetApi()
		test.AssertNoErr(err)
		test.putApi(apiNoPath)

		api, err := GetApiPath(test.TempDir + "/mnt/api")
		test.AssertNoErr(err)
		defer test.putApi(api)

		testKey := "ABABABABABABABABABAB"
		testData := daemon.GenData(2000)
		err = api.SetBlock(testKey, testData)
		test.AssertNoErr(err)

		readBack, err := api.GetBlock(testKey)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(testData, readBack),
			"Data changed between SetBlock and GetBlock")
	})
}

func TestBranchAndDeleteInterface(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()
		defer test.putApi(api)

		err := api.Branch("_/_/_", "test/test/test")
		test.AssertNoErr(err)

		// Ensure that the branch was created
		_, err = os.Stat(test.AbsPath("test/test/test"))
		test.AssertNoErr(err)

		// Test that we can delete it
		err = api.Delete("test/test/test")
		test.AssertNoErr(err)

		test.WaitFor("Workspace deletion", func() bool {
			// Ensure it's gone
			_, err = os.Stat(test.AbsPath("test/test/test"))
			return os.IsNotExist(err)
		})
	})
}

func TestInsertInode(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		filedata := daemon.GenData(2000)
		filename := workspace + "/file"
		err := testutils.PrintToFile(filename, string(filedata))
		test.AssertNoErr(err)

		attrKey := make([]byte, 100)
		attrLen, err := syscall.Getxattr(filename, "quantumfs.key", attrKey)
		test.AssertNoErr(err)
		fileKey := string(attrKey[:attrLen])

		api := test.getApi()
		defer test.putApi(api)

		err = api.InsertInode(test.RelPath(workspace)+"/fileCopy", fileKey,
			0777, uint32(os.Getuid()), uint32(os.Getgid()))
		test.AssertNoErr(err)

		readBack, err := ioutil.ReadFile(workspace + "/fileCopy")
		test.Assert(bytes.Equal(readBack, filedata),
			"inserted inode data mismatch")
	})
}

func TestAccessFileList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"
		test.AssertNoErr(testutils.PrintToFile(filename, "contents"))

		api := test.getApi()
		defer test.putApi(api)

		paths, err := api.GetAccessed(test.RelPath(workspace))
		test.AssertNoErr(err)

		test.Assert(len(paths.Paths) == 1, "Incorrect number of paths %d",
			len(paths.Paths))
		test.Assert(paths.Paths["/file"] == quantumfs.PathCreated|
			quantumfs.PathUpdated, "Incorrect access mark %x",
			paths.Paths["/file"])
	})
}
