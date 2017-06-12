// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

import (
	"bytes"
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/testutils"
)

func WaitForApi(test *testHelper) {
	test.WaitFor("Api inode to be seen by kernel", func() bool {
		_, err := os.Stat(test.TempDir + "/mnt/api")
		return (err == nil)
	})
}

func TestBasicInterface(t *testing.T) {
	runTest(t, func(test *testHelper) {
		WaitForApi(test)

		apiNoPath, err := GetApi()
		test.AssertNoErr(err)

		api, err := GetApiPath(test.TempDir + "/mnt/api")
		test.AssertNoErr(err)

		testKey := "ABABABABABABABABABAB"
		testData := daemon.GenData(2000)
		err = api.SetBlock(testKey, testData)
		test.AssertNoErr(err)

		readBack, err := api.GetBlock(testKey)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(testData, readBack),
			"Data changed between SetBlock and GetBlock")

		err = ReleaseApi(apiNoPath)
		test.AssertNoErr(err)

		err = ReleaseApi(api)
		test.AssertNoErr(err)
	})
}

func TestBranchInterface(t *testing.T) {
	runTest(t, func(test *testHelper) {
		WaitForApi(test)

		api := test.getApi()

		err := api.Branch("_/_/_", "test/test/test")
		test.AssertNoErr(err)

		// Ensure that the branch was created
		_, err = os.Stat(test.AbsPath("test/test/test"))
		test.AssertNoErr(err)

		err = ReleaseApi(api)
		test.AssertNoErr(err)
	})
}

func TestInsertInode(t *testing.T) {
	runTest(t, func(test *testHelper) {
		WaitForApi(test)

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

		err = api.InsertInode(test.RelPath(workspace)+"/fileCopy", fileKey,
			0777, uint32(os.Getuid()), uint32(os.Getgid()))
		test.AssertNoErr(err)

		readBack, err := ioutil.ReadFile(workspace + "/fileCopy")
		test.Assert(bytes.Equal(readBack, filedata),
			"inserted inode data mismatch")

		err = ReleaseApi(api)
		test.AssertNoErr(err)
	})
}
