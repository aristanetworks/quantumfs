// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/testutils"

func TestBasicInterface(t *testing.T) {
	runTest(t, func(test *daemon.TestHelper) {
		apiNoPath, err := GetApi()
		test.AssertNoErr(err)

		api, err := GetApiPath(test.TempDir + "/mnt/api")
		test.Assert(err == nil, fmt.Sprintf("Wha %s", test.TempDir+"/mnt/api"))

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
	runTest(t, func(test *daemon.TestHelper) {
		api, err := GetApiPath(test.TempDir + "/mnt/api")
		test.AssertNoErr(err)

		err = api.Branch("_/_/_", "test/test/test")
		test.AssertNoErr(err)

		// Ensure that the branch was created
		_, err = os.Stat(test.AbsPath("test/test/test"))
		test.AssertNoErr(err)

		err = ReleaseApi(api)
		test.AssertNoErr(err)
	})
}

func TestInsertInode(t *testing.T) {
	runTest(t, func(test *daemon.TestHelper) {
		workspace := test.NewWorkspace()

		filedata := daemon.GenData(2000)
		filename := workspace + "/file"
		err := testutils.PrintToFile(filename, string(filedata))
		test.AssertNoErr(err)

		attrKey := make([]byte, 100)
		attrLen, err := syscall.Getxattr(filename, "quantumfs.key", attrKey)
		test.AssertNoErr(err)
		fileKey := string(attrKey[:attrLen])

		api, err := GetApiPath(test.TempDir + "/mnt/api")
		test.AssertNoErr(err)

		err = api.InsertInode(test.RelPath(workspace)+"/fileCopy", fileKey,
			0777, uint32(os.Getuid()), uint32(os.Getgid()))
		test.AssertNoErr(err)

		readBack, err := ioutil.ReadFile(workspace+"/fileCopy")
		test.Assert(bytes.Equal(readBack, filedata),
			"inserted inode data mismatch")

		err = ReleaseApi(api)
		test.AssertNoErr(err)
	})
}
