// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

import "bytes"
import "testing"

import "github.com/aristanetworks/quantumfs/daemon"

func TestInterface(t *testing.T) {
	runTest(t, func(test *daemon.TestHelper) {
		apiNoPath, err := GetApi()
		test.AssertNoErr(err)	

		api, err := GetApiPath(test.TempDir + "/mnt/api")
		test.AssertNoErr(err)

		testKey := "ABABABABABABABABABAB"
		testData := []byte("This is some data")
		err = api.SetBlock(testKey, testData)
		test.AssertNoErr(err)

		readBack, err := api.GetBlock(testKey)
		test.AssertNoErr(err)
		test.Assert(bytes.Equal(testData, readBack),
			"SetBlock data changed in GetBlock")

		err = ReleaseApi(apiNoPath)
		test.AssertNoErr(err)

		err = ReleaseApi(api)
		test.AssertNoErr(err)
	})
}
