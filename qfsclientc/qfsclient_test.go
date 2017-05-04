// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

import "testing"

func TestInterface(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api, err := GetApi()
		test.AssertNoErr(err)

		apiPath, err := GetApiPath("/test")
		test.AssertNoErr(err)

		err = ReleaseApi(api)
		test.AssertNoErr(err)

		err = ReleaseApi(apiPath)
		test.AssertNoErr(err)
	})
}
