// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

// Tests of the basic API running on top of a processlocal backing store.

import (
	"testing"
)

func TestConnect(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		num, err := client.NumTypespaces(test.ctx)
		test.AssertNoErr(err)
		test.Assert(num == 1, "Unexpected number of typespaces %d", num)
	})
}
