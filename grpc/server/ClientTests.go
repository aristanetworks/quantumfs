// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

// Test the grpc client

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
)

func TestServerDisconnection(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()
		err := client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			"test", "test", "test")
		test.AssertNoErr(err)

		test.restartServer()

		num, err := client.NumTypespaces(test.ctx)
		test.AssertNoErr(err)
		test.Assert(num == 1, "Incorrect number of typespaces: %d", num)
	})
}
