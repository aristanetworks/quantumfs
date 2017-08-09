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

func TestSubscriptionsAcrossDisconnection(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		test.AssertNoErr(client.SubscribeTo("test1/test/test"))
		test.AssertNoErr(client.SubscribeTo("test2/test/test"))
		test.AssertNoErr(client.SubscribeTo("test3/test/test"))

		updated := map[string]bool{}
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			for workspace, _ := range updates {
				updated[workspace] = true
			}
		}
		client.SetCallback(callback)

		test.restartServer()

		err := client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			"test", "test", "test")
		test.AssertNoErr(err)

		err = client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			"test2", "test", "test")
		test.AssertNoErr(err)

		test.WaitFor("to receive workspace notification", func() bool {
			_, exists := updated["test2/test/test"]
			return exists
		})

		_, exists := updated["test/test/test"]
		test.Assert(!exists, "Invalid workspace notification received")
	})
}
