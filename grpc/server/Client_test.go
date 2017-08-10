// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

// Test the grpc client

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
)

func TestServerDisconnection(t *testing.T) {
	runTestWithEphemeralBackend(t, func(test *testHelper) {
		client := test.newClient()
		err := client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			"test", "test", "test")
		test.AssertNoErr(err)

		test.restartServer()

		test.WaitFor("new server to start", func() bool {
			num, err := client.NumTypespaces(test.ctx)
			if err != nil || num != 1 {
				return false
			}
			return true
		})
	})
}

func TestSubscriptionsAcrossDisconnection(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		test.AssertNoErr(client.SubscribeTo("test1/test/test"))
		test.AssertNoErr(client.SubscribeTo("test2/test/test"))
		test.AssertNoErr(client.SubscribeTo("test3/test/test"))

		err := client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			"test1", "test", "test")
		test.AssertNoErr(err)

		updated := map[string]bool{}
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			for workspace, _ := range updates {
				updated[workspace] = true
			}
		}
		client.SetCallback(callback)

		test.restartServer()

		// Confirm replay after reconnection
		test.WaitFor("to receive workspace reconnection", func() bool {
			_, exists := updated["test1/test/test"]
			return exists
		})

		// Confirm continuity of subscription
		updated = map[string]bool{}
		err = client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
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
