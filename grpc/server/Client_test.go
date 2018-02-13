// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

// Test the grpc client

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
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

		err := client.BranchWorkspace(test.ctx, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			"test1", "test", "test")
		test.AssertNoErr(err)

		var mutex utils.DeferableMutex
		updated := map[string]bool{}
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			defer mutex.Lock().Unlock()
			for workspace, _ := range updates {
				test.Log("Received notification for %s", workspace)
				updated[workspace] = true
			}
		}

		client.SetCallback(callback)
		test.AssertNoErr(client.SubscribeTo("test1/test/test"))
		test.AssertNoErr(client.SubscribeTo("test2/test/test"))
		test.AssertNoErr(client.SubscribeTo("test3/test/test"))

		test.restartServer()

		// Confirm replay after reconnection
		test.WaitFor("to receive workspace reconnection", func() bool {
			defer mutex.Lock().Unlock()
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
			defer mutex.Lock().Unlock()
			_, exists := updated["test2/test/test"]
			return exists
		})

		defer mutex.Lock().Unlock()
		_, exists := updated["test/test/test"]
		test.Assert(!exists, "Invalid workspace notification received")
	})
}

func TestAdvanceNonExistentWorkspace(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		_, err := client.AdvanceWorkspace(test.ctx, "test", "test",
			"test", quantumfs.WorkspaceNonce{},
			quantumfs.EmptyWorkspaceKey, quantumfs.ZeroKey)
		wsdbErr, ok := err.(quantumfs.WorkspaceDbErr)
		test.Assert(ok, "Error isn't WorkspaceDbErr: %s", err.Error())

		test.Assert(wsdbErr.Code == quantumfs.WSDB_WORKSPACE_NOT_FOUND,
			"Non-existent workspace was found: %s", err.Error())
	})
}

func TestFatalErrorNoRetry(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		err := client.DeleteWorkspace(test.ctx, "", "", "invalid")
		test.AssertErr(err)
	})
}
