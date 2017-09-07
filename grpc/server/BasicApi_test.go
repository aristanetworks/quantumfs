// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package server

// Tests of the basic API running on top of a processlocal backing store.

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
)

func TestConnect(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		num, err := client.NumTypespaces(test.ctx)
		test.AssertNoErr(err)
		test.Assert(num == 1, "Unexpected number of typespaces %d", num)
	})
}

func TestSubscribeAndNotify(t *testing.T) {
	runTest(t, func(test *testHelper) {
		client := test.newClient()

		called := false
		callback := func(updates map[string]quantumfs.WorkspaceState) {
			test.Assert(len(updates) == 1, "Wrong number of updates: %d",
				len(updates))
			called = true
		}

		client.SetCallback(callback)

		test.AssertNoErr(client.SubscribeTo("test/test/test"))

		test.AssertNoErr(client.BranchWorkspace(test.ctx,
			quantumfs.NullSpaceName, quantumfs.NullSpaceName,
			quantumfs.NullSpaceName, "test", "test", "test"))

		test.WaitFor("Callback to be invoked", func() bool { return called })
	})
}
