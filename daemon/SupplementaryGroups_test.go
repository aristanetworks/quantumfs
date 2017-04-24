// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test parsing supplementary groups

import "syscall"
import "testing"

func TestGroupParsing(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		defer test.setGroups([]int{0, 30, 200})()

		pid := uint32(syscall.Gettid())

		// Sanity test that we fail to match when there is no match
		test.Assert(!hasMatchingGid(test.newCtx(), 99, pid, 1000),
			"No matching failed")

		// Match the passed in gid even if it isn't in the groups line
		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 99),
			"Matching failed")

		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 0),
			"Matching failed")

		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 30),
			"Matching failed")

		test.Assert(hasMatchingGid(test.newCtx(), 99, pid, 200),
			"Matching failed")
	})
}
