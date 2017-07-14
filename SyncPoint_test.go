// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import (
	"testing"
)

func TestSyncPointInvalidSyncState(t *testing.T) {
	runTest(t, func(test *testHelper) {
		syncpoint := NewSyncPoint(test.Log)
		test.Assert(syncpoint.WaitFor(InvalidSyncState),
			"Failed to wait reach state 1")
		syncpoint.Die()
	})
}

func TestSyncPointOneSyncState(t *testing.T) {
	runTest(t, func(test *testHelper) {
		syncpoint := NewSyncPoint(test.Log)
		syncpoint.Signal(1)
		test.Assert(syncpoint.WaitFor(1), "Failed to wait reach state 1")
	})
}

func TestSyncPointUnreachableSyncState(t *testing.T) {
	runTest(t, func(test *testHelper) {
		syncpoint := NewSyncPoint(test.Log)
		test.Assert(!syncpoint.WaitFor(1), "Got to state 1 without signal")
	})
}

func TestSyncPointMultiSyncState(t *testing.T) {
	runTest(t, func(test *testHelper) {
		const (
			SyncState1 = SyncState(iota)
			SyncState2
		)
		syncpoint := NewSyncPoint(test.Log)
		syncpoint.Signal(SyncState1)
		test.Assert(syncpoint.WaitFor(SyncState1), "Not at state1")
		syncpoint.Signal(SyncState2)
		test.Assert(syncpoint.WaitFor(SyncState2), "Not at state2")
		test.Assert(!syncpoint.WaitFor(SyncState1), "must not be at state1")
		syncpoint.Signal(SyncState1)
		test.Assert(syncpoint.WaitFor(SyncState1), "Not at state1")
	})
}

func TestSyncPointMultiWaiters(t *testing.T) {
	runTest(t, func(test *testHelper) {
		const (
			SyncState1 = SyncState(iota + 1)
			SyncState2
			SyncState3
			SyncState4
		)
		syncpoint := NewSyncPoint(test.Log)
		failSyncState := 0
		go func() {
			if !syncpoint.WaitFor(SyncState1) {
				failSyncState = 1
			}
			syncpoint.Signal(SyncState2)
			if !syncpoint.WaitFor(SyncState3) {
				failSyncState = 3
			}
			syncpoint.Signal(SyncState4)
		}()
		syncpoint.Signal(SyncState1)
		if !syncpoint.WaitFor(SyncState2) {
			failSyncState = 2
		}
		syncpoint.Signal(SyncState3)
		if !syncpoint.WaitFor(SyncState4) {
			failSyncState = 4
		}
		test.Assert(failSyncState == 0, "Failed to reach state %d",
			failSyncState)
	})
}
