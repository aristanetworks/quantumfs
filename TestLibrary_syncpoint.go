// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import (
	"time"
)

type SyncState int64

type Logger func(format string, args ...interface{}) error

const (
	InvalidSyncState = SyncState(-1)
)

type SyncPoint struct {
	signalchan chan SyncState
	waitchan   chan SyncState
	die        chan interface{}
	logger     Logger
	state      SyncState
}

func NewSyncPoint(logger Logger) *SyncPoint {
	var syncpoint SyncPoint

	syncpoint.signalchan = make(chan SyncState, 1)
	syncpoint.waitchan = make(chan SyncState, 1)
	syncpoint.die = make(chan interface{}, 1)
	syncpoint.state = InvalidSyncState
	syncpoint.logger = logger
	go syncpoint.daemon()
	return &syncpoint
}

func (syncpoint *SyncPoint) daemon() {
	for {
		select {
		case <-syncpoint.die:
			syncpoint.logger("SyncPoint died at %d", syncpoint.state)
			return
		case state := <-syncpoint.signalchan:
			syncpoint.logger("transition %d -> %d",
				syncpoint.state, state)
			syncpoint.state = state
			// Tell all the blocked waiters that the state has changed
			// break out as soon as no waiter is reading
			for done := false; !done; {
				select {
				case syncpoint.waitchan <- syncpoint.state:
				default:
					done = true
					break
				}
			}
		}
	}
}

func (syncpoint *SyncPoint) WaitFor(expected SyncState) bool {
	syncpoint.logger("Waiting for state %d", expected)
	npolls := 0
	for {
		select {
		case <-syncpoint.die:
			syncpoint.logger("SyncPoint died")
			return false
		case state := <-syncpoint.waitchan:
			if state == expected {
				syncpoint.logger("got to state %d", state)
				return true
			}
			syncpoint.logger("waiting for state %d, currently at %d",
				expected, state)
			continue
		case <-time.After(5 * time.Millisecond):
			// Poll the state in case we missed an update
			npolls++
			if npolls > 100 {
				syncpoint.logger("timed out waiting for state %d",
					expected)
				return false
			}
			// Stale reads are alright, no need for atomic operations
			if syncpoint.state == expected {
				return true
			}
		}
	}
}

func (syncpoint *SyncPoint) Signal(state SyncState) {
	syncpoint.logger("Signalling state %d", state)
	syncpoint.signalchan <- state
}

func (syncpoint *SyncPoint) Die() {
	close(syncpoint.die)
}
