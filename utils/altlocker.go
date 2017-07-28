// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

type AlternatingLocker struct {
	slowSideLock    DeferableMutex
	slowSideCounter int

	fastSideLock DeferableRwMutex
}

// Acquire the slow side lock for reading and increment the counter
func (alt *AlternatingLocker) SlowLock() {
	// protect the counter
	defer alt.slowSideLock.Lock().Unlock()

	if alt.slowSideCounter == 0 {
		// We've been letting the fast side run, so we need to lock them out
		alt.fastSideLock.Lock()
	}

	alt.slowSideCounter++
}

func (alt *AlternatingLocker) SlowUnlock() {
	defer alt.slowSideLock.Lock().Unlock()

	// Decrement the counter and see if it's time to unlock the fast side
	alt.slowSideCounter--

	if alt.slowSideCounter < 0 {
		panic("Counting mismatch in slowUnlock")
	} else if alt.slowSideCounter == 0 {
		alt.fastSideLock.Unlock()
	}
}

func (alt *AlternatingLocker) FastLock() NeedReadUnlock {
	return alt.fastSideLock.RLock()
}
