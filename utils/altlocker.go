// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

type AlternatingLocker struct {
	aSideLock    DeferableMutex
	aSideCounter int

	bSideLock DeferableRwMutex
}

// Acquire the a side lock for reading and increment the counter
func (alt *AlternatingLocker) ALock() {
	// protect the counter
	defer alt.aSideLock.Lock().Unlock()

	if alt.aSideCounter == 0 {
		// We've been letting the b side run, so we need to lock them out
		alt.bSideLock.Lock()
	}

	alt.aSideCounter++
}

func (alt *AlternatingLocker) AUnlock() {
	defer alt.aSideLock.Lock().Unlock()

	// Decrement the counter and see if it's time to unlock the b side
	alt.aSideCounter--

	if alt.aSideCounter < 0 {
		panic("Counting mismatch in aUnlock")
	} else if alt.aSideCounter == 0 {
		alt.bSideLock.Unlock()
	}
}

func (alt *AlternatingLocker) RLock() NeedReadUnlock {
	return alt.bSideLock.RLock()
}
