// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

type locker int
const (
	lockerMapMutexLock locker = iota
	lockerFlusherLock
	lockerLinkLock
	lockerChildRecordLock
	lockerInodeLock
	lockerParentLock
}

type lockInfo struct {
	lockType	locker
	inode		InodeId
}

type lockOrder struct {
	mapMutex	utils.DeferableMutex

	
}
