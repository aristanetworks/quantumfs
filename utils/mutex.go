// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "sync"

type DeferableMutex struct {
	lock sync.Mutex
}

func (df *DeferableMutex) Lock() *sync.Mutex {
	df.lock.Lock()
	return &df.lock
}

func (df *DeferableMutex) Unlock() {
	df.lock.Unlock()
}

// Return the lock via a tiny interface to prevent read/write lock/unlock mismatch
type NeedReadUnlock interface {
	RUnlock()
}

type NeedWriteUnlock interface {
	Unlock()
}

type DeferableRwMutex struct {
	lock sync.RWMutex
}

func (df *DeferableRwMutex) RLock() NeedReadUnlock {
	df.lock.RLock()
	return &df.lock
}

func (df *DeferableRwMutex) Lock() NeedWriteUnlock {
	df.lock.Lock()
	return &df.lock
}

func (df *DeferableRwMutex) RUnlock() {
	df.lock.RUnlock()
}

func (df *DeferableRwMutex) Unlock() {
	df.lock.Unlock()
}
