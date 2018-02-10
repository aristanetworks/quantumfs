// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/silentred/gid"
)

// Mutex type which can have unlocking deferred. ie: defer df.Lock().Unlock()
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

// RWMutex type which can have the unlocking deferred. ie: defer drm.Lock().Unlock()
// or defer drm.RLock().RUnlock().
type DeferableRwMutex struct {
	lock sync.RWMutex

	// IDs of goroutines which are holding this lock for read
	readHolderLock DeferableMutex
	readHolders    map[int64]uintptr
}

// Set to true to have DeferableRwMutex panic if a single goroutine attempts to call
// RLock() on the same lock more than once.
var CheckForRecursiveRLock bool

func (df *DeferableRwMutex) RLock() NeedReadUnlock {
	if CheckForRecursiveRLock {
		defer df.readHolderLock.Lock().Unlock()
		goid := gid.Get()
		if df.readHolders == nil {
			df.readHolders = make(map[int64]uintptr)
		}
		pc, alreadyHeld := df.readHolders[goid]
		if alreadyHeld {
			f := runtime.FuncForPC(pc)
			file, line := f.FileLine(pc)
			location := fmt.Sprintf("%s:%d", file, line)

			Assert(!alreadyHeld,
				"goroutine %d attempted to RLock twice, "+
					"previously at %s!", goid, location)
		}

		pc, _, _, _ = runtime.Caller(1)
		df.readHolders[goid] = pc
	}

	df.lock.RLock()
	return df
}

func (df *DeferableRwMutex) RUnlock() {
	if CheckForRecursiveRLock {
		defer df.readHolderLock.Lock().Unlock()
		delete(df.readHolders, gid.Get())
	}
	df.lock.RUnlock()
}

func (df *DeferableRwMutex) Lock() NeedWriteUnlock {
	df.lock.Lock()
	return &df.lock
}

func (df *DeferableRwMutex) Unlock() {
	df.lock.Unlock()
}

var locklogname string

func init() {
	locklogname = os.Getenv("LOCK_LOG_FILE")
	if locklogname == "" {
		locklogname = "/tmp/qfs_locking_logs"
	}
}

func locklog(id int, str string) {
	f, err := os.OpenFile(locklogname,
		os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	Assert(err == nil, "can't open file %v", err)
	defer f.Close()
	_, err = f.WriteString(
		fmt.Sprintf("%s:%d:%s\n", str, id, callername(3)))
	Assert(err == nil, "can't write to file %v", err)
}

// In order to use this rwlock, replace the type of the DeferableRwMutex
// to LoggingDeferableRwMutex and then set the Id of the instance
// if necessary
type LoggingDeferableRwMutex struct {
	lock sync.RWMutex
	Id   int
}

func (df *LoggingDeferableRwMutex) RLock() NeedReadUnlock {
	locklog(df.Id, "RL")
	df.lock.RLock()
	return &df.lock
}

func (df *LoggingDeferableRwMutex) Lock() NeedWriteUnlock {
	locklog(df.Id, "L")
	df.lock.Lock()
	return &df.lock
}

func (df *LoggingDeferableRwMutex) RUnlock() {
	locklog(df.Id, "RUL")
	df.lock.RUnlock()
}

func (df *LoggingDeferableRwMutex) Unlock() {
	locklog(df.Id, "UL")
	df.lock.Unlock()
}

// In order to use this rwlock, on the locking side, substitute lock.Lock() with
// utils.NewWrappingDeferableRwMutex(nil, lock.Lock(), id)
// and substitute lock.RLock() with
// utils.NewWrappingDeferableRwMutex(nil, lock.RLock(), id)
type WrappingDeferableRwMutex struct {
	rlock NeedReadUnlock
	wlock NeedWriteUnlock
	id    int
}

func NewWrappingDeferableRwMutex(rlock NeedReadUnlock, wlock NeedWriteUnlock,
	id int) *WrappingDeferableRwMutex {

	var str string
	if rlock == nil {
		str = "L"
	} else {
		str = "RL"
	}
	l := WrappingDeferableRwMutex{
		rlock: rlock,
		wlock: wlock,
		id:    id,
	}
	locklog(l.id, str)
	return &l
}

func (l *WrappingDeferableRwMutex) Unlock() {
	locklog(l.id, "UL")
	l.wlock.Unlock()
}

func (l *WrappingDeferableRwMutex) RUnlock() {
	locklog(l.id, "RUL")
	l.rlock.RUnlock()
}
