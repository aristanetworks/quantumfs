// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"
	"runtime/debug"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type locker int

// These enums are specifically in the locking order, leafs first
const (
	lockerMapMutexLock locker = iota
	lockerLookupCountLock
	lockerFlusherLock
	lockerLinkLock
	lockerChildRecordLock
	lockerInodeLock
	lockerParentLock
	lockerInstantiationLock
	lockerTreeLock
)

type lockInfo struct {
	kind      locker
	inode     InodeId
	heldSince time.Time
}

func newLockInfoQuick(k locker, i InodeId) lockInfo {
	return lockInfo{
		kind:  k,
		inode: i,
	}
}

func newLockInfo(k locker, i InodeId, t time.Time) lockInfo {
	return lockInfo{
		kind:      k,
		inode:     i,
		heldSince: t,
	}
}

type lockOrder struct {
	stack    []lockInfo
	disabled bool
	timings  bool // Disable during production for performance
}

// The lock being requested must already be held so we can do checks with it
func (order *lockOrder) Push_(c *ctx, inode InodeId, kind locker, lock func()) {
	ensuredLock := callOnce(lock)
	defer ensuredLock.invoke()

	if c.qfs.disableLockChecks || order.disabled {
		return
	}

	var gotLock time.Time
	if order.timings {
		beforeLock := time.Now()
		ensuredLock.invoke()
		gotLock = time.Now()

		c.vlog("Locks: %d took %d ns to get", int64(kind),
			gotLock.Sub(beforeLock).Nanoseconds())
	} else {
		ensuredLock.invoke()
	}

	// Some of these checks require that the lock we're pushing has already been
	// acquired, so make sure ensuredLock is invoked first
	if len(order.stack) != 0 {
		if isInodeLock(kind) {
			order.checkInodeOrder(c, inode, kind)
		} else {
			// Outside of inode locks, all locks should be decreasing
			lastLock := order.stack[len(order.stack)-1]
			if lastLock.kind < kind {
				order.alertInversion(c, inode, kind)
			}
		}
	}

	var newInfo lockInfo
	if order.timings {
		newInfo = newLockInfo(kind, inode, gotLock)
	} else {
		newInfo = newLockInfoQuick(kind, inode)
	}
	order.stack = append(order.stack, newInfo)
}

func (order *lockOrder) Remove(c *ctx, inode InodeId, kind locker) {
	if c.qfs.disableLockChecks || order.disabled {
		return
	}

	if len(order.stack) <= 0 {
		c.elog("Empty stack got a pop %v",
			utils.BytesToString(debug.Stack()))
	}

	// Removing has an odd mechanism in that we don't necessarily unlock in the
	// same order that we locked. We can't assume we're popping the last element,
	// have to find it instead and remove it
	removeIdx := -1
	for i := len(order.stack) - 1; i >= 0; i-- {
		entry := order.stack[i]
		if entry.inode == inode && entry.kind == kind {
			removeIdx = i
			break
		}
	}

	if removeIdx == -1 {
		c.elog("Unable to find lock we're popping, %d %d",
			int64(kind), int64(inode))
		order.printStack(c)
		return
	}

	if order.timings {
		c.vlog("Locks: %d held for %d ns", int64(kind),
			time.Since(order.stack[removeIdx].heldSince).Nanoseconds())
	}

	order.stack = append(order.stack[:removeIdx], order.stack[removeIdx+1:]...)
}

func isInodeLock(kind locker) bool {
	return kind == lockerChildRecordLock || kind == lockerInodeLock ||
		kind == lockerParentLock
}

func (order *lockOrder) checkInodeOrder(c *ctx, inode InodeId, kind locker) {

	// Only if the previous inode is the same as our current, can we
	// expect the kind to stay decreasing
	lastLock := order.stack[len(order.stack)-1]
	if lastLock.inode == inode && lastLock.kind < kind {
		order.alertInversion(c, inode, kind)
		return
	}

	if kind != lockerParentLock {
		// Can't really check any more than this
		return
	}

	lockingInode, release := c.qfs.inodeNoInstantiate(c, inode)
	defer release()
	if lockingInode == nil {
		// If we're locking for a new inode that isn't in the inode map yet,
		// don't bother checking for now
		return
	}

	// Since we have the parentLock we can grab the parent inode id
	parent := lockingInode.parentId_()

	// Iterating backwards, we should never see our parent, or any other inode
	// type lock from this inode
	for i := len(order.stack) - 1; i >= 0; i-- {
		entry := order.stack[i]
		if entry.inode == parent {
			order.alertInversion(c, inode, kind)
			return
		}

		if entry.inode == inode && isInodeLock(entry.kind) {
			order.alertInversion(c, inode, kind)
			return
		}
	}
}

const lockInversionLog = "Lock inversion detected. New Lock %d Inode %d\n%v"

func (order *lockOrder) alertInversion(c *ctx, inode InodeId, kind locker) {
	c.elog(lockInversionLog, int64(kind), int64(inode),
		utils.BytesToString(debug.Stack()))
	order.printStack(c)
}

func (order *lockOrder) printStack(c *ctx) {
	stackStr := ""
	for _, info := range order.stack {
		stackStr += fmt.Sprintf("Lock %d Inode %d\n", int64(info.kind),
			int64(info.inode))
	}

	c.elog("Stack: %s\n%s", stackStr, utils.BytesToString(debug.Stack()))
}

// Generics
type orderedRwMutexUnlocker struct {
	mutex *utils.DeferableRwMutex

	inode InodeId
	kind  locker

	c *ctx
}

func (m *orderedRwMutexUnlocker) RUnlock() {
	defer m.mutex.RUnlock()
	m.c.lockOrder.Remove(m.c, m.inode, m.kind)
}

func (m *orderedRwMutexUnlocker) Unlock() {
	defer m.mutex.Unlock()
	m.c.lockOrder.Remove(m.c, m.inode, m.kind)
}

type orderedRwMutex struct {
	mutex utils.DeferableRwMutex
}

func (m *orderedRwMutex) RLock(c *ctx, inode InodeId,
	kind locker) utils.NeedReadUnlock {

	c.lockOrder.Push_(c, inode, kind, func() { m.mutex.RLock() })

	return &orderedRwMutexUnlocker{
		mutex: &m.mutex,
		inode: inode,
		kind:  kind,
		c:     c,
	}
}

func (m *orderedRwMutex) Lock(c *ctx, inode InodeId,
	kind locker) utils.NeedWriteUnlock {

	c.lockOrder.Push_(c, inode, kind, func() { m.mutex.Lock() })

	return &orderedRwMutexUnlocker{
		mutex: &m.mutex,
		inode: inode,
		kind:  kind,
		c:     c,
	}
}

func (m *orderedRwMutex) RUnlock(c *ctx, inode InodeId, kind locker) {
	defer m.mutex.RUnlock()
	c.lockOrder.Remove(c, inode, kind)
}

func (m *orderedRwMutex) Unlock(c *ctx, inode InodeId, kind locker) {
	defer m.mutex.Unlock()
	c.lockOrder.Remove(c, inode, kind)
}

type orderedMutex struct {
	mutex orderedRwMutex
}

func (m *orderedMutex) Lock(c *ctx, inode InodeId,
	kind locker) utils.NeedWriteUnlock {

	return m.mutex.Lock(c, inode, kind)
}

func (m *orderedMutex) Unlock(c *ctx, inode InodeId, kind locker) {
	m.mutex.Unlock(c, inode, kind)
}

// Lock types
type orderedMapMutex struct {
	mutex orderedRwMutex
}

func (m *orderedMapMutex) RLock(c *ctx) utils.NeedReadUnlock {
	return m.mutex.RLock(c, quantumfs.InodeIdInvalid, lockerMapMutexLock)
}

func (m *orderedMapMutex) Lock(c *ctx) utils.NeedWriteUnlock {
	return m.mutex.Lock(c, quantumfs.InodeIdInvalid, lockerMapMutexLock)
}

func (m *orderedMapMutex) Unlock(c *ctx) {
	m.mutex.Unlock(c, quantumfs.InodeIdInvalid, lockerMapMutexLock)
}

type orderedLookupCount struct {
	mutex orderedMutex
}

func (m *orderedLookupCount) Lock(c *ctx) utils.NeedWriteUnlock {
	return m.mutex.Lock(c, quantumfs.InodeIdInvalid, lockerLookupCountLock)
}

func (m *orderedLookupCount) Unlock(c *ctx) {
	m.mutex.Unlock(c, quantumfs.InodeIdInvalid, lockerLookupCountLock)
}

type orderedFlusher struct {
	mutex orderedMutex
}

func (m *orderedFlusher) Lock(c *ctx) utils.NeedWriteUnlock {
	return m.mutex.Lock(c, quantumfs.InodeIdInvalid, lockerFlusherLock)
}

func (m *orderedFlusher) Unlock(c *ctx) {
	m.mutex.Unlock(c, quantumfs.InodeIdInvalid, lockerFlusherLock)
}

type orderedInstantiation struct {
	mutex orderedMutex
}

func (m *orderedInstantiation) Lock(c *ctx) utils.NeedWriteUnlock {
	return m.mutex.Lock(c, quantumfs.InodeIdInvalid, lockerInstantiationLock)
}

func (m *orderedInstantiation) Unlock(c *ctx) {
	m.mutex.Unlock(c, quantumfs.InodeIdInvalid, lockerInstantiationLock)
}
