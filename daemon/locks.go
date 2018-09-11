// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)


type locker int
// These enums are specifically in the locking order, leafs first
const (
	lockerMapMutexLock locker = iota
	lockerFlusherLock
	lockerLinkLock
	lockerChildRecordLock
	lockerInodeLock
	lockerParentLock
	lockerInstantiationLock
	lockerTreeLock
)

type lockInfo struct {
	kind	locker
	inode		InodeId
}

type lockOrder struct {
	stack	[]lockInfo
}

// The lock being requested must already be held so we can do checks with it
func (order *lockOrder) Push_(c *ctx, inode InodeId, kind locker) {
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

	order.stack = append(order.stack, lockInfo {
		kind:	kind,
		inode:		inode,
	})
}

func (order *lockOrder) Pop(c *ctx, inode InodeId, kind locker) {
	c.Assert(len(order.stack) > 0, "Empty stack got a pop")

	// Popping has an odd mechanism in that we don't necessarily unlock in the
	// same order that we locked. We can't assume we're popping the last element,
	// have to find it instead and remove it
	removeIdx := -1
	for i := len(order.stack)-1; i >= 0; i-- {
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
	c.Assert(lockingInode != nil,
		"Somehow locking parentLock from uninstantiated inode")

	// Since we have the parentLock we can grab the parent inode id
	parent := lockingInode.parentId_()

	// Iterating backwards, we should never see our parent, or any other inode
	// type lock from this inode
	for i := len(order.stack)-1; i >= 0; i-- {
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

func (order *lockOrder) alertInversion(c *ctx, inode InodeId, kind locker) {
	c.elog("Lock inversion detected. New Lock %d Inode %d", int64(kind),
		int64(inode))
	order.printStack(c)
}

func (order *lockOrder) printStack(c *ctx) {
	stackStr := ""
	for _, info := range order.stack {
		stackStr += fmt.Sprintf("Lock %d Inode %d\n", int64(info.kind),
			int64(info.inode))
	}

	c.elog("Stack: %s", stackStr)
}

// Generics
type orderedRwMutexUnlocker struct {
	mutex	*utils.DeferableRwMutex

	inode	InodeId
	kind	locker

	c	*ctx
}

func (m *orderedRwMutexUnlocker) RUnlock() {
	defer m.mutex.RUnlock()
	m.c.lockOrder.Pop(m.c, m.inode, m.kind)
}

func (m *orderedRwMutexUnlocker) Unlock() {
	defer m.mutex.Unlock()
	m.c.lockOrder.Pop(m.c, m.inode, m.kind)
}

type orderedRwMutex struct {
	mutex utils.DeferableRwMutex
}

func (m *orderedRwMutex) RLock(c *ctx, inode InodeId,
	kind locker) utils.NeedReadUnlock {

	m.mutex.RLock()
	c.lockOrder.Push_(c, inode, kind)

	return &orderedRwMutexUnlocker {
		mutex:	&m.mutex,
		inode:	inode,
		kind:	kind,
		c:	c,
	}
}

func (m *orderedRwMutex) Lock(c *ctx, inode InodeId,
	kind locker) utils.NeedWriteUnlock {

	m.mutex.Lock()
	c.lockOrder.Push_(c, inode, kind)

	return &orderedRwMutexUnlocker {
		mutex:	&m.mutex,
		inode:	inode,
		kind:	kind,
		c:	c,
	}
}

func (m *orderedRwMutex) RUnlock(c *ctx, inode InodeId, kind locker) {
	defer m.mutex.RUnlock()
	c.lockOrder.Pop(c, inode, kind)
}

func (m *orderedRwMutex) Unlock(c *ctx, inode InodeId, kind locker) {
	defer m.mutex.Unlock()
	c.lockOrder.Pop(c, inode, kind)
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
	mutex	orderedRwMutex
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
