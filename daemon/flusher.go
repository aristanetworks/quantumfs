// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This file has the implementation of the flusher. For every workspace,
// a goroutine will be launched to flush its dirty inodes. Once everything
// is flushed, the goroutine terminates.

// Almost all of the functions in this module require the global flusher
// lock which is a single lock (per qfs instance) protecting all flushers.

package daemon

import (
	"container/list"
	"fmt"
	"strings"
	"time"

	"github.com/aristanetworks/quantumfs/utils"
)

const flushSanityTimeout = time.Minute

type dirtyInode struct {
	inode               Inode
	shouldUninstantiate bool
	expiryTime          time.Time
}

type FlushCmd int

const (
	KICK = FlushCmd(iota)
	QUIT
	QUITANDLOCK
	ABORT // N.B. ABORT is unused at the moment
)

type DirtyQueue struct {
	// The Front of the list are the Inodes next in line to flush.
	l    *list.List
	cmd  chan FlushCmd
	done chan error
	name string
	treelock *TreeLock
}

func NewDirtyQueue(treelock *TreeLock) *DirtyQueue {
	dq := DirtyQueue{
		l: list.New(),
		// We would like to allow a large number of
		// cmds to be queued for the flusher thread
		// without the callers worrying about blocking
		// This should change to consolidate all KICKs into one
		cmd:  make(chan FlushCmd, 1000),
		done: make(chan error),
		name: treelock.name,
		treelock: treelock,
	}
	return &dq
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) Len_() int {
	return dq.l.Len()
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) Remove_(element *list.Element) {
	dq.l.Remove(element)
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) Front_() *list.Element {
	return dq.l.Front()
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) PushBack_(v interface{}) *list.Element {
	return dq.l.PushBack(v)
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) PushFront_(v interface{}) *list.Element {
	return dq.l.PushFront(v)
}

// Try sending a command to the dirtyqueue, failing immediately
// if it would have blocked
// flusher lock must be locked when calling this function
func (dq *DirtyQueue) TryCommand_(c *ctx, cmd FlushCmd) error {
	c.vlog("Sending cmd %d to dirtyqueue %s", cmd, dq.name)
	select {
	case dq.cmd <- cmd:
		return nil
	default:
		c.vlog("sending cmd %d would have blocked", cmd)
		return fmt.Errorf("sending cmd %d would have blocked", cmd)
	}
}

// treeLock and flusher lock must be locked when calling this function
func flushCandidate_(c *ctx, dirtyInode *dirtyInode) bool {
	// We must release the flusher lock because when we flush
	// an Inode it will modify its parent and likely place that
	// parent onto the dirty queue. If we still hold that lock
	// we'll deadlock. We defer relocking in order to balance
	// against the deferred unlocking from our caller, even in
	// the case of a panic.
	uninstantiate := dirtyInode.shouldUninstantiate
	inode := dirtyInode.inode
	ret := func() bool {
		c.qfs.flusher.lock.Unlock()
		defer c.qfs.flusher.lock.Lock()
		return c.qfs.flushInode_(c, inode, uninstantiate)
	}()
	if !uninstantiate && dirtyInode.shouldUninstantiate {
		// we have released and re-acquired the flusher lock, and the
		// dirtyInode is now up for uninstantiation. This transition
		// cannot happen again, so it is safe to release the lock again.
		c.qfs.flusher.lock.Unlock()
		defer c.qfs.flusher.lock.Lock()
		c.qfs.uninstantiateInode(c, inode.inodeNum())
	}
	return ret
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) handleFlushError_(c *ctx, inodeId InodeId) {
	// Release the flusher lock as the caller may not be waiting for us yet
	c.qfs.flusher.lock.Unlock()
	defer c.qfs.flusher.lock.Lock()
	// Unblock the waiter with an error message as
	// the flushing hit an error in this iteration
	dq.done <- fmt.Errorf("Flushing inode %d failed", inodeId)
}

// treeLock and flusher lock must be locked when calling this function
func (dq *DirtyQueue) flushQueue_(c *ctx, flushAll bool) (next time.Time,
	done bool) {

	defer c.FuncIn("DirtyQueue::flushQueue_", "flushAll %t", flushAll).Out()
	defer logRequestPanic(c)
	next, done = time.Now(), false

	for dq.Len_() > 0 {
		// Should we clean this inode?
		element := dq.Front_()
		candidate := element.Value.(*dirtyInode)

		now := time.Now()
		if !flushAll && candidate.expiryTime.After(now) {
			// all expiring inodes have been flushed
			return candidate.expiryTime, false
		}
		if !flushCandidate_(c, candidate) {
			candidate.expiryTime = time.Now().Add(
				c.qfs.config.DirtyFlushDelay)
			if flushAll {
				dq.handleFlushError_(c, candidate.inode.inodeNum())
			}
			return candidate.expiryTime, false
		}
		dq.Remove_(element)
	}
	return time.Now(), true
}

func getSleepTime(c *ctx, nextExpiringInode time.Time) time.Duration {
	sleepTime := nextExpiringInode.Sub(time.Now())
	if sleepTime > flushSanityTimeout {
		c.elog("Overlong flusher sleepTime %s!", sleepTime)
		sleepTime = flushSanityTimeout
	}
	if sleepTime < time.Millisecond {
		c.vlog("Do not allow busywaiting in the flusher")
		sleepTime = time.Millisecond
	}
	c.vlog("Waiting until %s, sleepTime %s",
		nextExpiringInode.String(), sleepTime.String())
	return sleepTime
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) flush_(c *ctx) {
	defer c.FuncIn("DirtyQueue::flush_", "%s", dq.name).Out()
	// When we think we have no inodes try periodically anyways to ensure sanity
	nextExpiringInode := time.Now().Add(flushSanityTimeout)
	done := false
	for dq.Len_() > 0 && !done {
		sleepTime := getSleepTime(c, nextExpiringInode)
		cmd := KICK
		func() {
			c.qfs.flusher.lock.Unlock()
			defer c.qfs.flusher.lock.Lock()
			select {
			case cmd = <-dq.cmd:
				c.vlog("dirtyqueue received cmd %d", cmd)
			case <-time.After(sleepTime):
				c.vlog("flusher woken up due to timer")
			}
		}()

		flushAll := false
		switch cmd {
		case KICK:
		case QUITANDLOCK:
			dq.treelock.lock.Lock()
			defer dq.treelock.lock.Unlock()
			fallthrough
		case QUIT:
			flushAll = true
		case ABORT:
			return
		}
		nextExpiringInode, done = dq.flushQueue_(c, flushAll)
	}
}

type Flusher struct {
	// This is a map from the treeLock to a list of dirty inodes. We use the
	// treelock because every Inode already has the treelock of its workspace so
	// this is an easy way to sort Inodes by workspace.
	dqs  map[*TreeLock]*DirtyQueue
	lock utils.DeferableMutex
}

func NewFlusher() *Flusher {
	dqs := Flusher{
		dqs: make(map[*TreeLock]*DirtyQueue),
	}
	return &dqs
}

// If a workspace is specified, the treelock must already be locked.
// If sync all is specified, no treelock should be locked already
func (flusher *Flusher) sync_(c *ctx, workspace string) error {
	defer c.FuncIn("Flusher::sync", "%s", workspace).Out()
	doneChannels := make([]chan error, 0)
	var err error
	func() {
		defer flusher.lock.Lock().Unlock()
		// We cannot close the channel, as we have to distinguish between
		// ABORT and QUIT commands.
		c.vlog("Flusher: %d dirty queues should finish off",
			len(flusher.dqs))
		for _, dq := range flusher.dqs {
			if workspace != "" &&
				!strings.HasPrefix(workspace, dq.name) {
				continue
			}

			cmd := QUIT
			// If you specify a workspace, we assume the tree is already
			// locked before calling this function
			if workspace == "" {
				cmd = QUITANDLOCK
			}

			err = dq.TryCommand_(c, cmd)
			if err != nil {
				c.vlog("failed to send cmd to dirtyqueue")
				return
			}
			doneChannels = append(doneChannels, dq.done)
		}
	}()

	for _, doneChan := range doneChannels {
		if e := <-doneChan; e != nil {
			c.vlog("failed to sync dirty queue %s", e.Error())
			if err == nil {
				err = e
			}
		}
	}
	return err
}

func (flusher *Flusher) syncAll(c *ctx) error {
	defer c.funcIn("Flusher::syncAll").Out()
	return flusher.sync_(c, "")
}

// Must be called with the tree locked
func (flusher *Flusher) syncWorkspace_(c *ctx, workspace string) error {
	defer c.FuncIn("Flusher::syncWorkspace_", "%s", workspace).Out()
	return flusher.sync_(c, workspace)
}

// flusher lock must be locked when calling this function
func (flusher *Flusher) queue_(c *ctx, inode Inode,
	shouldUninstantiate bool, shouldWait bool) *list.Element {

	defer c.FuncIn("Flusher::queue_", "inode %d uninstantiate %t wait %t",
		inode.inodeNum(), shouldUninstantiate, shouldWait).Out()

	var dirtyNode *dirtyInode
	dirtyElement := inode.dirtyElement_()
	launch := false
	if dirtyElement == nil {
		// This inode wasn't in the dirtyQueue so add it now
		dirtyNode = &dirtyInode{
			inode:               inode,
			shouldUninstantiate: shouldUninstantiate,
		}

		treelock := inode.treeLock()
		dq, ok := flusher.dqs[treelock]
		if !ok {
			dq = NewDirtyQueue(treelock)
			flusher.dqs[treelock] = dq
			launch = true
		}

		if shouldWait {
			dirtyNode.expiryTime =
				time.Now().Add(c.qfs.config.DirtyFlushDelay)

			dirtyElement = dq.PushBack_(dirtyNode)
		} else {
			dirtyNode.expiryTime = time.Now()
			dirtyElement = dq.PushFront_(dirtyNode)
		}
	} else {
		dirtyNode = dirtyElement.Value.(*dirtyInode)
		c.vlog("Inode was already in the dirty queue %s",
			dirtyNode.expiryTime.String())
		dirtyNode.expiryTime = time.Now()
	}
	if shouldUninstantiate {
		dirtyNode.shouldUninstantiate = true
	}

	treelock := inode.treeLock()
	dq := flusher.dqs[treelock]
	if launch {
		go func() {
			nc := c.flusherCtx()
			defer flusher.lock.Lock().Unlock()
			dq.flush_(nc)
			close(dq.done)
			delete(flusher.dqs, treelock)
		}()
	}
	// N.B. dq might be nil if the flusher goroutines are ABORTed,
	// ABORT is not currently used anywhere, therefore dq must not be nil
	dq.TryCommand_(c, KICK)
	return dirtyElement
}

func (flusher *Flusher) dirtyQueueLength(wsr *WorkspaceRoot) int {
	defer flusher.lock.Lock().Unlock()

	dq := flusher.dqs[wsr.treeLock()]
	if dq == nil {
		return 0
	}

	return dq.Len_()
}
