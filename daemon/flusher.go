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

type triggerCmd struct {
	flushAll bool
	finished chan struct{}
}

type FlushCmd int

const (
	KICK = FlushCmd(iota)
	FLUSHALL
	RETURN
)

type DirtyQueue struct {
	// The Front of the list are the Inodes next in line to flush.
	l        *list.List
	trigger  chan triggerCmd
	cmd      chan FlushCmd
	done     chan error
	treelock *TreeLock
}

func NewDirtyQueue(treelock *TreeLock) *DirtyQueue {
	dq := DirtyQueue{
		l:       list.New(),
		trigger: make(chan triggerCmd, 1000),
		// We would like to allow a large number of
		// cmds to be queued for the flusher thread
		// without the callers worrying about blocking
		// This should change to consolidate all KICKs into one
		cmd:      make(chan FlushCmd, 1000),
		done:     make(chan error),
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

func (dq *DirtyQueue) kicker(c *ctx) {
	defer logRequestPanic(c)

	// When we think we have no inodes try periodically anyways to ensure sanity
	nextExpiringInode := time.Now().Add(flushSanityTimeout)

	for {
		sleepTime := getSleepTime(c, nextExpiringInode)
		cmd := KICK
		done := false

		select {
		case cmd = <-dq.cmd:
			c.vlog("dq kicker cmd %d", cmd)
		case <-time.After(sleepTime):
			c.vlog("dq kicker woken up due to timer")
		}

		func() {
			dq.treelock.lock.RLock()
			defer dq.treelock.lock.RUnlock()

			doneChan := make(chan struct{})
			func() {
				defer c.qfs.flusher.lock.Lock().Unlock()

				// By the time we get the flusher lock, the flush
				// thread may be done and gone by now, so we have to
				// check before we even think of waiting on it
				if dq.Len_() == 0 {
					done = true
					return
				}

				switch cmd {
				case KICK:
					dq.trigger <- triggerCmd{
						flushAll: false,
						finished: doneChan,
					}
				case FLUSHALL:
					dq.trigger <- triggerCmd{
						flushAll: true,
						finished: doneChan,
					}
				case RETURN:
					c.elog("RETURN with non-empty dirty queue")
				default:
					c.elog("Unhandled flushing type")
				}
			}()

			if done {
				return
			}

			// With the flusher lock released, we've allowed the flush
			// thread to do its job and inform us when we can release
			// the treelock
			<-doneChan

			defer c.qfs.flusher.lock.Lock().Unlock()
			if dq.Len_() > 0 {
				element := dq.Front_()
				candidate := element.Value.(*dirtyInode)
				nextExpiringInode = candidate.expiryTime
			} else {
				done = true
			}
		}()

		if done {
			return
		}
	}
}

// Try sending a command to the dirtyqueue, failing immediately
// if it would have blocked
func (dq *DirtyQueue) TryCommand(c *ctx, cmd FlushCmd) error {
	c.vlog("Sending cmd %d to dirtyqueue %s", cmd, dq.treelock.name)
	select {
	case dq.cmd <- cmd:
		return nil
	default:
		c.vlog("sending cmd %d would have blocked", cmd)
		return fmt.Errorf("sending cmd %d would have blocked", cmd)
	}
}

// treeLock and flusher lock must be locked R/W when calling this function
func (dq *DirtyQueue) flushCandidate_(c *ctx, dirtyInode *dirtyInode) bool {
	// We must release the flusher lock because when we flush
	// an Inode it will modify its parent and likely place that
	// parent onto the dirty queue. If we still hold that lock
	// we'll deadlock. We defer relocking in order to balance
	// against the deferred unlocking from our caller, even in
	// the case of a panic.
	uninstantiate := dirtyInode.shouldUninstantiate
	inode := dirtyInode.inode

	dqlen := dq.Len_()
	ret := func() bool {
		c.qfs.flusher.lock.Unlock()
		defer c.qfs.flusher.lock.Lock()
		return c.qfs.flushInode_(c, inode, uninstantiate, dqlen <= 1)
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

// treeLock and flusher lock must be locked R/W when calling this function
func (dq *DirtyQueue) flushQueue_(c *ctx, flushAll bool) bool {

	defer c.FuncIn("DirtyQueue::flushQueue_", "flushAll %t", flushAll).Out()
	defer logRequestPanic(c)

	for dq.Len_() > 0 {
		// Should we clean this inode?
		element := dq.Front_()
		candidate := element.Value.(*dirtyInode)

		now := time.Now()
		if !flushAll && candidate.expiryTime.After(now) {
			// all expiring inodes have been flushed
			return false
		}

		if !dq.flushCandidate_(c, candidate) {
			candidate.expiryTime = time.Now().Add(
				c.qfs.config.DirtyFlushDelay)
			if flushAll {
				dq.handleFlushError_(c, candidate.inode.inodeNum())
			}
			return false
		}
		dq.Remove_(element)
	}
	return true
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

func (dq *DirtyQueue) flusher(c *ctx) {
	defer c.FuncIn("DirtyQueue::flush", "%s", dq.treelock.name).Out()
	defer logRequestPanic(c)
	done := false
	var empty struct{}

	for !done {
		trigger := <-dq.trigger
		c.vlog("trigger, flushAll: %v", trigger.flushAll)

		// NOTE: When we are triggered, the treelock *must* already
		// be locked by the caller (the thread that pushed into dq.trigger),
		// exclusively or not

		func() {
			defer c.qfs.flusher.lock.Lock().Unlock()
			done = dq.flushQueue_(c, trigger.flushAll)

			if dq.Len_() == 0 {
				done = true
			} else if done {
				c.elog("Done without empty dirty queue")
			}

			if done {
				// Cleanup
				close(dq.done)
				delete(c.qfs.flusher.dqs, dq.treelock)

				// end the kicker thread and cleanup any triggers
				dq.TryCommand(c, RETURN)
				close(dq.trigger)
				close(dq.cmd)
			}
		}()

		if trigger.finished != nil {
			trigger.finished <- empty
		}
	}

	// consume any leftover triggers
	for range dq.trigger {
		trigger := <-dq.trigger

		if trigger.finished != nil {
			trigger.finished <- empty
		}
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

		c.vlog("Flusher: %d dirty queues should finish off",
			len(flusher.dqs))
		for _, dq := range flusher.dqs {
			if workspace != "" {
				if !strings.HasPrefix(workspace, dq.treelock.name) {
					continue
				}

				// For a single specific workspace, we assume to
				// already have the treelock acquired, so trigger
				// flusher thread manually
				dq.trigger <- triggerCmd{
					flushAll: true,
					finished: nil,
				}
			} else {
				err = dq.TryCommand(c, FLUSHALL)
				if err != nil {
					c.vlog("failed to send cmd to dirtyqueue")
					return
				}
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
		nc := c.flusherCtx()

		go dq.flusher(nc)
		go dq.kicker(nc)
	}

	dq.TryCommand(c, KICK)
	return dirtyElement
}
