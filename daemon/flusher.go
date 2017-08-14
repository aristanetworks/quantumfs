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
	"sync"
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
	ABORT
)

type DirtyQueue struct {
	// The Front of the list are the Inodes next in line to flush.
	l    *list.List
	cmd  chan FlushCmd
	done chan error
}

func NewDirtyQueue() *DirtyQueue {
	dq := DirtyQueue{
		l: list.New(),
		// We would like to allow a large number of
		// cmds to be queued for the flusher thread
		// without the callers worrying about blocking
		// This should change to consolidate all KICKs into one
		cmd:  make(chan FlushCmd, 1000),
		done: make(chan error),
	}
	return &dq
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) Len_() int {
	return dq.l.Len()
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) PopFront_() {
	dq.l.Remove(dq.l.Front())
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

// flusher lock must be locked when calling this function
func flushCandidate_(c *ctx, dirtyInode dirtyInode) bool {
	// We must release the flusher lock because when we flush
	// an Inode it will modify its parent and likely place that
	// parent onto the dirty queue. If we still hold that lock
	// we'll deadlock. We defer relocking in order to balance
	// against the deferred unlocking from our caller, even in
	// the case of a panic.
	c.qfs.flusher.lock.Unlock()
	defer c.qfs.flusher.lock.Lock()
	return c.qfs.flushInode(c, dirtyInode)
}

// flusher lock must be locked when calling this function
func (dq *DirtyQueue) flush_(c *ctx) {
	defer c.funcIn("DirtyQueue::flush").Out()
	// When we think we have no inodes try periodically anyways to ensure sanity
	nextExpiringInode := time.Now().Add(flushSanityTimeout)
	done := false
	for dq.Len_() > 0 && !done {
		sleepTime := nextExpiringInode.Sub(time.Now())

		if sleepTime > flushSanityTimeout {
			c.elog("Overlong flusher sleepTime %s!", sleepTime)
			sleepTime = flushSanityTimeout
		}
		cmd := KICK
		if sleepTime > 0 {
			c.vlog("Waiting until %s (%s)...",
				nextExpiringInode.String(), sleepTime.String())
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
		}
		switch cmd {
		case KICK:
			if c.qfs.flusher.skip {
				continue
			}
		case QUIT:
		case ABORT:
			return
		}

		nextExpiringInode, done = func() (time.Time, bool) {
			defer logRequestPanic(c)
			for dq.Len_() > 0 {
				// Should we clean this inode?
				candidate := dq.Front_().Value.(*dirtyInode)

				now := time.Now()
				if cmd != QUIT && candidate.expiryTime.After(now) {
					// all expiring inodes have been flushed
					return candidate.expiryTime, false
				}
				if !flushCandidate_(c, *candidate) {
					candidate.expiryTime = time.Now().Add(
						c.qfs.config.DirtyFlushDelay)
					if cmd == QUIT {
						dq.done <- fmt.Errorf("Failed to flush inode %d",
							candidate.inode.inodeNum())
					}
					return candidate.expiryTime, false
				}
				dq.PopFront_()
			}
			return time.Now(), true
		}()
	}
}

type Flusher struct {
	// This is a map from the treeLock to a list of dirty inodes. We use the
	// treelock because every Inode already has the treelock of its workspace so
	// this is an easy way to sort Inodes by workspace.
	dqs  map[*sync.RWMutex]*DirtyQueue
	lock utils.DeferableMutex
	// Set to true to disable timer based flushing. Use for tests only
	skip bool
}

func NewFlusher() *Flusher {
	dqs := Flusher{
		dqs:  make(map[*sync.RWMutex]*DirtyQueue),
		skip: false,
	}
	return &dqs
}

func (flusher *Flusher) sync(c *ctx, force bool) error {
	defer c.FuncIn("Flusher::sync", "%t", force).Out()
	doneChannels := make([]chan error, 0)
	func() {
		defer flusher.lock.Lock().Unlock()
		// The cmd channels are buffered, so the fact that we hold the lock
		// must not cause deadlock.
		// We cannot close the channel, as we have to distinguish between
		// ABORT and QUIT commands.
		c.vlog("Flusher: %d dirty queues should finish off",
			len(flusher.dqs))
		for _, dq := range flusher.dqs {
			if force || !flusher.skip {
				dq.cmd <- QUIT
			} else {
				dq.cmd <- ABORT
			}
			doneChannels = append(doneChannels, dq.done)
		}
	}()

	for _, c := range doneChannels {
		if err := <-c; err != nil {
			return err
		}
	}
	return nil
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
			dq = NewDirtyQueue()
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
			defer flusher.lock.Lock().Unlock()
			// KICK start the flusher
			dq.cmd <- KICK
			dq.flush_(c)
			close(dq.done)
			delete(flusher.dqs, treelock)
		}()
	} else {
		select {
		case dq.cmd <- KICK:
		default:
		}
	}
	return dirtyElement
}
