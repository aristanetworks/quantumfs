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
	"time"

	"github.com/aristanetworks/quantumfs/utils"
)

const flusherSanityTimeout = time.Minute

type dirtyInode struct {
	inode               Inode
	shouldUninstantiate bool
	shouldFlush         bool
	expiryTime          time.Time
}

type triggerCmd struct {
	flushAll bool
	finished chan error
	ctx      *ctx
}

type FlushRequest struct {
	cmd      FlushCmd
	response chan error
}

type FlushCmd int

const (
	KICK = FlushCmd(iota)
	FLUSHALL
	RETURN
)

type DirtyQueue struct {
	// The Front of the list are the Inodes next in line to flush.
	l         *list.List
	trigger   chan triggerCmd
	cmd       chan FlushRequest
	treeState *TreeState
}

func NewDirtyQueue(treeState *TreeState) *DirtyQueue {
	dq := DirtyQueue{
		l:       list.New(),
		trigger: make(chan triggerCmd, 1000),
		// We would like to allow a large number of
		// cmds to be queued for the flusher thread
		// without the callers worrying about blocking
		// This should change to consolidate all KICKs into one
		cmd:       make(chan FlushRequest, 1000),
		treeState: treeState,
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
	defer c.FuncIn("DirtyQueue::kicker", "%s", dq.treeState.name).Out()
	defer logRequestPanic(c)

	// When we think we have no inodes try periodically anyways to ensure sanity
	nextExpiringInode := time.Now().Add(flusherSanityTimeout)

	for {
		sleepTime := getSleepTime(c, nextExpiringInode)
		cmd := FlushRequest{
			cmd:      KICK,
			response: nil,
		}
		done := false

		select {
		case cmd = <-dq.cmd:
			c.vlog("dq kicker cmd %d", cmd.cmd)
		case <-time.After(sleepTime):
			c.vlog("dq kicker woken up due to timer")
		}

		err := func() error {
			dq.treeState.lock.RLock()
			defer dq.treeState.lock.RUnlock()

			doneChan := make(chan error, 1)
			func() {
				defer c.qfs.flusher.lock.Lock().Unlock()

				// By the time we get the flusher lock, the flush
				// thread may be done and gone by now, so we have to
				// check before we even think of waiting on it
				if dq.Len_() == 0 {
					done = true
					return
				}

				switch cmd.cmd {
				case KICK:
					dq.trigger <- triggerCmd{
						flushAll: false,
						finished: doneChan,
						ctx:      c,
					}
				case FLUSHALL:
					dq.trigger <- triggerCmd{
						flushAll: true,
						finished: doneChan,
						ctx:      c,
					}
				case RETURN:
					c.elog("RETURN with non-empty dirty queue")
				default:
					c.elog("Unhandled flushing type")
				}
			}()

			if done {
				return nil
			}

			// With the flusher lock released, we've allowed the flush
			// thread to do its job and inform us when we can release
			// the treelock
			err := <-doneChan

			defer c.qfs.flusher.lock.Lock().Unlock()
			if dq.Len_() > 0 {
				element := dq.Front_()
				candidate := element.Value.(*dirtyInode)
				nextExpiringInode = candidate.expiryTime
			} else {
				done = true
			}

			return err
		}()

		// notify the caller we're done
		if cmd.response != nil {
			cmd.response <- err
		}

		if done {
			return
		}
	}
}

// Try sending a command to the dirtyqueue, failing immediately
// if it would have blocked
func (dq *DirtyQueue) TryCommand(c *ctx, cmd FlushCmd, response chan error) error {
	c.vlog("Sending cmd %d to dirtyqueue %s", cmd, dq.treeState.name)

	newCmd := FlushRequest{
		cmd:      cmd,
		response: response,
	}

	select {
	case dq.cmd <- newCmd:
		return nil
	default:
		c.vlog("sending cmd %d would have blocked", cmd)
		return fmt.Errorf("sending cmd %d would have blocked", cmd)
	}
}

// treeState lock and flusher lock must be locked R/W when calling this function
func (dq *DirtyQueue) flushCandidate_(c *ctx, dirtyInode *dirtyInode) bool {
	// We must release the flusher lock because when we flush
	// an Inode it will modify its parent and likely place that
	// parent onto the dirty queue. If we still hold that lock
	// we'll deadlock. We defer relocking in order to balance
	// against the deferred unlocking from our caller, even in
	// the case of a panic.
	inode := dirtyInode.inode
	var dirtyElement *list.Element

	flushSuccess, shouldForget := func() (bool, bool) {
		// Increment the lookup count to prevent the inode from
		// getting uninstantiated.
		c.qfs.incrementLookupCount(c, inode.inodeNum())
		forgetCalled := false
		forget := func() bool {
			if !forgetCalled {
				forgetCalled = true
				return c.qfs.shouldForget(c, inode.inodeNum(), 1)
			}
			return false
		}
		defer forget()

		// the inode should be marked clean before flushing so that any new
		// attemps to write to the inode dirties it again. Even though the
		// inode is clean, it cannot be uninstantiated as the lookupCount is
		// incremented above.
		dirtyElement = inode.markClean_()

		if dq.treeState.skipFlush {
			// Don't waste time flushing inodes in deleted workspaces
			c.vlog("Skipping flush as workspace is deleted")
			return true, forget()
		}

		flushSuccess := true
		if dirtyInode.shouldFlush {
			c.qfs.flusher.lock.Unlock()
			defer c.qfs.flusher.lock.Lock()
			flushSuccess = c.qfs.flushInode_(c, inode)
		}
		return flushSuccess, forget()
	}()
	if !flushSuccess {
		// flushing the inode has failed, if the inode has been dirtied in
		// the meantime, just drop this list entry as there is now another
		// one
		return inode.markUnclean_(dirtyElement)
	}

	if dirtyInode.shouldUninstantiate || shouldForget {
		c.qfs.flusher.lock.Unlock()
		defer c.qfs.flusher.lock.Lock()
		c.qfs.uninstantiateInode(c, inode.inodeNum())
	}
	return true
}

func init() {
	panicErr = fmt.Errorf("flushQueue panic")
}

var panicErr error

// treeState lock and flusher lock must be locked R/W when calling this function
func (dq *DirtyQueue) flushQueue_(c *ctx, flushAll bool) (done bool, err error) {
	defer c.FuncIn("DirtyQueue::flushQueue_", "flushAll %t skipFlush %t",
		flushAll, dq.treeState.skipFlush).Out()
	defer logRequestPanic(c)
	err = panicErr

	if flushAll {
		dq.sortTopologically_(c)
	}

	// If we are going to skip flushing the inodes, there is no need to wait. We
	// must still go through all the motions to ensure the inode is properly
	// forgotten and, if necessary, uninstantiated.
	if !flushAll {
		flushAll = dq.treeState.skipFlush
	}

	for dq.Len_() > 0 {
		// Should we clean this inode?
		element := dq.Front_()
		candidate := element.Value.(*dirtyInode)

		now := time.Now()
		if !flushAll && candidate.expiryTime.After(now) {
			// all expiring inodes have been flushed
			return false, nil
		}

		if !dq.flushCandidate_(c, candidate) {
			candidate.expiryTime = time.Now().Add(
				c.qfs.config.DirtyFlushDelay)
			if flushAll {
				return false, fmt.Errorf("Flushing inode %d failed",
					candidate.inode.inodeNum())
			}
			return false, nil
		}
		dq.Remove_(element)
	}
	return true, nil
}

func getSleepTime(c *ctx, nextExpiringInode time.Time) time.Duration {
	sleepTime := nextExpiringInode.Sub(time.Now())
	if sleepTime > flusherSanityTimeout {
		sleepTime = flusherSanityTimeout
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
	defer c.FuncIn("DirtyQueue::flusher", "%s", dq.treeState.name).Out()
	defer logRequestPanic(c)
	done := false

	for !done {
		trigger := <-dq.trigger
		c = trigger.ctx
		c.vlog("trigger, flushAll: %v", trigger.flushAll)

		// NOTE: When we are triggered, the treelock *must* already
		// be locked by the caller (the thread that pushed into dq.trigger),
		// exclusively or not

		func() {
			defer c.qfs.flusher.lock.Lock().Unlock()

			var err error
			done, err = dq.flushQueue_(c, trigger.flushAll)

			// Provide the triggerer a result
			defer func() {
				if trigger.finished != nil {
					trigger.finished <- err
				}
			}()

			if dq.Len_() == 0 {
				done = true
			} else if done {
				c.elog("Done without empty dirty queue")
			}

			utils.Assert(!(done && err != nil),
				"Somehow finished flush with error")

			if done {
				// Cleanup
				delete(c.qfs.flusher.dqs, dq.treeState)

				// end the kicker thread and cleanup triggers
				dq.TryCommand(c, RETURN, nil)
				close(dq.trigger)
				close(dq.cmd)

				// consume any leftover triggers
				for trigger := range dq.trigger {
					if trigger.finished != nil {
						trigger.finished <- err
					}
				}

				// consume any leftover kicker commands
				for cmd := range dq.cmd {
					if cmd.response != nil {
						cmd.response <- err
					}
				}
			}
		}()
	}
}

// flusher lock must be held
func (dq *DirtyQueue) requeue_(c *ctx, inode Inode) {
	defer c.FuncIn("DirtyQueue::requeue_", "inode %d", inode.inodeNum()).Out()

	for {
		dq.moveToBackOfQueue_(c, inode)

		done := func() bool {
			// Normally we would need to take the parent lock here to
			// prevent the inode from being reparented while we traverse
			// up the tree. However, we cannot do that because we already
			// hold the flusher lock and so cannot safely grab the parent
			// lock (for example see Directory.parentSetChildAttr()).
			// Luckily it isn't important that the queue is sorted
			// absolutely correctly in all cases and we can thus retrieve
			// the parent without holding the parent lock because if we
			// receive an out of date parent we'll still operate
			// correctly, just less efficiently.
			if inode.isWorkspaceRoot() || inode.isOrphaned_() {
				return true
			}
			inode = inode.parent_(c)
			return false
		}()
		if done {
			return
		}
	}
}

// Must hold flusher lock
func (dq *DirtyQueue) moveToBackOfQueue_(c *ctx, inode Inode) {
	de := inode.dirtyElement_()
	if de != nil {
		c.vlog("Moving inode %d to end of dirty queue", inode.inodeNum())
		dq.l.MoveToBack(de)
	} else {
		c.vlog("Adding inode %d to end of dirty queue", inode.inodeNum())
		inode.dirty_(c)
	}
}

// When flushing the dirty queue normally the WSR will be dirtied many times
// as it is occasionally published, but children of that WSR are still on the
// dirty queue. When syncing the entire queue we can flush any particular
// inode only once by flushing in reverse topological order, from the leaves
// up to the root.
//
// Must hold the flusher lock
func (dq *DirtyQueue) sortTopologically_(c *ctx) {
	defer c.funcIn("DirtyQueue::sortTopologically_").Out()

	// The general strategy is to dirty all the parents and grand-parents of each
	// inode on the dirty queue up to the WSR. At the end we'll have a close,
	// though not necessarily perfect, approximation of a reverse topological
	// sort. At least the WSR will only be uploaded once, which is the primary
	// goal.

	dirtyInodes := make([]*dirtyInode, 0, dq.Len_())
	for e := dq.Front_(); e != nil; e = e.Next() {
		di := e.Value.(*dirtyInode)
		inodeNum := di.inode.inodeNum()

		// We do not have the parent lock, see the comment in requeue_()
		if di.inode.isOrphaned_() {
			c.vlog("Skipping orphaned inode %d", inodeNum)
			continue
		}

		if di.inode.isListingType() {
			c.vlog("Skipping listing inode %d", inodeNum)
			continue
		}

		dirtyInodes = append(dirtyInodes, di)
		c.vlog("Added inode %d to sorting list", inodeNum)
	}

	for _, di := range dirtyInodes {
		dq.requeue_(c, di.inode)
	}
}

type Flusher struct {
	// This is a map from the treeState to a list of dirty inodes. We use the
	// treeState because every Inode already has the treeState of its workspace
	// so this is an easy way to sort Inodes by workspace.
	dqs  map[*TreeState]*DirtyQueue
	lock utils.DeferableMutex
}

func (flusher *Flusher) nQueued(c *ctx, treeState *TreeState) int {
	defer flusher.lock.Lock().Unlock()
	return flusher.nQueued_(c, treeState)
}

// flusher lock must be locked when calling this function
func (flusher *Flusher) nQueued_(c *ctx, treeState *TreeState) int {
	dq, exists := flusher.dqs[treeState]
	if !exists {
		return 0
	}
	return dq.Len_()
}

func NewFlusher() *Flusher {
	dqs := Flusher{
		dqs: make(map[*TreeState]*DirtyQueue),
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
			response := make(chan error, 1)
			if workspace != "" {
				if workspace != dq.treeState.name {
					continue
				}

				// For a single specific workspace, we assume to
				// already have the treelock acquired, so trigger
				// flusher thread manually
				dq.trigger <- triggerCmd{
					flushAll: true,
					finished: response,
					ctx:      c,
				}
			} else {
				err = dq.TryCommand(c, FLUSHALL, response)
				if err != nil {
					c.vlog("failed to send cmd to dirtyqueue")
					return
				}
			}

			doneChannels = append(doneChannels, response)
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
	shouldUninstantiate bool) *list.Element {

	defer c.FuncIn("Flusher::queue_", "inode %d uninstantiate %t",
		inode.inodeNum(), shouldUninstantiate).Out()

	var dirtyNode *dirtyInode
	dirtyElement := inode.dirtyElement_()
	launch := false
	if dirtyElement == nil {
		// This inode wasn't in the dirtyQueue so add it now
		dirtyNode = &dirtyInode{
			inode:               inode,
			shouldUninstantiate: shouldUninstantiate,
		}

		treeState := inode.treeState()
		dq, ok := flusher.dqs[treeState]
		if !ok {
			dq = NewDirtyQueue(treeState)
			flusher.dqs[treeState] = dq
			launch = true
		}

		if shouldUninstantiate {
			// There is not much point in delaying the uninstantiation,
			// do it as soon as possible.
			dirtyNode.expiryTime = time.Now()
			dirtyElement = dq.PushFront_(dirtyNode)
		} else {
			// Delay the flushing of dirty inode so as to potentially
			// absorb more writes and consolidate them into a single
			// write into durable storage.
			dirtyNode.expiryTime =
				time.Now().Add(c.qfs.config.DirtyFlushDelay)

			dirtyElement = dq.PushBack_(dirtyNode)
		}
	} else {
		dirtyNode = dirtyElement.Value.(*dirtyInode)
		c.vlog("Inode was already in the dirty queue %s",
			dirtyNode.expiryTime.String())
		dirtyNode.expiryTime = time.Now()
	}
	if shouldUninstantiate {
		dirtyNode.shouldUninstantiate = true
	} else {
		dirtyNode.shouldFlush = true
	}

	treeState := inode.treeState()
	dq := flusher.dqs[treeState]
	if launch {
		go dq.flusher(c)
		go dq.kicker(c.flusherCtx())
	}

	dq.TryCommand(c, KICK, nil)
	return dirtyElement
}
