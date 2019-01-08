// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

import (
	"container/list"
	"runtime/debug"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type reusableId struct {
	id     InodeId
	usable time.Time
}

func newInodeIds(delay time.Duration, gcPeriod time.Duration) *inodeIds {
	return &inodeIds{
		highMark:      quantumfs.InodeIdReservedEnd + 1,
		gcPeriod:      gcPeriod,
		reusableMap:   make(map[InodeId]struct{}),
		reusableDelay: delay,
	}
}

type inodeIds struct {
	lock utils.DeferableMutex

	highMark uint64
	// The last time of garbage collection or a change in highMark
	lastEvent time.Time
	gcPeriod  time.Duration

	reusableIds list.List
	reusableMap map[InodeId]struct{}

	// configurations
	reusableDelay time.Duration
}

func (ids *inodeIds) newInodeId(c *ctx) (newId InodeId, reused bool) {
	defer ids.lock.Lock().Unlock()

	ids.testHighmark_(c)

	for {
		nextIdElem := ids.reusableIds.Front()
		if nextIdElem == nil {
			// no ids left to reuse
			break
		}

		nextId := nextIdElem.Value.(reusableId)
		if nextId.usable.After(time.Now()) {
			// the next tuple is too new to use
			break
		}

		// now that we know this tuple isn't under delay, we will either
		// return it or garbage collect it
		ids.remove_(nextId.id, nextIdElem)

		if uint64(nextId.id) < ids.highMark {
			// this id is useable
			c.vlog("Reusing inode id %d", uint64(nextId.id))
			return nextId.id, true
		}

		// discard to garbage collect and try the next element
	}

	// we didn't find an id to reuse, so return a fresh one
	return ids.allocateFreshId_(), false
}

func (ids *inodeIds) releaseInodeId(c *ctx, id InodeId) {
	defer ids.lock.Lock().Unlock()

	ids.testHighmark_(c)

	if uint64(id) >= ids.highMark {
		// garbage collect this id
		return
	}

	ids.push_(c, id)
}

const inodeIdsGb = "Garbage collected highmark %d %d"

// The inodeids lock must be held
func (ids *inodeIds) testHighmark_(c *ctx) {
	if time.Since(ids.lastEvent) > ids.gcPeriod {
		ids.lastEvent = ids.lastEvent.Add(ids.gcPeriod)
		newHighMark := uint64(0.9 * float64(ids.highMark))
		c.vlog(inodeIdsGb, ids.highMark, newHighMark)
		ids.highMark = newHighMark
		if ids.highMark < quantumfs.InodeIdReservedEnd+1 {
			ids.highMark = quantumfs.InodeIdReservedEnd + 1
		}
	}
}

// ids.lock must be locked
func (ids *inodeIds) allocateFreshId_() InodeId {
	ids.lastEvent = time.Now()

	for {
		nextId := InodeId(ids.highMark)
		ids.highMark++

		_, exists := ids.reusableMap[nextId]
		if !exists {
			// this id isn't on a delay, so it's safe to use
			return nextId
		}
	}
}

// ids.lock must be locked
func (ids *inodeIds) push_(c *ctx, id InodeId) {
	_, exists := ids.reusableMap[id]
	if exists {
		// This should never happen, but recover if it does
		c.elog("Double push of inode id %d\n%s", int64(id),
			utils.BytesToString(debug.Stack()))
		return
	}

	ids.reusableIds.PushBack(reusableId{
		id:     id,
		usable: time.Now().Add(ids.reusableDelay),
	})
	ids.reusableMap[id] = struct{}{}
}

// ids.lock must be locked. idElem should correspond to the id given
func (ids *inodeIds) remove_(id InodeId, idElem *list.Element) {
	ids.reusableIds.Remove(idElem)
	delete(ids.reusableMap, id)
}
