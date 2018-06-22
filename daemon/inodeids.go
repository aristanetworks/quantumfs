// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"container/list"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type reusableId struct {
	id     InodeId
	usable time.Time
}

func newInodeIds(delay time.Duration) *inodeIds {
	return &inodeIds{
		highMark:      quantumfs.InodeIdReservedEnd,
		reusableMap:   make(map[InodeId]struct{}),
		reusableDelay: delay,
	}
}

type inodeIds struct {
	highMark uint64

	reusableIds list.List
	reusableMap map[InodeId]struct{}
	lock        utils.DeferableMutex

	// configurations
	reusableDelay time.Duration
}

func (ids *inodeIds) newInodeId() InodeId {
	defer ids.lock.Lock().Unlock()

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
			return nextId.id
		}

		// discard to garbage collect and try the next element
	}

	// we didn't find an id to reuse, so return a fresh one
	return ids.allocateFreshId_()
}

func (ids *inodeIds) releaseInodeId(id InodeId) {
	defer ids.lock.Lock().Unlock()

	if uint64(id) >= ids.highMark {
		// garbage collect this id
		return
	}

	ids.push_(id)
}

// ids.lock must be locked
func (ids *inodeIds) allocateFreshId_() InodeId {
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
func (ids *inodeIds) push_(id InodeId) {
	ids.reusableIds.PushBack(reusableId{
		id:     id,
		usable: time.Now().Add(ids.reusableDelay),
	})
	ids.reusableMap[id] = struct{}{}
}

// ids.lock must be locked. id an idElem must match
func (ids *inodeIds) remove_(id InodeId, idElem *list.Element) {
	ids.reusableIds.Remove(idElem)
	delete(ids.reusableMap, id)
}
