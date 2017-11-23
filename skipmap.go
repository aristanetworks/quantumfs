// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"container/list"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/aristanetworks/quantumfs/walker"
)

type SkipMap struct {
	lru		list.List
	keys		map[string]*list.Element

	maxLen		int

	mutex		utils.DeferableMutex
}

func NewSkipMap(maxLength int) *SkipMap {
	return &SkipMap {
		lru:		list.List{},
		keys:		make(map[string]*list.Element),
		maxLen:		maxLength,
	}
}

func (bc *SkipMap) Check(c *walker.Ctx,
	key quantumfs.ObjectKey) (inCache bool){

	defer bc.mutex.Lock().Unlock()

	element, exists := bc.keys[string(key.Value())]

	if exists {
		// Update the position in the lru
		bc.lru.MoveToBack(element)
	}

	return exists
}

var empty struct{}
func (bc *SkipMap) Set(c *walker.Ctx, key quantumfs.ObjectKey) {
	defer bc.mutex.Lock().Unlock()

	kv := string(key.Value())

	// We must always check, when we have the cache lock, whether a key exists
	// before doing operations that assume it exists, since there is no guarantee
	// that the key exists by the time we've acquired the lock
	if element, exists := bc.keys[kv]; exists {
		c.Qctx.Vlog(qlog.LogTool, "Update SkipMap for %s", key.String())

		// Update the position in the lru
		bc.lru.MoveToBack(element)
		return
	}

	c.Qctx.Vlog(qlog.LogTool, "Set SkipMap for %s", key.String())
	bc.keys[kv] = bc.lru.PushBack(kv)

	// Ensure the cache length is maintained
	if bc.lru.Len() > bc.maxLen {
		front := bc.lru.Front()
		bc.lru.Remove(front)
		delete(bc.keys, front.Value.(string))

		utils.Assert(bc.lru.Len() <= bc.maxLen, "Lru length violated")
	}
}

func (bc *SkipMap) Clear() {
	defer bc.mutex.Lock().Unlock()

	bc.keys = make(map[string]*list.Element)
	bc.lru = list.List{}
}

func (bc *SkipMap) Len() (lruLen int, mapLen int) {
	defer bc.mutex.Lock().Unlock()
	return bc.lru.Len(), len(bc.keys)
}
