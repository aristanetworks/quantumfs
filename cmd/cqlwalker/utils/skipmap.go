// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package utils

import (
	"container/list"

	"github.com/aristanetworks/quantumfs/utils"
)

type SkipMap struct {
	lru  list.List
	keys map[string]*list.Element

	maxLen int

	mutex utils.DeferableMutex
}

func NewSkipMap(maxLength int) *SkipMap {
	return &SkipMap{
		lru:    list.List{},
		keys:   make(map[string]*list.Element),
		maxLen: maxLength,
	}
}

func (bc *SkipMap) Check(key string) (inCache bool) {
	defer bc.mutex.Lock().Unlock()

	element, exists := bc.keys[key]
	if exists {
		// Update the position in the lru
		bc.lru.MoveToBack(element)
	}

	return exists
}

var empty struct{}

func (bc *SkipMap) Set(key string) {
	defer bc.mutex.Lock().Unlock()

	// We must always check, when we have the cache lock, whether a key exists
	// before doing operations that assume it exists, since there is no guarantee
	// that the key exists by the time we've acquired the lock
	if element, exists := bc.keys[key]; exists {
		// Update the position in the lru
		bc.lru.MoveToBack(element)
		return
	}

	bc.keys[key] = bc.lru.PushBack(key)

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

func (bc *SkipMap) Merge(local *SkipMap) {
	defer local.mutex.Lock().Unlock()
	for key := range local.keys {
		bc.Set(key)
	}
}
