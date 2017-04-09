// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This file contains functions to help with merging.

func (le *linkEntry) Merge(remote interface{}, local interface{}) interface{} {
	return remote
}

type hardlinkMap struct {
	hardlinks *map[HardlinkId]linkEntry
}

func wrapHardlinkMap(linkMap *map[HardlinkId]linkEntry) hardlinkMap {
	return hardlinkMap {
		hardlinks:	linkMap,
	}
}

func (hl hardlinkMap) Keys() []interface{} {
	rtn := make([]interface{}, 0, len(*hl.hardlinks))
	for k, _ := range *hl.hardlinks {
		rtn = append(rtn, k)
	}

	return rtn
}

func (hl hardlinkMap) Get(key interface{}) (value interface{}, exists bool) {
	value, exists = (*hl.hardlinks)[key.(HardlinkId)]
	return value, exists
}

func (hl hardlinkMap) Set(key interface{}, value interface{}) {
	(*hl.hardlinks)[key.(HardlinkId)] = value.(linkEntry)
}

type mergeValue interface {
	Merge(remote interface{}, local interface{}) interface{}
}

type mergeable interface {
	Keys() []interface{}
	Get(key interface{}) (value interface{}, exists bool)
	Set(key interface{}, value interface{})
}

// Mergeable data structures should have universeally unique keys.
// Otherwise, you'll just get weirdness where items made at the same
// time on different quantumfs instances all become linked.
func mergeGeneric(base mergeable, remote mergeable, local mergeable,
	output mergeable) {

	for _, k := range base.Keys() {
		// if something exists in both remote and local, then neither ws
		// deleted it so just merge it
		remoteLink, remoteExists := remote.Get(k)
		localLink, localExists := local.Get(k)
		if remoteExists && localExists {
			baseItem, _ := base.Get(k)
			output.Set(k, baseItem.(mergeValue).Merge(remoteLink,
				localLink))
		}
	}

	// Now handle new entries in both maps
	for _, k := range remote.Keys() {
		_, baseExists := base.Get(k)
		if baseExists {
			continue
		}

		remoteItem, _ := remote.Get(k)
		output.Set(k, remoteItem)
	}
	for _, k := range local.Keys() {
		_, baseExists := base.Get(k)
		if baseExists {
			continue
		}

		localItem, _ := local.Get(k)
		output.Set(k, localItem)
	}
}
