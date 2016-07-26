// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import "encoding/json"

import "github.com/aristanetworks/quantumfs"

func (dir *Directory) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("Directory::sync Enter")
	defer c.vlog("Directory::sync Exit")
	if !dir.isDirty() {
		c.vlog("directory not dirty")
		return dir.baseLayerId
	}

	defer dir.Lock().Unlock()

	dir.updateRecords_DOWN_(c)
	return dir.publish(c)
}

func publishDirectoryEntry(c *ctx, layer *quantumfs.DirectoryEntry,
	nextKey quantumfs.ObjectKey) quantumfs.ObjectKey {

	layer.NumEntries = uint32(len(layer.Entries))
	layer.Next = nextKey
	bytes, err := json.Marshal(layer)
	if err != nil {
		panic("Failed to marshal baselayer")
	}

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newKey, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new baseLayer object")
	}

	return newKey
}

func (dir *Directory) publish(c *ctx) quantumfs.ObjectKey {
	c.vlog("Directory::publish Enter")
	defer c.vlog("Directory::publish Exit")

	// Compile the internal records into a series of blocks which can be placed
	// in the datastore.

	newBaseLayerId := quantumfs.EmptyDirKey

	// childIdx indexes into dir.childrenRecords, entryIdx indexes into the
	// metadata block
	baseLayer := quantumfs.NewDirectoryEntry()
	entryIdx := 0
	for _, child := range dir.childrenRecords {
		if entryIdx > quantumfs.MaxDirectoryRecords {
			// This block is full, upload and create a new one
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			baseLayer = quantumfs.NewDirectoryEntry()
			entryIdx = 0
		}

		baseLayer.Entries = append(baseLayer.Entries, *child)

		entryIdx++
	}

	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId)

	c.vlog("Directory key %x -> %x", dir.baseLayerId.Key[:], newBaseLayerId.Key[:])
	dir.baseLayerId = newBaseLayerId

	dir.setDirty(false)

	return dir.baseLayerId
}

// Walk the list of children which are dirty and have them recompute their new key
// wsr can update its new key.
func (dir *Directory) updateRecords_DOWN_(c *ctx) {
	for _, child := range dir.dirtyChildren_ {
		newKey := child.sync_DOWN(c)
		dir.childrenRecords[child.inodeNum()].ID = newKey
	}
	dir.dirtyChildren_ = make(map[InodeId]Inode, 0)
}
