// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to directory.go

import "github.com/aristanetworks/quantumfs"

func (dir *Directory) forget_DOWN(c *ctx) {
	c.vlog("Directory::forget_DOWN Enter")
	defer c.vlog("Directory::forget_DOWN Exit")

	defer dir.Lock().Unlock()

	// We need to ensure that we sync before we forget, or we won't save changes
	if dir.isDirty() {
		dir.updateRecords_DOWN_(c)
		newKey := dir.publish(c)

		dir.parent().syncChild(c, dir.InodeCommon.id, newKey)
	}

	// Tell all the children to forget themselves
	dir.dirChildren.forget_DOWN(c)	

	// Then remove the inode from the map, ready to be garbage collected
	c.qfs.setInode(c, dir.id, nil)
}

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

	layer.SetNext(nextKey)
	bytes := layer.Bytes()

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

	// childIdx indexes into childrenRecords, entryIdx indexes into the
	// metadata block
	baseLayer := quantumfs.NewDirectoryEntry()
	entryIdx := 0
	for _, child := range dir.dirChildren.getRecords() {
		if entryIdx == quantumfs.MaxDirectoryRecords {
			// This block is full, upload and create a new one
			baseLayer.SetNumEntries(entryIdx)
			newBaseLayerId = publishDirectoryEntry(c, baseLayer,
				newBaseLayerId)
			baseLayer = quantumfs.NewDirectoryEntry()
			entryIdx = 0
		}

		baseLayer.SetEntry(entryIdx, child)

		entryIdx++
	}

	baseLayer.SetNumEntries(entryIdx)
	newBaseLayerId = publishDirectoryEntry(c, baseLayer, newBaseLayerId)

	c.vlog("Directory key %s -> %s", dir.baseLayerId.String(),
		newBaseLayerId.String())
	dir.baseLayerId = newBaseLayerId

	dir.setDirty(false)

	return dir.baseLayerId
}

// Walk the list of children which are dirty and have them recompute their new key
// wsr can update its new key.
func (dir *Directory) updateRecords_DOWN_(c *ctx) {
	dirtyChildren := dir.dirChildren.popDirtyInodes()

	for _, childId := range dirtyChildren {
		child := c.qfs.inode(c, childId)

		newKey := child.sync_DOWN(c)
		record, exists := dir.dirChildren.getRecord(c, childId)
		if !exists {
			panic("Unexpected missing child during update")
		}
		record.SetID(newKey)
	}
}
