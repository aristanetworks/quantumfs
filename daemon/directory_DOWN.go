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

	// Compile the internal records into a block which can be placed in the
	// datastore.
	baseLayer := quantumfs.NewDirectoryEntry(len(dir.childrenRecords))
	baseLayer.NumEntries = uint32(len(dir.childrenRecords))

	for _, entry := range dir.childrenRecords {
		baseLayer.Entries = append(baseLayer.Entries, *entry)
	}

	// Upload the base layer object
	bytes, err := json.Marshal(baseLayer)
	if err != nil {
		panic("Failed to marshal baselayer")
	}

	buf := newBuffer(c, bytes, quantumfs.KeyTypeMetadata)
	newBaseLayerId, err := buf.Key(&c.Ctx)
	if err != nil {
		panic("Failed to upload new baseLayer object")
	}

	c.vlog("Directory key %v -> %v", dir.baseLayerId, newBaseLayerId)
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
	dir.dirtyChildren_ = make([]Inode, 0)
}
