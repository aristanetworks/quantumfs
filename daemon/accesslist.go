// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type accessList struct {
	lock      utils.DeferableMutex
	hardlinks map[quantumfs.FileId]quantumfs.PathFlags
	paths     map[string]quantumfs.PathFlags
}

func NewAccessList() *accessList {
	return &accessList{
		hardlinks: make(map[quantumfs.FileId]quantumfs.PathFlags),
		paths:     make(map[string]quantumfs.PathFlags),
	}
}

func (al *accessList) generate(c *ctx,
	accessMap map[quantumfs.FileId][]string) quantumfs.PathsAccessed {

	defer c.funcIn("accessList::generate").Out()
	defer al.lock.Lock().Unlock()

	// make a copy of the regular files map first
	rtn := make(map[string]quantumfs.PathFlags)
	for k, v := range al.paths {
		rtn["/"+k] = v
	}

	for fileId, pathFlags := range al.hardlinks {
		paths := accessMap[fileId]
		for _, path := range paths {
			path = "/" + path

			if current, exists := rtn[path]; exists {
				rtn[path] = current | pathFlags
			} else {
				rtn[path] = pathFlags
			}
		}
	}

	return quantumfs.PathsAccessed{
		Paths: rtn,
	}
}

func (al *accessList) markHardlinkAccessed(c *ctx, fileId quantumfs.FileId,
	op quantumfs.PathFlags) {

	defer c.funcIn("accessList::markHardlinkAccessed").Out()

	utils.Assert(!utils.BitFlagsSet(uint(op),
		quantumfs.PathCreated|quantumfs.PathDeleted),
		"Cannot create and delete simultaneously")

	// This code pathway shouldn't ever see Created or Deleted flags
	if op&(quantumfs.PathCreated|quantumfs.PathDeleted) != 0 {
		c.elog("Creation / Deletion accesslist update in hardlink pathway")
	}

	defer al.lock.Lock().Unlock()

	pathFlags, exists := al.hardlinks[fileId]
	if !exists {
		c.vlog("Creating new hardlink entry")
		al.hardlinks[fileId] = op
		return
	}

	newFlags, deleteEntry := updatePathFlags(c, pathFlags, op)
	// TODO: Perhaps support hardlink deletion at some point (BUG229575)
	if !deleteEntry {
		al.hardlinks[fileId] = newFlags
	}
}

func (al *accessList) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {

	defer c.funcIn("accessList::markAccessed").Out()

	utils.Assert(!utils.BitFlagsSet(uint(op),
		quantumfs.PathCreated|quantumfs.PathDeleted),
		"Cannot create and delete simultaneously")

	defer al.lock.Lock().Unlock()

	pathFlags, exists := al.paths[path]
	if !exists {
		c.vlog("Creating new entry")
		al.paths[path] = op
		return
	}

	newFlags, deleteEntry := updatePathFlags(c, pathFlags, op)
	c.vlog("updatePathFlags result %d %d %v", pathFlags, newFlags, deleteEntry)
	if deleteEntry {
		delete(al.paths, path)
	} else {
		al.paths[path] = newFlags
	}
}

func (al *accessList) clear() {
	defer al.lock.Lock().Unlock()

	al.hardlinks = make(map[quantumfs.FileId]quantumfs.PathFlags)
	al.paths = make(map[string]quantumfs.PathFlags)
}

func updatePathFlags(c *ctx, pathFlags quantumfs.PathFlags,
	op quantumfs.PathFlags) (newFlags quantumfs.PathFlags, deleteEntry bool) {

	defer c.FuncIn("accesslist::updatePathFlags", "%x", pathFlags).Out()

	pathFlags |= op & (quantumfs.PathRead | quantumfs.PathUpdated)

	if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathCreated) &&
		utils.BitFlagsSet(uint(op), quantumfs.PathDeleted) {

		// Entries which were created and are then subsequently
		// deleted are removed from the accessed list under the
		// assumption they are temporary files and of no interest.
		c.vlog("Nullifying entry")
		return pathFlags, true
	} else if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathDeleted) &&
		utils.BitFlagsSet(uint(op), quantumfs.PathCreated) {

		if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathIsDir) {
			// Directories which are deleted and then recreated are not
			// recorded as either created or deleted. However, if it was
			// read, that is maintained.
			if utils.BitFlagsSet(uint(pathFlags), quantumfs.PathRead) {
				c.vlog("Keeping delete->create directory as read")
				pathFlags = pathFlags &^ quantumfs.PathDeleted
			} else {
				c.vlog("Unmarking directory with delete->create")
				return pathFlags, true
			}
		} else {
			// Files which are deleted then recreated are recorded as
			// being neither deleted nor created, but instead truncated
			// (updated). This simplifies the case where some program
			// unlinked and then created/moved a file into place.
			c.vlog("Removing deleted from recreated file")
			pathFlags = quantumfs.PathUpdated
		}
	} else {
		// Here we only have a delete or create and simply record them.
		pathFlags |= op & (quantumfs.PathCreated | quantumfs.PathDeleted)
	}

	return pathFlags, false
}
