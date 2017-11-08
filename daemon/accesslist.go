// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type accessList struct {
	lock		utils.DeferableMutex
	hardlinks	map[quantumfs.FileId]quantumfs.PathFlags
	paths		map[string]quantumfs.PathFlags
}

func NewAccessList() *accessList {
	return &accessList {
		hardlinks:	make(map[quantumfs.FileId]quantumfs.PathFlags),
		paths:		make(map[string]quantumfs.PathFlags),
	}
}

// Note: We need a useless boolean return here to split this function into two lines
func (al *accessList) generate(hardlinks map[quantumfs.FileId]linkEntry) (a bool,
	list quantumfs.PathsAccessed) {

	defer al.lock.Lock().Unlock()

	// make a copy of the regular files map first
	rtn := make(map[string]quantumfs.PathFlags)
	for k, v := range al.paths {
		rtn["/" + k] = v
	}

	for k, v := range al.hardlinks {
		hardlink := hardlinks[k]
		for _, path := range hardlink.paths {
			if current, exists := rtn[path]; exists {
				rtn["/" + path] = current | v
			} else {
				rtn["/" + path] = v
			}
		}
	}

	return false, quantumfs.PathsAccessed {
		Paths:	rtn,
	}
}

func (al *accessList) markHardlinkAccessed(c *ctx, fileId quantumfs.FileId,
	op quantumfs.PathFlags) {

	utils.Assert(!utils.BitFlagsSet(uint(op),
		quantumfs.PathCreated|quantumfs.PathDeleted),
		"Cannot create and delete simultaneously")

	// This code pathway shouldn't ever see Created or Deleted flags
	if op & (quantumfs.PathCreated | quantumfs.PathDeleted) != 0 {
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
	// TODO: Perhaps support hardlink deletion at some point
	if !deleteEntry {
		al.hardlinks[fileId] = newFlags
	}
}

func (al *accessList) markAccessed(c *ctx, path string, op quantumfs.PathFlags) {

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

	c.vlog("Updating existing entry: %x", pathFlags)

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
	//wsr.accessList.Paths[path] = pathFlags
}

