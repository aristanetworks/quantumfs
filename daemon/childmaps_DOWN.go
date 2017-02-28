// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to childmaps.go

import "github.com/aristanetworks/quantumfs"


// We need to check records so that if a hardlink has nlink of 1, that we turn it
// back into a normal file again
func (cmap *ChildMap) checkLinkAndAlone_DOWN(c *ctx, inodeId InodeId) {

	record := cmap.firstRecord(inodeId)
	if record == nil || record.Type() != quantumfs.ObjectTypeHardlink {
		// no hardlink record here, nothing to do
		return
	}

	link := record.(*Hardlink)

	// This may need to be turned back into a normal file
	newRecord, inodeId := cmap.wsr.removeHardlink(c, link.linkId)

	if newRecord == nil && inodeId == quantumfs.InodeIdInvalid {
		// wsr says hardlink isn't ready for removal yet
		return
	}

	// Because we have to call on the child, this must be in a DOWN function
	inode := c.qfs.inodeNoInstantiate(c, inodeId)
	if inode == nil {
		c.qfs.addUninstantiated(c, []InodeId{inodeId}, cmap.dir.inodeNum())
	} else {
		inode.setParent(cmap.dir.inodeNum())
	}

	// Ensure that we update this version of the record with this instance
	// of the hardlink's information
	newRecord.SetFilename(link.Filename())

	// Here we do the opposite of makeHardlink DOWN - we re-insert it
	cmap.setRecord(inodeId, newRecord)
	cmap.dir.dirty(c)
}

