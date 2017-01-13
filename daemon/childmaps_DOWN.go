// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is _DOWN counterpart to childmaps.go

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

func (cmap *ChildMap) makeHardlink_DOWN(c *ctx, wsr *WorkspaceRoot,
	childName string) (copy DirectoryRecordIf, err fuse.Status) {

	childId, exists := cmap.children[childName]
	if !exists {
		c.elog("No child record for %s in childmap", childName)
		return nil, fuse.ENOENT
	}

	child := cmap.getRecord(childId, childName)
	if child == nil {
		panic("Mismatched childId and record")
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*Hardlink); isLink {
		recordCopy := *link
		return &recordCopy, fuse.OK
	}

	// record must be a file type to be hardlinked
	if child.Type() != quantumfs.ObjectTypeSmallFile &&
		child.Type() != quantumfs.ObjectTypeMediumFile &&
		child.Type() != quantumfs.ObjectTypeLargeFile &&
		child.Type() != quantumfs.ObjectTypeVeryLargeFile {

		c.dlog("Cannot hardlink %s - not a file", childName)
		return nil, fuse.EINVAL
	}

	// If the file hasn't been synced already, then we can't link it yet
	if _, exists = cmap.dirtyChildren[childId]; exists {
		c.elog("makeHardlink called on dirty child %d", childId)
		return nil, fuse.EIO
	}

	// It needs to become a hardlink now. Hand it off to wsr
	newLink := wsr.newHardlink(c, childId, child)

	cmap.setRecord(childId, newLink)
	linkCopy := *newLink
	return &linkCopy, fuse.OK
}
