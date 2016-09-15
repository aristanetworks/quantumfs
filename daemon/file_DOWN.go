// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to file.go

import "github.com/aristanetworks/quantumfs"

func (fi *File) forget_DOWN(c *ctx) {
	c.vlog("File::forget_DOWN Enter")
	defer c.vlog("File::forget_DOWN Exit")

	key := fi.accessor.sync(c)
	fi.setDirty(false)
	fi.parent().syncChild(c, fi.InodeCommon.id, key)

	// Remove the inode from the map, ready to be garbage collected
	c.qfs.setInode(c, fi.id, nil)
}

func (fi *File) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	c.vlog("File::sync_DOWN Enter")
	defer c.vlog("File::sync_DOWN Exit")

	key := fi.accessor.sync(c)
	fi.setDirty(false)
	return key
}
