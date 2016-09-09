// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

func (cr *childRecords) forget_DOWN(c *ctx) {
	if cr.data == nil {
		// Nothing has been loaded, so nothing to forget
		return
	}

	for _, inodeId := range cr.data.fileToInode {
		inode := c.qfs.inode(c, inodeId)
		if inode != nil {
			inode.forget_DOWN(c)
		}
	}

	// Now throw all the child metadata away
	cr.data = nil
}
