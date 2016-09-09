// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to symlink.go

import "github.com/aristanetworks/quantumfs"

func (link *Symlink) forget_DOWN(c *ctx) {
	c.elog("Invalid forget_DOWN call on Symlink")
}

func (link *Symlink) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	link.setDirty(false)
	return link.key
}
