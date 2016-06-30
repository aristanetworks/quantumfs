// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to file.go

import "github.com/aristanetworks/quantumfs"

func (fi *File) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	key := fi.accessor.sync(c)
	fi.setDirty(false)
	return key
}
