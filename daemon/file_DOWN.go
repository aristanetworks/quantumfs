// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to file.go

import "arista.com/quantumfs"

func (fi *File) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	fi.setDirty(false)
	return fi.key
}
