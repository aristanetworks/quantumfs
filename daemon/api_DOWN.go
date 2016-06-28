// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// _DOWN counterpart to api.go

import "github.com/aristanetworks/quantumfs"

func (api *ApiInode) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}
