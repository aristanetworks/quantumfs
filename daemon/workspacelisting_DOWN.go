// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// This is the _DOWN counterpart to workspacelisting.go

import "github.com/aristanetworks/quantumfs"

func (wsl *WorkspaceList) forget_DOWN(c *ctx) {
	c.elog("Invalid forget_DOWN on WorkspaceList")
}

func (wsl *WorkspaceList) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}

func (nsl *NamespaceList) forget_DOWN(c *ctx) {
	c.elog("Invalid forget_DOWN on NamespaceList")
}

func (nsl *NamespaceList) sync_DOWN(c *ctx) quantumfs.ObjectKey {
	return quantumfs.EmptyBlockKey
}
