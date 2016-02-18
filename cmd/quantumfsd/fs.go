// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// This contains all the top level filesystem code. This includes handling all the
// paths up to the workspace root. ie. "/", "/abuild", "/abuild/" and the hand off
// into the workspace root itself.
package main

import "github.com/hanwen/go-fuse/fuse/nodefs"

func NewQuantumfs() nodefs.Node {
	return &WorkspaceListing{
		Node: nodefs.NewDefaultNode(),
		path: "/",
	}
}

type WorkspaceListing struct {
	nodefs.Node
	path string
}
