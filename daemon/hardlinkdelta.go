// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
)

// This class includes the Hardlink deltas kept for each instantiated
// directory in the tree.
//
// Whenever a directory is flushed, its parent's HardlinkDelta will
// get populated with the content of the child.
//
// When the delta gets propagated to the workspace root, it gets incorporated
// to the hardlink table to update nlink of the corresponding hardlinks to reflect
// the changes from the subtrees.

type HardlinkDelta struct {
	m map[quantumfs.FileId]int
}

func newHardlinkDelta() *HardlinkDelta {
	nd := HardlinkDelta{
		m: make(map[quantumfs.FileId]int),
	}
	return &nd
}

func (nd *HardlinkDelta) inc(fileId quantumfs.FileId) {
	nd.m[fileId] = nd.m[fileId] + 1
}

func (nd *HardlinkDelta) dec(fileId quantumfs.FileId) {
	nd.m[fileId] = nd.m[fileId] - 1
}

func (nd *HardlinkDelta) reset() {
	nd.m = make(map[quantumfs.FileId]int, len(nd.m))
}

func (nd *HardlinkDelta) populateFrom(ond *HardlinkDelta) {
	if ond != nil {
		for fileId, delta := range ond.m {
			nd.m[fileId] = nd.m[fileId] + delta
		}
		ond.reset()
	}
}

func (nd *HardlinkDelta) foreach(visitor func(id quantumfs.FileId, delta int)) {
	for fileId, delta := range nd.m {
		visitor(fileId, delta)
	}
}
