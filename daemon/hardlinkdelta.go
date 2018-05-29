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

type deltaTuple struct {
	additions int
	deletions int
}

type HardlinkDelta struct {
	m map[quantumfs.FileId]deltaTuple
}

func newHardlinkDelta() *HardlinkDelta {
	nd := HardlinkDelta{}
	nd.reset()
	return &nd
}

func (nd *HardlinkDelta) reset() {
	nd.m = make(map[quantumfs.FileId]deltaTuple)
}

func (nd *HardlinkDelta) inc(fileId quantumfs.FileId) {
	d := nd.m[fileId]
	d.additions++
	nd.m[fileId] = d
}

func (nd *HardlinkDelta) dec(fileId quantumfs.FileId) {
	d := nd.m[fileId]
	d.deletions++
	nd.m[fileId] = d
}

func (nd *HardlinkDelta) populateFrom(ond *HardlinkDelta) {
	if ond != nil {
		for fileId, delta := range ond.m {
			d := nd.m[fileId]
			d.additions += delta.additions
			d.deletions += delta.deletions
			nd.m[fileId] = d
		}
		ond.reset()
	}
}

func (nd *HardlinkDelta) foreach(visitor func(id quantumfs.FileId,
	delta deltaTuple)) {

	for fileId, delta := range nd.m {
		visitor(fileId, delta)
	}
}
