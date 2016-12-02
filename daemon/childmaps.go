// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "errors"

import "github.com/aristanetworks/quantumfs"

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one
type ChildMap struct {

	children	map[string]InodeId
	dirtyChildren	map[InodeId]Inode // set of children which are currently dirty

	// These fields are protected by childRecordLock. This includes all the
	// entries within childrenRecords, which must be accessed only under this
	// lock.
	childRecordLock DeferableMutex
	childrenRecords map[InodeId]DirectoryRecordIf
}

func newChildMap(numEntries int) *ChildMap {
	return &ChildMap {
		children:	make(map[string]InodeId, numEntries),
		dirtyChildren:	make(map[InodeId]Inode, 0),
		childrenRecords: make(map[InodeId]DirectoryRecordIf, numEntries),
	}
}
