// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"
import "github.com/aristanetworks/quantumfs"

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
type ChildMap struct {
	children      map[string]InodeId
	dirtyChildren map[InodeId]InodeId // a set

	childrenRecords map[InodeId]DirectoryRecordIf
}

func newChildMap(numEntries int) *ChildMap {
	return &ChildMap{
		children:        make(map[string]InodeId, numEntries),
		dirtyChildren:   make(map[InodeId]InodeId, 0),
		childrenRecords: make(map[InodeId]DirectoryRecordIf, numEntries),
	}
}

func (cmap *ChildMap) newChild(c *ctx, entry DirectoryRecordIf,
	wsr *WorkspaceRoot) InodeId {	

	inodeId := InodeId(quantumfs.InodeIdInvalid)
	if entry.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(entry.ID())
		entry = newHardlink(linkId, wsr)
		inodeId = wsr.getHardlinkInodeId(linkId)
	} else {
		inodeId = c.qfs.newInodeId()
	}

	cmap.setChild_(c, entry, inodeId)

	return inodeId
}

func (cmap *ChildMap) setChild(c *ctx, entry DirectoryRecordIf, inodeId InodeId) {
	if entry.Type() == quantumfs.ObjectTypeHardlink {
		panic(fmt.Sprintf("Hardlink inodeId manually set, not naturally "+
			"loaded %d", inodeId))
	}

	cmap.setChild_(c, entry, inodeId)
}

func (cmap *ChildMap) setChild_(c *ctx, entry DirectoryRecordIf, inodeId InodeId) {
	if entry == nil {
		panic(fmt.Sprintf("Nil DirectoryEntryIf set attempt: %d", inodeId))
	}

	cmap.children[entry.Filename()] = inodeId
	// child is not dirty by default

	cmap.childrenRecords[inodeId] = entry
}

func (cmap *ChildMap) count() uint64 {
	return uint64(len(cmap.childrenRecords))
}

func (cmap *ChildMap) deleteChild(inodeNum InodeId) DirectoryRecordIf {
	record, exists := cmap.childrenRecords[inodeNum]
	if !exists {
		return nil
	} else {
		delete(cmap.children, record.Filename())
	}

	delete(cmap.childrenRecords, inodeNum)
	delete(cmap.dirtyChildren, inodeNum)

	return record
}

func (cmap *ChildMap) renameChild(oldName string,
	newName string) (oldInodeRemoved InodeId) {

	if oldName == newName {
		return quantumfs.InodeIdInvalid
	}

	// record whether we need to cleanup a file we're overwriting
	cleanupInodeId, needCleanup := cmap.children[newName]

	inodeId := cmap.children[oldName]
	cmap.children[newName] = inodeId
	cmap.childrenRecords[inodeId].SetFilename(newName)

	delete(cmap.children, oldName)

	if needCleanup {
		delete(cmap.childrenRecords, cleanupInodeId)
		delete(cmap.dirtyChildren, cleanupInodeId)
		return cleanupInodeId
	}

	return quantumfs.InodeIdInvalid
}

func (cmap *ChildMap) popDirty() map[InodeId]InodeId {
	rtn := cmap.dirtyChildren
	cmap.dirtyChildren = make(map[InodeId]InodeId, 0)

	return rtn
}

func (cmap *ChildMap) setDirty(c *ctx, inodeNum InodeId) {
	if _, exists := cmap.childrenRecords[inodeNum]; !exists {
		c.elog("Attempt to dirty child that doesn't exist: %d", inodeNum)
		return
	}

	cmap.dirtyChildren[inodeNum] = inodeNum
}

func (cmap *ChildMap) inodeNum(name string) InodeId {
	if inodeId, exists := cmap.children[name]; exists {
		return inodeId
	}

	return quantumfs.InodeIdInvalid
}

func (cmap *ChildMap) inodes() []InodeId {
	rtn := make([]InodeId, 0, len(cmap.children))
	for _, v := range cmap.children {
		rtn = append(rtn, v)
	}

	return rtn
}

func (cmap *ChildMap) records() []DirectoryRecordIf {
	rtn := make([]DirectoryRecordIf, 0, len(cmap.childrenRecords))
	for _, i := range cmap.childrenRecords {
		rtn = append(rtn, i)
	}

	return rtn
}

func (cmap *ChildMap) record(inodeNum InodeId) DirectoryRecordIf {
	entry, exists := cmap.childrenRecords[inodeNum]
	if !exists {
		return nil
	}

	return entry
}

func (cmap *ChildMap) recordByName(c *ctx, name string) DirectoryRecordIf {
	inodeNum, exists := cmap.children[name]
	if !exists {
		return nil
	}

	entry, exists := cmap.childrenRecords[inodeNum]
	if !exists {
		c.elog("child record map mismatch %d %s", inodeNum, name)
		return nil
	}

	return entry
}
