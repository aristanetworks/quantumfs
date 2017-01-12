// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"
import "github.com/aristanetworks/quantumfs"

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
type ChildMap struct {
	// can be many to one
	children      map[string]InodeId
	dirtyChildren map[InodeId]InodeId // a set

	childrenRecords map[InodeId][]DirectoryRecordIf
}

func newChildMap(numEntries int) *ChildMap {
	return &ChildMap{
		children:        make(map[string]InodeId, numEntries),
		dirtyChildren:   make(map[InodeId]InodeId, 0),
		childrenRecords: make(map[InodeId][]DirectoryRecordIf, numEntries),
	}
}

func (cmap *ChildMap) loadChild(c *ctx, entry DirectoryRecordIf,
	wsr *WorkspaceRoot) InodeId {

	inodeId := InodeId(quantumfs.InodeIdInvalid)
	if entry.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(entry.ID())
		entry = newHardlink(entry.Filename(), linkId, wsr)
		inodeId = wsr.getHardlinkInodeId(c, linkId)
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

func (cmap *ChildMap) setRecord(inodeId InodeId, record DirectoryRecordIf) {
	// To prevent overwriting one map, but not the other, ensure we clear first
	cmap.delRecord(inodeId, record.Filename())

	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		list = make([]DirectoryRecordIf, 0)
	}

	list = append(list, record)
	cmap.childrenRecords[inodeId] = list
}

func (cmap *ChildMap) delRecord(inodeId InodeId, name string) DirectoryRecordIf {
	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		return nil
	}

	for i, v := range list {
		if v.Filename() == name {
			list = append(list[:i], list[i+1:]...)
			if len(list) > 0 {
				cmap.childrenRecords[inodeId] = list
			} else {
				delete(cmap.childrenRecords, inodeId)
			}
			return v
		}
	}

	return nil
}

func (cmap *ChildMap) firstRecord(inodeId InodeId) DirectoryRecordIf {
	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		return nil
	}

	if len(list) == 0 {
		panic("Empty list leftover and not cleanup up")
	}

	return list[0]
}

func (cmap *ChildMap) getRecord(inodeId InodeId, name string) DirectoryRecordIf {
	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		return nil
	}

	for _, v := range list {
		if v.Filename() == name {
			return v
		}
	}

	return nil
}

func (cmap *ChildMap) setChild_(c *ctx, entry DirectoryRecordIf, inodeId InodeId) {
	if entry == nil {
		panic(fmt.Sprintf("Nil DirectoryEntryIf set attempt: %d", inodeId))
	}

	cmap.children[entry.Filename()] = inodeId
	// child is not dirty by default

	cmap.setRecord(inodeId, entry)
}

func (cmap *ChildMap) count() uint64 {
	return uint64(len(cmap.children))
}

func (cmap *ChildMap) deleteChild(name string,
	wsr *WorkspaceRoot) (needsReparent DirectoryRecordIf) {

	inodeId, exists := cmap.children[name]
	if exists {
		delete(cmap.dirtyChildren, inodeId)
		delete(cmap.children, name)
	}

	record := cmap.getRecord(inodeId, name)
	if record == nil {
		return nil
	}

	if link, isHardlink := record.(*Hardlink); isHardlink {
		wsr.chgHardlinkRef(link.linkId, false)
	}

	return cmap.delRecord(inodeId, name)
}

func (cmap *ChildMap) renameChild(oldName string,
	newName string) (oldInodeRemoved InodeId) {

	if oldName == newName {
		return quantumfs.InodeIdInvalid
	}

	inodeId, exists := cmap.children[oldName]
	if !exists {
		return quantumfs.InodeIdInvalid
	}

	record := cmap.getRecord(inodeId, oldName)
	if record == nil {
		panic("inode set without record")
	}

	// record whether we need to cleanup a file we're overwriting
	cleanupInodeId, needCleanup := cmap.children[newName]
	if needCleanup {
		// we have to cleanup before we move, to allow the case where we
		// rename a hardlink to an existing one with the same inode
		cmap.delRecord(cleanupInodeId, newName)
		delete(cmap.children, newName)
		delete(cmap.dirtyChildren, cleanupInodeId)
	}

	delete(cmap.children, oldName)
	cmap.children[newName] = inodeId
	record.SetFilename(newName)

	if needCleanup {
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
	entry := cmap.firstRecord(inodeNum)
	if entry == nil {
		panic(fmt.Sprintf("setDirty on empty record for %d",
			inodeNum))
	} else if entry.Type() == quantumfs.ObjectTypeHardlink {
		// we don't need to track dirty hardlinks
		return
	}

	cmap.dirtyChildren[inodeNum] = inodeNum
}

func (cmap *ChildMap) clearDirty(inodeNum InodeId) {
	delete(cmap.dirtyChildren, inodeNum)
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
		rtn = append(rtn, i...)
	}

	return rtn
}

func (cmap *ChildMap) record(inodeNum InodeId) DirectoryRecordIf {
	return cmap.firstRecord(inodeNum)
}

func (cmap *ChildMap) recordByName(c *ctx, name string) DirectoryRecordIf {
	inodeNum, exists := cmap.children[name]
	if !exists {
		return nil
	}

	entry := cmap.getRecord(inodeNum, name)
	if entry == nil {
		c.elog("child record map mismatch %d %s", inodeNum, name)
		return nil
	}

	return entry
}
