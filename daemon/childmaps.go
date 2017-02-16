// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "fmt"
import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
type ChildMap struct {
	wsr *WorkspaceRoot
	dir *Directory

	// can be many to one
	children map[string]InodeId

	childrenRecords map[InodeId][]DirectoryRecordIf
}

func newChildMap(numEntries int, wsr_ *WorkspaceRoot, owner *Directory) *ChildMap {
	return &ChildMap{
		wsr:             wsr_,
		dir:             owner,
		children:        make(map[string]InodeId, numEntries),
		childrenRecords: make(map[InodeId][]DirectoryRecordIf, numEntries),
	}
}

func (cmap *ChildMap) loadChild(c *ctx, entry DirectoryRecordIf) InodeId {

	inodeId := InodeId(quantumfs.InodeIdInvalid)
	if entry.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(entry.ID())
		entry = newHardlink(entry.Filename(), linkId, cmap.wsr)
		inodeId = cmap.wsr.getHardlinkInodeId(c, linkId)
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
		panic("Empty list leftover and not cleaned up")
	}

	return list[0]
}

func (cmap *ChildMap) getRecord(c *ctx, inodeId InodeId,
	name string) DirectoryRecordIf {

	defer c.FuncIn("ChildMap::getRecord", "%d %s", inodeId, name).out()

	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		return nil
	}

	for _, v := range list {
		if v.Filename() == name {
			return cmap.checkForReplace(c, v)
		}
	}

	return nil
}

// We need to check a record when we're returning it so that if a hardlink has nlink
// of 1, that we turn it back into a normal file again
func (cmap *ChildMap) checkForReplace(c *ctx,
	record DirectoryRecordIf) DirectoryRecordIf {

	if record.Type() != quantumfs.ObjectTypeHardlink {
		return record
	}

	link := record.(*Hardlink)

	// This needs to be turned back into a normal file
	newRecord, inodeId := cmap.wsr.removeHardlink(c, link.linkId,
		cmap.dir.inodeNum())

	if newRecord == nil && inodeId == quantumfs.InodeIdInvalid {
		// wsr says hardlink isn't ready for removal yet
		return record
	}

	// Ensure that we update this version of the record with this instance
	// of the hardlink's information
	newRecord.SetFilename(link.Filename())

	// Here we do the opposite of makeHardlink DOWN - we re-insert it
	cmap.setRecord(inodeId, newRecord)
	cmap.dir.dirty(c)

	return newRecord
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

func (cmap *ChildMap) deleteChild(c *ctx,
	name string) (needsReparent DirectoryRecordIf) {

	inodeId, exists := cmap.children[name]
	if exists {
		delete(cmap.children, name)
	}

	record := cmap.getRecord(c, inodeId, name)
	if record == nil {
		return nil
	}

	result := cmap.delRecord(inodeId, name)

	if link, isHardlink := record.(*Hardlink); isHardlink {
		if cmap.wsr.hardlinkDec(link.linkId) {
			// If the refcount was greater than one we shouldn't
			// reparent.
			return nil
		}
	}
	return result
}

func (cmap *ChildMap) renameChild(c *ctx, oldName string,
	newName string) (oldInodeRemoved InodeId) {

	if oldName == newName {
		return quantumfs.InodeIdInvalid
	}

	inodeId, exists := cmap.children[oldName]
	if !exists {
		return quantumfs.InodeIdInvalid
	}

	record := cmap.getRecord(c, inodeId, oldName)
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
	}

	delete(cmap.children, oldName)
	cmap.children[newName] = inodeId
	record.SetFilename(newName)

	if needCleanup {
		return cleanupInodeId
	}

	return quantumfs.InodeIdInvalid
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
	defer c.funcIn("ChildMap::recordByName").out()

	inodeNum, exists := cmap.children[name]
	if !exists {
		return nil
	}

	entry := cmap.getRecord(c, inodeNum, name)
	if entry == nil {
		c.elog("child record map mismatch %d %s", inodeNum, name)
		return nil
	}

	return entry
}

func (cmap *ChildMap) makeHardlink(c *ctx,
	childName string) (copy DirectoryRecordIf, err fuse.Status) {

	childId, exists := cmap.children[childName]
	if !exists {
		c.elog("No child record for %s in childmap", childName)
		return nil, fuse.ENOENT
	}

	child := cmap.getRecord(c, childId, childName)
	if child == nil {
		panic("Mismatched childId and record")
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*Hardlink); isLink {
		recordCopy := *link

		// Ensure we update the ref count for this hardlink
		cmap.wsr.hardlinkInc(link.linkId)

		return &recordCopy, fuse.OK
	}

	// record must be a file type to be hardlinked
	if child.Type() != quantumfs.ObjectTypeSmallFile &&
		child.Type() != quantumfs.ObjectTypeMediumFile &&
		child.Type() != quantumfs.ObjectTypeLargeFile &&
		child.Type() != quantumfs.ObjectTypeVeryLargeFile {

		c.dlog("Cannot hardlink %s - not a file", childName)
		return nil, fuse.EINVAL
	}

	// It needs to become a hardlink now. Hand it off to wsr
	newLink := cmap.wsr.newHardlink(c, childId, child)

	cmap.setRecord(childId, newLink)
	linkCopy := *newLink
	return &linkCopy, fuse.OK
}
