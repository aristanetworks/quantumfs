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
			return v
		}
	}

	return nil
}

// Returns the inodeId used for the child
func (cmap *ChildMap) loadChild(c *ctx, entry DirectoryRecordIf,
	inodeId InodeId) InodeId {

	if entry.Type() == quantumfs.ObjectTypeHardlink {
		linkId := decodeHardlinkKey(entry.ID())
		entry = newHardlink(entry.Filename(), linkId, cmap.wsr)
		establishedInodeId := cmap.wsr.getHardlinkInodeId(c, linkId)

		// If you try to load a hardlink and provide a real inodeId, it
		// should match the actual inodeId for the hardlink or else
		// something is really wrong in the system
		if inodeId != quantumfs.InodeIdInvalid &&
			inodeId != establishedInodeId {

			c.elog("Attempt to set hardlink with mismatched inodeId, "+
				"%d vs %d", inodeId, establishedInodeId)
		}
		inodeId = establishedInodeId
	} else if inodeId == quantumfs.InodeIdInvalid {
		inodeId = c.qfs.newInodeId()
	}

	if entry == nil {
		panic(fmt.Sprintf("Nil DirectoryEntryIf set attempt: %d", inodeId))
	}

	cmap.children[entry.Filename()] = inodeId
	// child is not dirty by default

	cmap.setRecord(inodeId, entry)

	return inodeId
}

func (cmap *ChildMap) count() uint64 {
	return uint64(len(cmap.children))
}

func (cmap *ChildMap) deleteChild(c *ctx,
	name string) (needsReparent DirectoryRecordIf) {

	inodeId, exists := cmap.children[name]
	if !exists {
		return nil
	}

	record := cmap.getRecord(c, inodeId, name)
	if record == nil {
		return nil
	}

	// This may be a hardlink that is due to be converted.
	if hardlink, isHardlink := record.(*Hardlink); isHardlink {
		newRecord, inodeId := cmap.wsr.removeHardlink(c,
			hardlink.linkId)

		// Wsr says we're about to orphan the last hardlink copy
		if newRecord != nil || inodeId != quantumfs.InodeIdInvalid {
			newRecord.SetFilename(hardlink.Filename())
			record = newRecord
			cmap.loadChild(c, newRecord, inodeId)
		}
	}
	delete(cmap.children, name)

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

func (cmap *ChildMap) directInodes() []InodeId {
	rtn := make([]InodeId, 0, len(cmap.childrenRecords))
	for id, record := range cmap.childrenRecords {
		if _, isHardlink := record[0].(*Hardlink); isHardlink {
			continue
		}
		rtn = append(rtn, id)
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
	childId InodeId) (copy DirectoryRecordIf, err fuse.Status) {

	child := cmap.firstRecord(childId)
	if child == nil {
		c.elog("No child record for inode id %d in childmap", childId)
		return nil, fuse.ENOENT
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*Hardlink); isLink {
		c.vlog("Already a hardlink")

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

		c.dlog("Cannot hardlink %s - not a file", child.Filename())
		return nil, fuse.EINVAL
	}

	// It needs to become a hardlink now. Hand it off to wsr
	c.vlog("Converting into a hardlink")
	newLink := cmap.wsr.newHardlink(c, childId, child)

	cmap.setRecord(childId, newLink)
	linkCopy := *newLink
	return &linkCopy, fuse.OK
}
