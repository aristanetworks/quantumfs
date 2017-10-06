// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"fmt"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
type ChildMap struct {
	wsr *WorkspaceRoot

	// can be many to one
	children map[string]InodeId

	// Use childrenRecords() to access this
	childrenRecords_ map[InodeId][]quantumfs.DirectoryRecord
}

func newChildMap(c *ctx, wsr_ *WorkspaceRoot,
	baseLayerId quantumfs.ObjectKey) (*ChildMap, []InodeId) {

	defer c.FuncIn("newChildMap", "baseLayer %s", baseLayerId.String()).Out()

	cmap := &ChildMap{
		wsr:              wsr_,
		children:         make(map[string]InodeId),
		childrenRecords_: make(map[InodeId][]quantumfs.DirectoryRecord),
	}

	uninstantiated := cmap.loadAllChildren(c, baseLayerId)

	return cmap, uninstantiated
}

func (cmap *ChildMap) loadAllChildren(c *ctx,
	baseLayerId quantumfs.ObjectKey) []InodeId {

	defer c.funcIn("ChildMap::loadAllChildren").Out()

	uninstantiated := make([]InodeId, 0, 200) // 200 arbitrarily chosen

	foreachDentry(c, baseLayerId, func(record *quantumfs.DirectRecord) {
		inodeNum, exists := cmap.children[record.Filename()]
		if !exists {
			inodeNum = quantumfs.InodeIdInvalid
		}
		c.vlog("Loading child %s with id %d", record.Filename(), inodeNum)
		childInodeNum := cmap.loadChild(c, record, inodeNum)
		c.vlog("loaded child %d", childInodeNum)
		uninstantiated = append(uninstantiated, childInodeNum)
	})

	return uninstantiated
}

func (cmap *ChildMap) baseLayerIs(c *ctx, baseLayerId quantumfs.ObjectKey) {
	defer c.funcIn("ChildMap::baseLayerIs").Out()

	cmap.childrenRecords_ = make(map[InodeId][]quantumfs.DirectoryRecord)
	cmap.loadAllChildren(c, baseLayerId)
}

func (cmap *ChildMap) childrenRecords() map[InodeId][]quantumfs.DirectoryRecord {
	return cmap.childrenRecords_
}

func (cmap *ChildMap) setRecord(inodeId InodeId, record quantumfs.DirectoryRecord) {
	// To prevent overwriting one map, but not the other, ensure we clear first
	cmap.delRecord(inodeId, record.Filename())

	list, exists := cmap.childrenRecords()[inodeId]
	if !exists {
		list = make([]quantumfs.DirectoryRecord, 0)
	}

	list = append(list, record)
	cmap.childrenRecords()[inodeId] = list
}

func (cmap *ChildMap) delRecord(inodeId InodeId,
	name string) quantumfs.DirectoryRecord {

	list, exists := cmap.childrenRecords()[inodeId]
	if !exists {
		return nil
	}

	for i, v := range list {
		if v.Filename() == name {
			list = append(list[:i], list[i+1:]...)
			if len(list) > 0 {
				cmap.childrenRecords()[inodeId] = list
			} else {
				delete(cmap.childrenRecords(), inodeId)
			}
			return v
		}
	}

	return nil
}

func (cmap *ChildMap) firstRecord(inodeId InodeId) quantumfs.DirectoryRecord {
	list, exists := cmap.childrenRecords()[inodeId]
	if !exists {
		return nil
	}

	if len(list) == 0 {
		panic("Empty list leftover and not cleaned up")
	}

	return list[0]
}

func (cmap *ChildMap) getRecord(c *ctx, inodeId InodeId,
	name string) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildMap::getRecord", "%d %s", inodeId, name).Out()

	list, exists := cmap.childrenRecords()[inodeId]
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
func (cmap *ChildMap) loadChild(c *ctx, entry quantumfs.DirectoryRecord,
	inodeId InodeId) InodeId {

	defer c.FuncIn("ChildMap::loadChild", "inode %d", inodeId).Out()

	if entry.Type() == quantumfs.ObjectTypeHardlink {
		fileId := entry.FileId()
		// hardlink leg creation time is stored in its ContentTime
		entry = newHardlink(entry.Filename(), fileId, entry.ContentTime(),
			cmap.wsr)
		establishedInodeId := cmap.wsr.getHardlinkInodeId(c, fileId, inodeId)

		// If you try to load a hardlink and provide a real inodeId, it
		// should normally match the actual inodeId.
		// The only exception is when the file used to be of another type,
		// but after a refresh it has been changed to be a hardlink.
		if inodeId != quantumfs.InodeIdInvalid &&
			inodeId != establishedInodeId {

			c.wlog("requested hardlink inodeId %d exists as %d",
				inodeId, establishedInodeId)
		}
		inodeId = establishedInodeId
	} else if inodeId == quantumfs.InodeIdInvalid {
		inodeId = c.qfs.newInodeId()
	}

	if entry == nil {
		panic(fmt.Sprintf("Nil DirectoryEntry for inode %d", inodeId))
	}

	utils.Assert(inodeId != quantumfs.InodeIdInvalid,
		"Inode for loaded child %s is zero", entry.Filename())
	cmap.children[entry.Filename()] = inodeId
	// child is not dirty by default

	cmap.setRecord(inodeId, entry)

	return inodeId
}

func (cmap *ChildMap) count() uint64 {
	return uint64(len(cmap.children))
}

func (cmap *ChildMap) foreachChild(c *ctx, fxn func(name string, inodeId InodeId)) {
	for childname, childId := range cmap.children {
		fxn(childname, childId)
	}
}

func (cmap *ChildMap) deleteChild(c *ctx,
	name string, fixHardlinks bool) (needsReparent quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildMap::deleteChild", "name %s", name).Out()

	inodeId, exists := cmap.children[name]
	if !exists {
		c.vlog("name does not exist")
		return nil
	}

	record := cmap.getRecord(c, inodeId, name)
	if record == nil {
		c.vlog("record does not exist")
		return nil
	}

	// This may be a hardlink that is due to be converted.
	if hardlink, isHardlink := record.(*Hardlink); isHardlink && fixHardlinks {
		newRecord, inodeId := cmap.wsr.removeHardlink(c,
			hardlink.fileId)

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
		if !fixHardlinks {
			return nil
		}
		if !cmap.wsr.hardlinkExists(c, link.fileId) {
			c.vlog("hardlink does not exist")
			return nil
		}
		if cmap.wsr.hardlinkDec(link.fileId) {
			// If the refcount was greater than one we shouldn't
			// reparent.
			c.vlog("Hardlink referenced elsewhere")
			return nil
		}
	}
	return result
}

func (cmap *ChildMap) renameChild(c *ctx, oldName string, newName string) {
	defer c.FuncIn("ChildMap::renameChild", "oldName %s newName %s", oldName,
		newName).Out()

	if oldName == newName {
		c.vlog("Names are identical")
		return
	}

	inodeId, exists := cmap.children[oldName]
	if !exists {
		c.vlog("oldName doesn't exist")
		return
	}

	record := cmap.getRecord(c, inodeId, oldName)
	if record == nil {
		c.vlog("oldName record doesn't exist")
		panic("inode set without record")
	}

	// record whether we need to cleanup a file we're overwriting
	cleanupInodeId, needCleanup := cmap.children[newName]
	if needCleanup {
		// we have to cleanup before we move, to allow the case where we
		// rename a hardlink to an existing one with the same inode
		cmap.delRecord(cleanupInodeId, newName)
		delete(cmap.children, newName)
		c.vlog("cleanupInodeId %d", cleanupInodeId)
	}

	delete(cmap.children, oldName)
	cmap.children[newName] = inodeId
	record.SetFilename(newName)

	// if this is a hardlink, we must update its creationTime
	if hardlink, isHardlink := record.(*Hardlink); isHardlink {
		hardlink.creationTime = quantumfs.NewTime(time.Now())
	}
}

func (cmap *ChildMap) inodeNum(name string) InodeId {
	if inodeId, exists := cmap.children[name]; exists {
		return inodeId
	}

	return quantumfs.InodeIdInvalid
}

func (cmap *ChildMap) directInodes() []InodeId {
	rtn := make([]InodeId, 0, len(cmap.childrenRecords()))
	for id, record := range cmap.childrenRecords() {
		if _, isHardlink := record[0].(*Hardlink); isHardlink {
			continue
		}
		rtn = append(rtn, id)
	}

	return rtn
}

func (cmap *ChildMap) records() []quantumfs.DirectoryRecord {
	rtn := make([]quantumfs.DirectoryRecord, 0, len(cmap.childrenRecords()))
	for _, i := range cmap.childrenRecords() {
		rtn = append(rtn, i...)
	}

	return rtn
}

func (cmap *ChildMap) record(inodeNum InodeId) quantumfs.DirectoryRecord {
	return cmap.firstRecord(inodeNum)
}

func (cmap *ChildMap) recordByName(c *ctx, name string) quantumfs.DirectoryRecord {
	defer c.FuncIn("ChildMap::recordByName", "name %s", name).Out()

	inodeNum, exists := cmap.children[name]
	if !exists {
		c.vlog("name doesn't exist")
		return nil
	}

	entry := cmap.getRecord(c, inodeNum, name)
	if entry == nil {
		c.elog("child record map mismatch %d %s", inodeNum, name)
		return nil
	}

	return entry
}

func (cmap *ChildMap) makeHardlink(c *ctx, childId InodeId) (
	copy quantumfs.DirectoryRecord, err fuse.Status) {

	defer c.FuncIn("ChildMap::makeHardlink", "inode %d", childId).Out()

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
		cmap.wsr.hardlinkInc(link.fileId)

		return &recordCopy, fuse.OK
	}

	// record must be a file type to be hardlinked
	if child.Type() != quantumfs.ObjectTypeSmallFile &&
		child.Type() != quantumfs.ObjectTypeMediumFile &&
		child.Type() != quantumfs.ObjectTypeLargeFile &&
		child.Type() != quantumfs.ObjectTypeVeryLargeFile &&
		child.Type() != quantumfs.ObjectTypeSymlink &&
		child.Type() != quantumfs.ObjectTypeSpecial {

		c.dlog("Cannot hardlink %s - not a file", child.Filename())
		return nil, fuse.EINVAL
	}

	childname := child.Filename()
	// remove the record from the childmap before donating it to be a hardlink
	cmap.delRecord(childId, childname)

	c.vlog("Converting %s into a hardlink", childname)
	newLink := cmap.wsr.newHardlink(c, childId, child)

	linkSrcCopy := newLink.Clone()
	linkSrcCopy.SetFilename(childname)
	cmap.setRecord(childId, linkSrcCopy)

	newLink.creationTime = quantumfs.NewTime(time.Now())
	newLink.SetContentTime(newLink.creationTime)
	return newLink, fuse.OK
}
