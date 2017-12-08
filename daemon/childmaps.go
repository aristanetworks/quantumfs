// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// Handles map coordination and partial map pairing (for hardlinks) since now the
// mapping between maps isn't one-to-one.
type ChildMap struct {
	dir             *Directory
	childrenRecords map[InodeId][]quantumfs.DirectoryRecord
}

func newChildMap(c *ctx, dir_ *Directory,
	baseLayerId quantumfs.ObjectKey) (*ChildMap, []InodeId) {

	defer c.FuncIn("newChildMap", "baseLayer %s", baseLayerId.String()).Out()

	cmap := &ChildMap{
		dir:             dir_,
		childrenRecords: make(map[InodeId][]quantumfs.DirectoryRecord),
	}

	uninstantiated := cmap.loadAllChildren(c, baseLayerId)

	return cmap, uninstantiated
}

func (cmap *ChildMap) loadAllChildren(c *ctx,
	baseLayerId quantumfs.ObjectKey) []InodeId {

	defer c.funcIn("ChildMap::loadAllChildren").Out()

	uninstantiated := make([]InodeId, 0, 200) // 200 arbitrarily chosen

	foreachDentry(c, baseLayerId, func(record quantumfs.DirectoryRecord) {
		c.vlog("Loading child %s", record.Filename())
		childInodeNum := cmap.loadChild(c, record, quantumfs.InodeIdInvalid)
		c.vlog("loaded child %d", childInodeNum)
		uninstantiated = append(uninstantiated, childInodeNum)
	})

	return uninstantiated
}

func (cmap *ChildMap) setRecord(c *ctx, inodeId InodeId,
	record quantumfs.DirectoryRecord) {

	// To prevent overwriting one map, but not the other, ensure we clear first
	cmap.delRecord(inodeId, record.Filename())

	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		list = make([]quantumfs.DirectoryRecord, 0)
	}

	list = append(list, record)
	cmap.childrenRecords[inodeId] = list

	// Build the hardlink path list if we just set a hardlink record
	if record.Type() == quantumfs.ObjectTypeHardlink {
		cmap.dir.markHardlinkPath(c, record.Filename(), record.FileId())
	}
}

func (cmap *ChildMap) delRecord(inodeId InodeId,
	name string) quantumfs.DirectoryRecord {

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

func (cmap *ChildMap) firstRecord(inodeId InodeId) quantumfs.DirectoryRecord {
	list, exists := cmap.childrenRecords[inodeId]
	if !exists {
		return nil
	}

	if len(list) == 0 {
		panic("Empty list leftover and not cleaned up")
	} else if len(list) > 1 {
		utils.Assert(list[0].Type() == quantumfs.ObjectTypeHardlink,
			"Wrong type %d", list[0].Type())
	}

	return list[0]
}

func (cmap *ChildMap) getRecord(c *ctx, inodeId InodeId,
	name string) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildMap::getRecord", "%d %s", inodeId, name).Out()

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
func (cmap *ChildMap) loadChild(c *ctx, entry quantumfs.DirectoryRecord,
	inodeId InodeId) InodeId {

	defer c.FuncIn("ChildMap::loadChild", "inode %d", inodeId).Out()

	if entry.Type() == quantumfs.ObjectTypeHardlink {
		fileId := entry.FileId()
		if _, isHardlink := entry.(*Hardlink); !isHardlink {
			// hardlink leg creation time is stored in its ContentTime
			entry = newHardlink(entry.Filename(), fileId,
				entry.ContentTime(), cmap.dir.hardlinkTable)
		}

		inodeId = cmap.dir.hardlinkTable.findHardlinkInodeId(c,
			fileId, inodeId)
	}

	if inodeId == quantumfs.InodeIdInvalid {
		inodeId = c.qfs.newInodeId()
	}
	cmap.setRecord(c, inodeId, entry)

	return inodeId
}

func (cmap *ChildMap) foreachChild(c *ctx, fxn func(name string, inodeId InodeId)) {
	for inodeId, recordList := range cmap.childrenRecords {
		for _, record := range recordList {
			fxn(record.Filename(), inodeId)
		}
	}
}

func (cmap *ChildMap) deleteChild(c *ctx, inodeId InodeId,
	name string, fixHardlinks bool) (needsReparent quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildMap::deleteChild", "name %s", name).Out()

	record := cmap.getRecord(c, inodeId, name)
	if record == nil {
		c.vlog("record does not exist")
		return nil
	}

	// This may be a hardlink that is due to be converted.
	if hardlink, isHardlink := record.(*Hardlink); isHardlink && fixHardlinks {
		newRecord, inodeId := cmap.dir.hardlinkTable.removeHardlink(c,
			hardlink.fileId)

		// Wsr says we're about to orphan the last hardlink copy
		if newRecord != nil || inodeId != quantumfs.InodeIdInvalid {
			newRecord.SetFilename(hardlink.Filename())
			record = newRecord
			cmap.loadChild(c, newRecord, inodeId)
			// XXX This child must be moved to the effective view now.
		}
	}
	result := cmap.delRecord(inodeId, name)

	if link, isHardlink := record.(*Hardlink); isHardlink {
		if !fixHardlinks {
			return nil
		}
		if !cmap.dir.hardlinkTable.hardlinkExists(c, link.fileId) {
			c.vlog("hardlink does not exist")
			return nil
		}
		if cmap.dir.hardlinkTable.hardlinkDec(link.fileId) {
			// If the refcount was greater than one we shouldn't
			// reparent.
			c.vlog("Hardlink referenced elsewhere")
			return nil
		}
	}
	return result
}

func (cmap *ChildMap) records() []quantumfs.DirectoryRecord {
	rtn := make([]quantumfs.DirectoryRecord, 0, len(cmap.childrenRecords))
	for _, i := range cmap.childrenRecords {
		rtn = append(rtn, i...)
	}

	return rtn
}

func (cmap *ChildMap) record(inodeNum InodeId) quantumfs.DirectoryRecord {
	return cmap.firstRecord(inodeNum)
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
		cmap.dir.hardlinkTable.hardlinkInc(link.fileId)

		return &recordCopy, fuse.OK
	}

	// record must be a file type to be hardlinked
	if !child.Type().IsRegularFile() &&
		child.Type() != quantumfs.ObjectTypeSymlink &&
		child.Type() != quantumfs.ObjectTypeSpecial {

		c.dlog("Cannot hardlink %s - not a file", child.Filename())
		return nil, fuse.EINVAL
	}

	childname := child.Filename()
	// remove the record from the childmap before donating it to be a hardlink
	cmap.delRecord(childId, childname)

	c.vlog("Converting %s into a hardlink", childname)
	newLink := cmap.dir.hardlinkTable.newHardlink(c, childId, child)

	linkSrcCopy := newLink.Clone()
	linkSrcCopy.SetFilename(childname)
	cmap.setRecord(c, childId, linkSrcCopy)

	newLink.creationTime = quantumfs.NewTime(time.Now())
	newLink.SetContentTime(newLink.creationTime)
	return newLink, fuse.OK
}
