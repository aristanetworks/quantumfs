// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

// A map of inodeIds to the list of children of the directory with that
// inode Id
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
		if _, isHardlink := entry.(*HardlinkLeg); !isHardlink {
			// hardlink leg creation time is stored in its ContentTime
			entry = newHardlinkLeg(entry.Filename(), fileId,
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

func (cmap *ChildMap) records() []quantumfs.DirectoryRecord {
	rtn := make([]quantumfs.DirectoryRecord, 0, len(cmap.childrenRecords))
	for _, i := range cmap.childrenRecords {
		rtn = append(rtn, i...)
	}

	return rtn
}

func (cmap *ChildMap) recordByInodeId(inodeNum InodeId) quantumfs.DirectoryRecord {
	return cmap.firstRecord(inodeNum)
}
