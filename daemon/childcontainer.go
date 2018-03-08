// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

// The combination of the effective and published views gives us a coherent
// filesystem state from a local user's perspective
type ChildContainer struct {
	dir      *Directory
	children map[string]InodeId

	// Publishable contains a view of the children that is always consistent
	// The records can be updated in publishable once we are sure they are
	// pointing to consistent children already present in the datastore
	publishable map[InodeId]map[string]quantumfs.DirectoryRecord

	// Effective contains a partial view of the children that cannot be
	// published yet. Once the children publish to the datastore, it would be
	// safe to move their corresponding record from effective to publishable
	// Effective cannot contain any hardlink records, therefore its
	// implementation is a simple hashtable from inodes to records
	effective map[InodeId]map[string]quantumfs.DirectoryRecord
}

func newChildContainer(c *ctx, dir *Directory,
	baseLayerId quantumfs.ObjectKey) (*ChildContainer, []InodeId) {

	defer c.funcIn("newChildContainer").Out()

	container := &ChildContainer{
		dir:         dir,
		children:    make(map[string]InodeId),
		publishable: make(map[InodeId]map[string]quantumfs.DirectoryRecord),
		effective:   make(map[InodeId]map[string]quantumfs.DirectoryRecord),
	}

	uninstantiated := container.loadAllChildren(c, baseLayerId)

	return container, uninstantiated
}

func removeFromMap(m map[InodeId]map[string]quantumfs.DirectoryRecord,
	inodeId InodeId, name string) {

	if len(m[inodeId]) == 1 {
		for k, _ := range m[inodeId] {
			utils.Assert(k == name, "deleting %s instead of %s", k, name)
		}
		delete(m, inodeId)
	} else {
		delete(m[inodeId], name)
	}
}

func addToMap(m map[InodeId]map[string]quantumfs.DirectoryRecord,
	inodeId InodeId, record quantumfs.DirectoryRecord) {

	names, exists := m[inodeId]
	if !exists {
		names = make(map[string]quantumfs.DirectoryRecord)
	}
	if _, exists := names[record.Filename()]; exists {
		utils.Assert(false, "name %s already exists in map",
			record.Filename())
	}
	names[record.Filename()] = record
	m[inodeId] = names
}

func (container *ChildContainer) loadAllChildren(c *ctx,
	baseLayerId quantumfs.ObjectKey) []InodeId {

	defer c.funcIn("ChildContainer::loadAllChildren").Out()

	uninstantiated := make([]InodeId, 0, 200) // 200 arbitrarily chosen

	foreachDentry(c, baseLayerId, func(record quantumfs.DirectoryRecord) {
		c.vlog("Loading child %s", record.Filename())
		childInodeNum := container.loadChild(c, record)
		c.vlog("loaded child %d", childInodeNum)
		uninstantiated = append(uninstantiated, childInodeNum)
	})

	return uninstantiated
}

// Use this when you are loading a child's metadata from the datastore and do not
// know the InodeId.
func (container *ChildContainer) loadChild(c *ctx,
	record quantumfs.DirectoryRecord) InodeId {

	defer c.FuncIn("ChildContainer::loadChild", "name %s type %d",
		record.Filename(), record.Type()).Out()

	// Since we do not have an inodeId this child is/will not be instantiated and
	// so it is placed in the publishable set.

	var inodeId InodeId
	if record.Type() == quantumfs.ObjectTypeHardlink {
		// The hardlink table will have/create an InodeId for us
		fileId := record.FileId()
		inodeId = container.dir.hardlinkTable.findHardlinkInodeId(c,
			fileId, quantumfs.InodeIdInvalid)
		record = newHardlinkLegFromRecord(record,
			container.dir.hardlinkTable)
		container.dir.markHardlinkPath(c, record.Filename(), record.FileId())
	} else {
		inodeId = c.qfs.newInodeId()
	}

	addToMap(container.publishable, inodeId, record)
	container.children[record.Filename()] = inodeId

	return inodeId
}

// Use this when you know the child's InodeId. Either the child must be instantiated
// and dirty, or markPublishable() must be called immediately afterwards for the
// changes set here to eventually be published.
func (container *ChildContainer) setRecord(c *ctx, inodeId InodeId,
	record quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildContainer::setRecord", "inode %d name %s", inodeId,
		record.Filename()).Out()

	// Since we have an inodeId this child is or will be instantiated and so is
	// placed in the effective set.

	utils.Assert(inodeId != quantumfs.InodeIdInvalid,
		"setRecord without inodeId")

	_, isHardlinkLeg := record.(*HardlinkLeg)
	utils.Assert(record.Type() != quantumfs.ObjectTypeHardlink || isHardlinkLeg,
		"setRecord with naked hardlink record")

	addToMap(container.effective, inodeId, record)
	container.children[record.Filename()] = inodeId

	// Build the hardlink path list if we just set a hardlink record
	if record.Type() == quantumfs.ObjectTypeHardlink {
		container.dir.markHardlinkPath(c, record.Filename(), record.FileId())

		// The child is a hardlink which means it will be part of the
		// hardlink map in wsr the next time wsr gets published, therefore,
		// we can mark it as publishable.
		container.makePublishable(c, record.Filename())
	}
}

func (container *ChildContainer) recordByName(c *ctx,
	name string) quantumfs.ImmutableDirectoryRecord {

	return container._recordByName(c, name)
}

// Internal use only method
func (container *ChildContainer) _recordByName(c *ctx,
	name string) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildContainer::_recordByName", "%s", name).Out()

	inodeId := container.inodeNum(name)
	records := container.effective[inodeId]
	if records == nil {
		records = container.publishable[inodeId]
	}

	if records == nil {
		c.vlog("Inode does not exist")
		return nil // Does not exist
	}

	record := records[name]
	if record == nil {
		c.vlog("Name does not exist")
	}
	return record
}

// Note that this will return one arbitrary record in cases where that inode has
// multiple names in this container/directory.
func (container *ChildContainer) recordByInodeId(c *ctx,
	inodeId InodeId) quantumfs.ImmutableDirectoryRecord {

	return container._recordByInodeId(c, inodeId)
}

// Internal use only method
func (container *ChildContainer) _recordByInodeId(c *ctx,
	inodeId InodeId) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildContainer::_recordByInodeId", "inodeId %d",
		inodeId).Out()

	records := container.effective[inodeId]
	if records == nil {
		c.vlog("No effective record")
		records = container.publishable[inodeId]
	}

	if records == nil {
		c.vlog("Does not exist")
		return nil // Does not exist
	}
	for _, record := range records {
		c.vlog("Returning %s", record.Filename())
		return record
	}
	utils.Assert(false, "Empty records listing")
	return nil
}

func (container *ChildContainer) inodeNum(name string) InodeId {
	if inodeId, exists := container.children[name]; exists {
		return inodeId
	}

	return quantumfs.InodeIdInvalid
}

func (container *ChildContainer) count() uint64 {
	return uint64(len(container.children))
}

func (container *ChildContainer) foreachChild(c *ctx, fxn func(name string,
	inodeId InodeId)) {

	for name, inodeId := range container.children {
		fxn(name, inodeId)
	}
}

func (container *ChildContainer) deleteChild(c *ctx,
	name string) (needsReparent quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildContainer::deleteChild", "name %s", name).Out()

	inodeId := container.inodeNum(name)
	if inodeId == quantumfs.InodeIdInvalid {
		c.vlog("name %s does not exist", name)
		return nil
	}

	record := container._recordByName(c, name)

	removeFromMap(container.publishable, inodeId, name)
	removeFromMap(container.effective, inodeId, name)
	delete(container.children, name)

	return record
}

func (container *ChildContainer) renameChild(c *ctx, oldName string,
	newName string) {

	defer c.FuncIn("ChildContainer::renameChild", "%s -> %s",
		oldName, newName).Out()
	utils.Assert(oldName != newName,
		"Identical names must have been handled")
	utils.Assert(container.inodeNum(newName) == quantumfs.InodeIdInvalid,
		"The newName must have been already removed")

	inodeId := container.inodeNum(oldName)
	c.vlog("child %s has inode %d", oldName, inodeId)
	record := container.deleteChild(c, oldName)
	if record == nil {
		c.vlog("oldName doesn't exist")
		return
	}
	record.SetFilename(newName)

	// if this is a hardlink, we must update its creationTime and the accesslist
	// path
	if hardlink, isHardlink := record.(*HardlinkLeg); isHardlink {
		hardlink.setCreationTime(quantumfs.NewTime(time.Now()))
		container.dir.markHardlinkPath(c, record.Filename(), record.FileId())
	}
	container.setRecord(c, inodeId, record)
	container.makePublishable(c, newName)
}

// Modify the effective view of a child with the given function. The child Inode must
// be instantiated and must be on the dirty queue in order for this changes to
// eventually be publishable.
func (container *ChildContainer) modifyChildWithFunc(c *ctx, inodeId InodeId,
	modify func(record quantumfs.DirectoryRecord)) {

	defer c.funcIn("ChildContainer::modifyChildWithFunc").Out()

	record := container._recordByInodeId(c, inodeId)
	if record == nil {
		return
	}

	_, hasEffective := container.effective[inodeId]

	if !hasEffective && record.Type() != quantumfs.ObjectTypeHardlink {
		// We do not modify publishable records in this method. If we don't
		// have an effective entry we must create one. Hardlinks are always
		// publishable, so do not create an effective entry for those types.
		record = record.Clone()
		container.setRecord(c, inodeId, record)
	}

	modify(record)
}

func (container *ChildContainer) directInodes() []InodeId {
	inodes := make([]InodeId, 0, len(container.children))

	for name, inodeId := range container.children {
		records := container.effective[inodeId]
		if records == nil {
			records = container.publishable[inodeId]
		}
		utils.Assert(records != nil, "did not find child %s", name)
		record := records[name]
		_, isHardlink := record.(*HardlinkLeg)
		if !isHardlink {
			inodes = append(inodes, inodeId)
		}
	}
	return inodes
}

func (container *ChildContainer) publishableRecords(
	c *ctx) []quantumfs.DirectoryRecord {

	defer c.funcIn("ChildContainer::publishableRecords").Out()

	records := make([]quantumfs.DirectoryRecord, 0, container.count())
	for _, byInode := range container.publishable {
		for _, record := range byInode {
			records = append(records, record)
		}
	}

	c.vlog("Returning %d records", len(records))
	return records
}

func (c *ChildContainer) records() map[string]quantumfs.ImmutableDirectoryRecord {
	records := make(map[string]quantumfs.ImmutableDirectoryRecord, c.count())

	for name, inodeId := range c.children {
		if effective, exists := c.effective[inodeId]; exists {
			records[name] = effective[name]
		} else {
			records[name] = c.publishable[inodeId][name]
		}
	}

	return records
}

func (container *ChildContainer) makePublishable(c *ctx, name string) {
	defer c.FuncIn("ChildContainer::makePublishable", "%s", name).Out()

	inodeId := container.inodeNum(name)
	utils.Assert(inodeId != quantumfs.InodeIdInvalid, "No such child %s", name)

	record := container.effective[inodeId][name]
	if record == nil {
		c.vlog("Already publishable")
		return
	}

	records := container.publishable[inodeId]
	if records == nil {
		records = make(map[string]quantumfs.DirectoryRecord, 0)
	}
	records[name] = record
	container.publishable[inodeId] = records
	c.vlog("Inode has %d names in this directory", len(records))

	removeFromMap(container.effective, inodeId, name)
}

func (container *ChildContainer) setID(c *ctx, name string,
	key quantumfs.ObjectKey) {

	defer c.FuncIn("ChildContainer::setID", "name %s key %s", name,
		key.String()).Out()

	record := container._recordByName(c, name)
	utils.Assert(record != nil, "Child '%s' not found in setID", name)

	record.SetID(key)
	container.makePublishable(c, name)
}
