// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// The combination of the effective and published views gives us a coherent
// filesystem state from a local user's perspective
type ChildContainer struct {
	children map[string]InodeId
	dir      *Directory

	// Publishable contains a view of the children that is always consistent
	// The records can be updated in publishable once we are sure they are
	// pointing to consistent children already present in the datastore
	publishable ChildMap

	// Effective contains a partial view of the children that cannot be
	// published yet. Once the children publish to the datastore, it would be
	// safe to move their corresponding record from effective to publishable
	// Effective cannot contain any hardlink records, therefore its
	// implementation is a simple hashtable from inodes to records
	effective EffectiveMap
}

type EffectiveMap struct {
	m map[InodeId]quantumfs.DirectoryRecord
}

func newEffectiveMap() *EffectiveMap {
	return &EffectiveMap{
		m: make(map[InodeId]quantumfs.DirectoryRecord),
	}
}

func (em *EffectiveMap) get(inodeId InodeId) quantumfs.DirectoryRecord {
	record, exists := em.m[inodeId]
	if exists {
		return record
	}
	return nil
}

func (em *EffectiveMap) del(inodeId InodeId) {
	delete(em.m, inodeId)
}

func (em *EffectiveMap) set(inodeId InodeId, record quantumfs.DirectoryRecord) {
	utils.Assert(em.get(inodeId) == nil, "Setting inodeId for existing record")
	utils.Assert(record.Type() != quantumfs.ObjectTypeHardlink,
		"Hardlink records are always publishable.")
	em.m[inodeId] = record
}

func (em *EffectiveMap) foreach(fxn func(inodeId InodeId,
	record quantumfs.DirectoryRecord)) {

	for inodeId, record := range em.m {
		fxn(inodeId, record)
	}
}

func newChildContainer(c *ctx, dir_ *Directory,
	baseLayerId quantumfs.ObjectKey) (*ChildContainer, []InodeId) {

	publishable, uninstantiated := newChildMap(c, dir_, baseLayerId)
	effective := newEffectiveMap()
	children := make(map[string]InodeId)

	publishable.foreachChild(c, func(name string, inodeId InodeId) {
		c.vlog("Adding child %s to the map", name)
		children[name] = inodeId
	})

	return &ChildContainer{
		dir:         dir_,
		children:    children,
		publishable: *publishable,
		effective:   *effective,
	}, uninstantiated
}

func (container *ChildContainer) setRecord(c *ctx, inodeId InodeId,
	record quantumfs.DirectoryRecord) {

	container.publishable.setRecord(c, inodeId, record)
}

func (container *ChildContainer) getRecord(c *ctx, inodeId InodeId,
	name string) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildContainer::getRecord", "%d %s", inodeId, name).Out()
	if record := container.effective.get(inodeId); record != nil {
		return record
	}
	return container.publishable.getRecord(c, inodeId, name)
}

func (container *ChildContainer) loadChild(c *ctx, record quantumfs.DirectoryRecord,
	inodeId InodeId) InodeId {

	defer c.FuncIn("ChildContainer::loadChild", "inode %d", inodeId).Out()
	if record.Type() == quantumfs.ObjectTypeHardlink {
		return container.loadPublishableChild(c, record, inodeId)
	}
	if inodeId != quantumfs.InodeIdInvalid {
		container.effective.set(inodeId, record)
	} else {
		// As there is no inodeNum, there doesn't exist an instantiated inode
		// for this child so its entry must be made publishable immediately.
		inodeId = c.qfs.newInodeId()
		container.publishable.loadChild(c, record, inodeId)
	}

	container.children[record.Filename()] = inodeId

	return inodeId
}

func (container *ChildContainer) loadPublishableChild(c *ctx,
	record quantumfs.DirectoryRecord, inodeId InodeId) InodeId {

	defer c.FuncIn("ChildContainer::loadPublishableChild", "inode %d",
		inodeId).Out()
	inodeId = container.publishable.loadChild(c, record, inodeId)
	container.children[record.Filename()] = inodeId
	container.effective.del(inodeId)
	return inodeId
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

func (container *ChildContainer) deleteChild(c *ctx, name string,
	fixHardlinks bool) (needsReparent quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildContainer::deleteChild", "name %s", name).Out()

	inodeId := container.children[name]
	if inodeId == quantumfs.InodeIdInvalid {
		c.vlog("name %s does not exist", name)
		return nil
	}
	delete(container.children, name)
	effectiveRecord := container.effective.get(inodeId)
	container.effective.del(inodeId)
	record := container.publishable.deleteChild(c, inodeId, name, fixHardlinks)
	if effectiveRecord != nil {
		return effectiveRecord
	}
	return record
}

func (container *ChildContainer) renameChild(c *ctx, oldName string,
	newName string) {

	defer c.FuncIn("ChildContainer::renameChild", "%s -> %s",
		oldName, newName).Out()
	if oldName == newName {
		c.vlog("Names are identical")
		return
	}
	inodeId := container.inodeNum(oldName)
	c.vlog("child %s has inode %d", oldName, inodeId)
	record := container.getRecord(c, inodeId, oldName)
	if record == nil {
		c.vlog("oldName doesn't exist")
		return
	}
	container.deleteChild(c, newName, true)
	container.deleteChild(c, oldName, false)
	record.SetFilename(newName)

	// if this is a hardlink, we must update its creationTime and the accesslist
	// path
	if hardlink, isHardlink := record.(*HardlinkLeg); isHardlink {
		hardlink.setCreationTime(quantumfs.NewTime(time.Now()))
		container.dir.markHardlinkPath(c, record.Filename(), record.FileId())
	}
	container.loadPublishableChild(c, record, inodeId)

	delete(container.children, oldName)
}

func (container *ChildContainer) inodeNum(name string) InodeId {
	if inodeId, exists := container.children[name]; exists {
		return inodeId
	}

	return quantumfs.InodeIdInvalid
}

func (container *ChildContainer) directInodes() []InodeId {
	inodes := make([]InodeId, 0, len(container.children))

	for _, inodeId := range container.children {
		isHardlink, _ := container.dir.hardlinkTable.checkHardlink(
			inodeId)
		if !isHardlink {
			inodes = append(inodes, inodeId)
		}
	}
	return inodes
}

func (container *ChildContainer) publishableRecords(
	c *ctx) []quantumfs.DirectoryRecord {

	defer c.funcIn("ChildContainer::publishableRecords").Out()
	return container.publishable.records()
}

func (container *ChildContainer) records() []quantumfs.DirectoryRecord {
	publishableRecords := container.publishable.records()

	records := make([]quantumfs.DirectoryRecord, 0, container.count())
	seen := make(map[string]bool, len(records))

	container.effective.foreach(func(inodeId InodeId,
		record quantumfs.DirectoryRecord) {

		records = append(records, record)
		seen[record.Filename()] = true
	})

	for _, record := range publishableRecords {
		if _, isSeen := seen[record.Filename()]; isSeen {
			continue
		}
		records = append(records, record)
	}
	return records
}

func (container *ChildContainer) record(
	inodeId InodeId) quantumfs.DirectoryRecord {

	if record := container.effective.get(inodeId); record != nil {
		return record
	}
	return container.publishable.record(inodeId)
}

func (container *ChildContainer) recordByName(c *ctx,
	name string) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildContainer::recordByName", "%s", name).Out()
	inodeId := container.children[name]
	if inodeId == quantumfs.InodeIdInvalid {
		return nil
	}
	if record := container.effective.get(inodeId); record != nil {
		return record
	}
	return container.publishable.getRecord(c, inodeId, name)
}

func (container *ChildContainer) makeHardlink(c *ctx, childId InodeId) (
	copy quantumfs.DirectoryRecord, err fuse.Status) {

	defer c.FuncIn("ChildContainer::makeHardlink", "inode %d", childId).Out()
	record := container.record(childId)
	if record == nil {
		c.elog("No child record for inode %d", childId)
		return nil, fuse.ENOENT
	}
	// The child is becoming a hardlink which means it will be part of the
	// hardlink map in wsr the next time wsr gets published, therefore,
	// we can mark it as publishable
	container.deleteChild(c, record.Filename(), false)
	container.loadPublishableChild(c, record, childId)
	return container.publishable.makeHardlink(c, childId)
}

func (container *ChildContainer) makePublishable(c *ctx, name string) {
	defer c.FuncIn("ChildContainer::makePublishable", "%s", name).Out()

	inodeId := container.inodeNum(name)
	if inodeId == quantumfs.InodeIdInvalid {
		panic("No such child " + name)
	}

	record := container.record(inodeId)
	container.deleteChild(c, name, false)
	container.loadPublishableChild(c, record, inodeId)
}
