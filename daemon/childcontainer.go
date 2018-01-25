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

	container := &ChildContainer{
		dir:         dir,
		children:    make(map[string]InodeId),
		publishable: make(map[InodeId]map[string]quantumfs.DirectoryRecord),
		effective:   make(map[InodeId]map[string]quantumfs.DirectoryRecord),
	}

	uninstantiated := container.loadAllChildren(c, baseLayerId)

	return container, uninstantiated
}

func (container *ChildContainer) loadAllChildren(c *ctx,
	baseLayerId quantumfs.ObjectKey) []InodeId {

	defer c.funcIn("ChildContainer::loadAllChildren").Out()

	uninstantiated := make([]InodeId, 0, 200) // 200 arbitrarily chosen

	foreachDentry(c, baseLayerId, func(record quantumfs.DirectoryRecord) {
		c.vlog("Loading child %s", record.Filename())
		childInodeNum := container.loadChild(c, record,
			quantumfs.InodeIdInvalid)
		container.children[record.Filename()] = childInodeNum
		c.vlog("loaded child %d", childInodeNum)
		uninstantiated = append(uninstantiated, childInodeNum)
	})

	return uninstantiated
}

func (container *ChildContainer) loadChild(c *ctx, record quantumfs.DirectoryRecord
	) InodeId {

	defer c.FuncIn("ChildContainer::loadChild", "inode %d", inodeId).Out()

	// Since we do not have an inodeId this child is/will not be instantiated and
	// so it placed in the publishable set.
	
	inodeId = c.qfs.newInodeId()

	names, exists := container.publishable[inodeId]
	if !exists {
		names = make(map[string]quantumfs.DirectoryRecord)
	}

	names[record.Filename()] = record

	container.publishable[inodeId] = names

	container.children[record.Filename()] = inodeId

	return inodeId
}

func (container *ChildContainer) setRecord(c *ctx, inodeId InodeId,
	record quantumfs.DirectoryRecord) {

	// Since we have an inodeId this child is or will be instantiated and so is
	// placed in the effective set.

	names, exists := container.effective[inodeId]
	if !exists {
		names = make(map[string]quantumfs.DirectoryRecord)
	}

	names[record.Filename()] = record

	container.effective[inodeId] = names

	// Build the hardlink path list if we just set a hardlink record
	if record.Type() == quantumfs.ObjectTypeHardlink {
		container.dir.markHardlinkPath(c, record.Filename(), record.FileId())
	}
}

func (container *ChildContainer) delRecord(c *ctx, inodeId InodeId,
	name string) quantumfs.DirectoryRecord {

	record := container.getRecordByName(c, name)
	if record == nil {
		return nil // Doesn't exist
	}

	delete(container.publishable[inodeId], name)
	delete(container.effective[inodeId], name)
	delete(container.children, name)

	return record
}

func (container *ChildContainer) getRecordByName(c *ctx,
	name string) quantumfs.DirectoryRecord {

	defer c.FuncIn("ChildContainer::getRecordByName", "%s", name).Out()

	inodeId := container.inodeNum(name)
	records := container.effective[inodeId]
	if records == nil {
		records = container.publishable[inodeId]
	}

	if records == nil {
		return nil // Does not exist
	}
	return records[name]
}

// Note that this will return one arbitrary record in cases where that inode has
// multiple names in this container/directory.
func (container *ChildContainer) getRecordById(c *ctx,
	inodeId InodeId) quantumfs.DirectoryRecord {

	records := container.effective[inodeId]
	if records == nil {
		records = container.publishable[inodeId]
	}

	if records == nil {
		return nil // Does not exist
	}
	for _, record := range records {
		return record
	}
	utils.Assert(false, "Empty records listing")
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

func (container *ChildContainer) deleteChild(c *ctx, name string,
	fixHardlinks bool) (needsReparent quantumfs.DirectoryRecord) {

	defer c.FuncIn("ChildContainer::deleteChild", "name %s", name).Out()

	inodeId := container.inodeNum(name)
	if inodeId == quantumfs.InodeIdInvalid {
		c.vlog("name %s does not exist", name)
		return nil
	}
	record := container.getRecordByName(name)

	// This may be a hardlink that is due to be converted.
	if hardlink, isHardlink := record.(*Hardlink); isHardlink && fixHardlinks {
		newRecord, inodeId := container.dir.hardlinkTable.removeHardlink(c,
			hardlink.fileId)

		// Wsr says we're about to orphan the last hardlink copy
		if newRecord != nil {
			newRecord.SetFilename(hardlink.Filename())
			record = newRecord
			container.setRecord(c, newRecord, inodeId)
		}
	}

	result := container.delRecord(c, inodeId, name)

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
	record := container.getRecordByName(c, oldName)
	if record == nil {
		c.vlog("oldName doesn't exist")
		return
	}
	container.deleteChild(c, newName, true)
	container.deleteChild(c, oldName, false)
	record.SetFilename(newName)

	// if this is a hardlink, we must update its creationTime and the accesslist
	// path
	if hardlink, isHardlink := record.(*Hardlink); isHardlink {
		hardlink.creationTime = quantumfs.NewTime(time.Now())
		container.dir.markHardlinkPath(c, record.Filename(), record.FileId())
	}
	container.setRecord(c, record, inodeId)
	container.makePublishable(c, newName)
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

	records := make([]quantumfs.DirectoryRecord, 0, container.count())
	for _, byInode := range container.publishable {
		for _, record := range byInode {
			records = append(records, record)
		}
	}

	return records
}

func (container *ChildContainer) records() []quantumfs.DirectoryRecord {
	publishableRecords := container.publishable.records()

	records := make([]quantumfs.DirectoryRecord, 0, container.count())
	seen := make(map[string]bool, container.count())

	for _, byInode := range container.effective {
		for _, record := range byInode {
			records = append(records, record)
			seen[record.Filename()] = true
		}
	}

	for _, byInode := range container.publishable {
		for _, record := range byInode {
			if !seen[record.Filename()] {
				records = append(records, record)
			}
		}
	}

	return records
}

func (container *ChildContainer) makeHardlink(c *ctx, childId InodeId) (
	copy quantumfs.DirectoryRecord, err fuse.Status) {

	defer c.FuncIn("ChildContainer::makeHardlink", "inode %d", childId).Out()
	child := container.getRecordById(childId)
	if child == nil {
		c.elog("No child record for inode %d", childId)
		return nil, fuse.ENOENT
	}

	// If it's already a hardlink, great no more work is needed
	if link, isLink := child.(*Hardlink); isLink {
		c.vlog("Already a hardlink")

		recordCopy := *link

		// Ensure we update the ref count for this hardlink
		container.dir.hardlinkTable.hardlinkInc(link.fileId)

		return &recordCopy, fuse.OK
	}

	// record must be a file type to be hardlinked
	if !child.Type().IsRegularFile() &&
		child.Type() != quantumfs.ObjectTypeSymlink &&
		child.Type() != quantumfs.ObjectTypeSpecial {

		c.dlog("Cannot hardlink %s - not a file", record.Filename())
		return nil, fuse.EINVAL
	}

	childname := child.Filename()
	// remove the record from the childmap before donating it to be a hardlink
	container.delRecord(childId, childname)

	c.vlog("Converting %s into a hardlink", childname)
	newLink := container.dir.hardlinkTable.newHardlink(c, childId, child)

	linkSrcCopy := newLink.Clone()
	linkSrcCopy.SetFilename(childname)
	container.setRecord(c, childId, linkSrcCopy)

	// The child is becoming a hardlink which means it will be part of the
	// hardlink map in wsr the next time wsr gets published, therefore,
	// we can mark it as publishable
	container.makePublishable(c, childname)

	newLink.creationTime = quantumfs.NewTime(time.Now())
	newLink.SetContentTime(newLink.creationTime)
	return newLink, fuse.OK
}

func (container *ChildContainer) makePublishable(c *ctx, name string) {
	defer c.FuncIn("ChildContainer::makePublishable", "%s", name).Out()

	inodeId := container.inodeNum(name)
	utils.Assert(inodeId != quantumfs.InodeIdInvalid, "No such child %s", name)

	record := container.effective[inodeId][name]
	utils.Assert(record != nil, "No effective record for name: %s", name)

	records := container.publishable[inodeId]
	if records == nil {
		records = make(map[string]quantumfs.DirectoryRecord, 0)
	}
	records[name] = record
	container.publishable[inodeId] = records

	records = container.effective[inodeId]
	if len(records) == 0 {
		delete(container.effective, inodeId)
	} else {
		delete(container.effective[inodeId], name)
	}
}
